from spark_streaming.apps.ddareungi.ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from datetime import datetime

class DdareungiToS3(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.topic_lst = ['bike-station-info']
        self.log_mode = 'DEBUG'
        self.state_df = None

    def _main(self):
        self.logger.write_log('INFO', 'Starting _main method', None)
        
        self.logger.write_log('INFO', 'Creating Spark session', None)
        spark = self.create_spark_session()
        self.logger.write_log('INFO', 'Spark session created successfully', None)

        self.logger.write_log('INFO', 'Initializing state_df', None)
        state_schema = T.StructType([
            T.StructField("stationId", T.StringType(), nullable=False),
            T.StructField("previous_parkingBikeTotCnt", T.IntegerType(), nullable=False),
        ])
        self.state_df = spark.createDataFrame([], state_schema)
        self.logger.write_log('INFO', 'state_df initialized', None)

        self.logger.write_log('INFO', 'Setting up streaming query', None)
        try:
            query = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                        .option("subscribe", ','.join(self.source_topic_lst)) \
                        .option("startingOffsets", "earliest") \
                        .load() \
                        .selectExpr("CAST(value AS STRING) as value") \
                        .writeStream \
                        .foreachBatch(self.process_batch) \
                        .outputMode("update") \
                        .start()
            
            self.logger.write_log('INFO', 'Streaming query started successfully', None)
            
            self.logger.write_log('INFO', 'Waiting for query termination', None)
            query.awaitTermination()
        except Exception as e:
            self.logger.write_log('ERROR', f'Error in streaming query: {str(e)}', None)

    def check_data_quality(self, df):
        total_rows = df.count()
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
        for col, null_count in zip(df.columns, null_counts):
            if null_count is not None and null_count > 0:
                self.logger.write_log('WARNING', f'Column {col} has {null_count} NULL values out of {total_rows} rows', None)
            elif null_count is None:
                self.logger.write_log('WARNING', f'Unable to determine NULL count for column {col}', None)

    def process_batch(self, df: DataFrame, epoch_id):
        self.logger.write_log('INFO', 'Starting process_batch', epoch_id)

        self.logger.write_log('INFO', 'Parsing JSON data', epoch_id)
        json_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("data")).select("data.*")
        json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        json_df = json_df.drop("timestamp")

        self.check_data_quality(json_df)

        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.logger.write_log('INFO', f'Current time: {now}', epoch_id)

        self.logger.write_log('INFO', 'Joining dataframes', epoch_id)
        joined_df = json_df.join(self.state_df, on="stationId", how="left")

        if self.log_mode == 'DEBUG':
            self.logger.write_log('DEBUG', 'joined_df.show()', epoch_id)
            joined_df.show(truncate=False)

        self.logger.write_log('INFO', 'Calculating changes', epoch_id)
        changes_df = joined_df.withColumn(
            "previous_parkingBikeTotCnt", F.coalesce(F.col("previous_parkingBikeTotCnt"), F.lit(0))
        ).withColumn(
            "parkingBikeTotCnt", F.coalesce(F.col("parkingBikeTotCnt"), F.lit(0))
        ).withColumn(
            "change",
            F.col("parkingBikeTotCnt") - F.col("previous_parkingBikeTotCnt")
        ).withColumn(
            "return",
            F.when(F.col("change") > 0, F.col("change")).otherwise(0)
        ).withColumn(
            "rental",
            F.when(F.col("change") < 0, -F.col("change")).otherwise(0)
        )

        self.logger.write_log('INFO', 'Updating state', epoch_id)
        new_state_df = changes_df.select(
            F.col("stationId"),
            F.coalesce(F.col("parkingBikeTotCnt"), F.col("previous_parkingBikeTotCnt")).alias("previous_parkingBikeTotCnt")
        )
        self.state_df = self.state_df.union(new_state_df).groupBy("stationId").agg(F.last("previous_parkingBikeTotCnt").alias("previous_parkingBikeTotCnt"))

        self.logger.write_log('INFO', 'Calculating hourly summary', epoch_id)
        hourly_summary = changes_df.groupBy(
            F.window("event_time", "1 hour").alias("window"),
            "stationId"
        ).agg(
            F.sum(F.coalesce(F.col("rental"), F.lit(0))).alias("total_rental"),
            F.sum(F.coalesce(F.col("return"), F.lit(0))).alias("total_return")
        ).select(
            F.col("window.start").alias("start"),
            F.col("window.end").alias("end"),
            "stationId",
            "total_rental",
            "total_return"
        ).filter(F.col("start") == now)

        if self.log_mode == 'DEBUG':
            self.logger.write_log('DEBUG', 'hourly_summary.show()', epoch_id)
            hourly_summary.show(truncate=False)

        self.logger.write_log('INFO', 'Preparing to write to S3', epoch_id)
        window_end = hourly_summary.select(F.max("end")).collect()[0][0]
        year = window_end.strftime('%Y')
        month = window_end.strftime('%m')
        day = window_end.strftime('%d')
        hour = window_end.strftime('%H')

        bucket_name = 'ddareungidatabucket'
        folder_name = 'hourly_summary'
        path = f"s3a://{bucket_name}/{folder_name}/{year}/{month}/{day}/{hour}"

        self.logger.write_log('INFO', f'Writing to S3: {path}', epoch_id)
        try:
            hourly_summary.repartition(1).write.mode("overwrite").parquet(path)
            self.logger.write_log('INFO', 'Successfully wrote to S3', epoch_id)
        except Exception as e:
            self.logger.write_log('ERROR', f'Error writing to S3: {str(e)}', epoch_id)

        hourly_summary.orderBy("stationId").show()

        self.logger.write_log('INFO', 'Batch processing completed', epoch_id)

if __name__ == "__main__":
    app = DdareungiToS3("DdareungiToS3")
    app.main()