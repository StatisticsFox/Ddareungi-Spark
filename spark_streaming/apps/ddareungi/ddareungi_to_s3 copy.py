from ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from datetime import datetime

class DdareungiToS3(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)  # 'self' 인자 제거
        self.topic_lst = ['bike-station-info']
        self.log_mode = 'DEBUG'
        self.checkpoint_location = "/home/ubuntu/ddareungi/checkpoint"  
        self.state_store_location = "/home/ubuntu/ddareungi/state_store"  

    def _main(self):
        self.logger.write_log('INFO', 'Starting _main method', None)
        
        self.logger.write_log('INFO', 'Creating Spark session', None)
        spark = self.create_spark_session()
        spark.sparkContext.setCheckpointDir(self.checkpoint_location)
        self.logger.write_log('INFO', 'Spark session created successfully', None)

        self.logger.write_log('INFO', 'Initializing state_df', None)
        state_schema = T.StructType([
            T.StructField("stationId", T.StringType(), nullable=False),
            T.StructField("previous_parkingBikeTotCnt", T.IntegerType(), nullable=False),
        ])
        state_df = spark.createDataFrame([], state_schema)
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
                        .foreachBatch(lambda df, epoch_id: self.process_batch(df, epoch_id, state_df)) \
                        .outputMode("update") \
                        .start()
            
            self.logger.write_log('INFO', 'Streaming query started successfully', None)
            
            self.logger.write_log('INFO', 'Waiting for query termination', None)
            query.awaitTermination()
        except Exception as e:
            self.logger.write_log('ERROR', f'Error in streaming query: {str(e)}', None)

    def calculate_hourly_summary(self, changes_df: DataFrame) -> DataFrame:
        return changes_df.withColumn(
            "hour", F.date_trunc("hour", F.col("event_time"))
        ).groupBy("hour", "stationId").agg(
            F.sum("rental").alias("total_rental"),
            F.sum("return").alias("total_return")
        ).select(
            F.col("hour").alias("start"),
            (F.col("hour") + F.expr("INTERVAL 1 HOUR")).alias("end"),
            "stationId",
            "total_rental",
            "total_return"
        )

    def process_batch(self, df: DataFrame, epoch_id, state_df: DataFrame):
        self.logger.write_log('INFO', 'Starting process_batch', epoch_id)

        # Parse JSON data and prepare DataFrame
        json_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("data")).select("data.*")
        json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")).drop("timestamp")

        # Join with state DataFrame
        joined_df = json_df.join(state_df, on="stationId", how="left")

        # Calculate changes
        changes_df = joined_df.withColumn(
            "previous_parkingBikeTotCnt", F.coalesce(F.col("previous_parkingBikeTotCnt"), F.lit(0))
        ).withColumn(
            "change", F.when(F.col("previous_parkingBikeTotCnt") == 0, 0)
                      .otherwise(F.col("parkingBikeTotCnt") - F.col("previous_parkingBikeTotCnt"))
        ).withColumn(
            "return", F.when(F.col("change") > 0, F.col("change")).otherwise(0)
        ).withColumn(
            "rental", F.when(F.col("change") < 0, -F.col("change")).otherwise(0)
        )

        # Update state
        state_df = changes_df.select("stationId", F.col("parkingBikeTotCnt").alias("previous_parkingBikeTotCnt"))
        state_df.checkpoint()

        # Calculate hourly summary using the new method
        hourly_summary = self.calculate_hourly_summary(changes_df)

        # Filter for the current hour
        now = F.current_timestamp().cast("timestamp").truncate("hour")
        hourly_summary = hourly_summary.filter(F.col("start") == now)

        # Write to S3
        self.write_to_s3(hourly_summary, epoch_id)

        self.logger.write_log('INFO', 'Batch processing completed', epoch_id)

    def write_to_s3(self, df: DataFrame, epoch_id):
        window_end = df.select(F.max("end")).collect()[0][0]
        path = f"s3a://ddareungidatabucket/hourly_summary/{window_end.strftime('%Y/%m/%d/%H')}"

        self.logger.write_log('INFO', f'Writing to S3: {path}', epoch_id)
        try:
            df.repartition(1).write.mode("overwrite").parquet(path)
            self.logger.write_log('INFO', 'Successfully wrote to S3', epoch_id)
        except Exception as e:
            self.logger.write_log('ERROR', f'Error writing to S3: {str(e)}', epoch_id)

        df.orderBy("stationId").show()


if __name__ == "__main__":
    app = DdareungiToS3("DdareungiToS3")
    app.main()