from spark_streaming.apps.ddareungi.ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime

class DdareungiToS3(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(self, app_name)
        self.topic_lst = ['bike-station-info']
        self.log_mode = 'DEBUG'

    def _main(self):
        spark = self.create_spark_session()

        # state_df 초기화
        state_schema = T.StructType([
            T.StructField("stationId", T.StringType(), nullable=False),
            T.StructField("previous_parkingBikeTotCnt", T.IntegerType(), nullable=False),
        ])
        state_df = spark.createDataFrame([], state_schema)

        # streaming 시작
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

        query.awaitTermination()

    def process_batch(self, df, epoch_id, state_df):
        self.logger.write_log('INFO','microbatch start',epoch_id)

        json_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("data")).select("data.*")
        json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        json_df = json_df.drop("timestamp")

        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        joined_df = json_df.join(state_df, on="stationId", how="left")

        if self.log_mode == 'DEBUG':
            self.logger.write_log('DEBUG', 'joined_df.show()', epoch_id)
            joined_df.show(truncate=False)

        changes_df = joined_df.withColumn(
            "previous_parkingBikeTotCnt", F.coalesce(F.col("previous_parkingBikeTotCnt"), F.lit(0))
        ).withColumn(
            "change",
            F.col("parkingBikeTotCnt") - F.col("previous_parkingBikeTotCnt")
        ).withColumn(
            "return",
            F.when(F.col("change") > 0, F.col("change")).otherwise(0)
        ).withColumn(
            "rental",
            F.when(F.col("change") < 0, -F.col("change")).otherwise(0)
        ).withColumn(
            "change",
            F.when(F.col("previous_parkingBikeTotCnt") == 0, 0).otherwise(F.col("change"))
        ).withColumn(
            "return",
            F.when(F.col("previous_parkingBikeTotCnt") == 0, 0).otherwise(F.col("return"))
        ).withColumn(
            "rental",
            F.when(F.col("previous_parkingBikeTotCnt") == 0, 0).otherwise(F.col("rental"))
        )

        new_state_df = changes_df.select(
            F.col("stationId"),
            F.col("parkingBikeTotCnt").alias("previous_parkingBikeTotCnt")
        )

        state_df = new_state_df

        # 정확한 시간 구간 집계를 위해 window 함수를 event_time에 따라 추가
        hourly_summary = changes_df.groupBy(
            F.window("event_time", "1 hour").alias("window"),
            "stationId"
        ).agg(
            F.sum("rental").alias("total_rental"),
            F.sum("return").alias("total_return")
        ).select(
            F.col("window.start").alias("start"),
            F.col("window.end").alias("end"),
            "stationId",
            "total_rental",
            "total_return"
        ).filter(F.col("start") == now)  # 현재 시간대에 해당하는 데이터만 선택

        if self.log_mode == 'DEBUG':
            self.logger.write_log('DEBUG', 'hourly_summary.show()', epoch_id)
            hourly_summary.show(truncate=False)

        window_end = hourly_summary.select(F.max("end")).collect()[0][0]
        year = window_end.strftime('%Y')
        month = window_end.strftime('%m')
        day = window_end.strftime('%d')
        hour = window_end.strftime('%H')

        bucket_name = 'ddareungidatabucket'
        folder_name = 'hourly_summary'
        path = f"s3a://{bucket_name}/{folder_name}/{year}/{month}/{day}/{hour}"

        hourly_summary.repartition(1).write.mode("overwrite").parquet(path)
        hourly_summary.orderBy("stationId").show()

        self.logger.write_log('INFO', 'microbatch end', epoch_id)