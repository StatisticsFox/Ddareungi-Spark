from ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
# from datetime import datetime
from pyspark.sql.window import Window

class DdareungiToS3(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.topic_lst = ['bike-station-info']
        self.log_mode = 'DEBUG'
        self.checkpoint_location = "/home/ubuntu/ddareungi/checkpoint"  
        self.state_store_location = "/home/ubuntu/ddareungi/state_store"  

    def _main(self):
        spark = self.create_spark_session()
        spark.sparkContext.setCheckpointDir(self.checkpoint_location)
        state_schema = T.StructType([
            T.StructField("stationId", T.StringType(), nullable=False),
            T.StructField("previous_parkingBikeTotCnt", T.IntegerType(), nullable=False),
        ])
        state_df = spark.createDataFrame([], state_schema)

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
                        .option("checkpointLocation", self.checkpoint_location) \
                        .start()

            query.awaitTermination()
        except Exception as e:
            self.logger.write_log('ERROR', f'스트리밍 쿼리 오류: {str(e)}', None)

    def process_batch(self, df: DataFrame, epoch_id, state_df: DataFrame):
        
        json_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("data")).select( 
            "data.stationId",
            "data.parkingBikeTotCnt",
            "data.timestamp")
        json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        json_df = json_df.drop("timestamp")

        windowSpec = Window.partitionBy("stationId").orderBy("event_time")
        diff_df = json_df \
            .withColumn("prev_count", F.lag("parkingBikeTotCnt").over(windowSpec)) \
            .withColumn("diff", F.col("parkingBikeTotCnt") - F.col("prev_count")) \
            .withColumn("rental", F.when(F.col("diff") < 0, -F.col("diff")).otherwise(0)) \
            .withColumn("return", F.when(F.col("diff") > 0, F.col("diff")).otherwise(0))
        
        diff_df = diff_df.checkpoint()

        self.logger.write_log('INFO', '배치 처리 시작', epoch_id)
        hourly_stats = diff_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                F.window("timestamp", "1 hour", "5 minutes"), "stationId" ) \
            .agg(
        sum("rental").alias("hourly_rental"),
        sum("return").alias("hourly_return")
        )

        # 시간별 전체 통계
        total_hourly_stats = hourly_stats \
            .groupBy("window") \
            .agg(
                sum("hourly_rental").alias("total_hourly_rental"),
                sum("hourly_return").alias("total_hourly_return")
            )


        if self.log_mode == 'DEBUG':
            self.logger.write_log('DEBUG', 'total_hourly_stats 출력', epoch_id)
            total_hourly_stats.show(truncate=False)

        self.logger.write_log('INFO', 'S3 쓰기 준비 중', epoch_id)
        window_end = total_hourly_stats.select(F.max("end")).collect()[0][0]
        year = window_end.strftime('%Y')
        month = window_end.strftime('%m')
        day = window_end.strftime('%d')
        hour = window_end.strftime('%H')

        bucket_name = 'ddareungidatabucket'
        folder_name = 'total_hourly_stats'
        path = f"s3a://{bucket_name}/{folder_name}/{year}/{month}/{day}/{hour}"

        self.logger.write_log('INFO', f'S3에 쓰기 중: {path}', epoch_id)
        try:
            total_hourly_stats.repartition(1).write.mode("overwrite").parquet(path)
            self.logger.write_log('INFO', 'S3 쓰기 성공', epoch_id)
        except Exception as e:
            self.logger.write_log('ERROR', f'S3 쓰기 오류: {str(e)}', epoch_id)

        total_hourly_stats.orderBy("stationId").show()

        self.logger.write_log('INFO', '배치 처리 완료', epoch_id)

if __name__ == "__main__":
    app = DdareungiToS3("DdareungiToS3")
    app.main()