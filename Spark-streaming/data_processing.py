# test_data_processing.py
from pyspark.sql.functions import col, from_json, window, expr
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

class DataProcessor:
    def __init__(self, schema, spark):
        self.schema = schema
        self.spark = spark
        self.state_df = self.initialize_state_df()

    def transform_kafka_df(self, kafka_df):
        json_df = kafka_df.select(F.from_json(F.col("value"), self.schema).alias("data")).select("data.*")
        json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        json_df = json_df.drop("timestamp")
        return json_df

    def initialize_state_df(self):
        state_schema = T.StructType([
            T.StructField("stationId", T.StringType(), nullable=False),
            T.StructField("previous_parkingBikeTotCnt", T.IntegerType(), nullable=False),
        ])
        return self.spark.createDataFrame([], state_schema)

    # 상태 업데이트 함수
    def update_state(self, batch_df, batch_id):
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        joined_df = batch_df.join(self.state_df, on="stationId", how="left")

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

        self.state_df = new_state_df

        hourly_summary = changes_df.groupBy(
            F.window("event_time", "1 hour"),
            "stationId"
        ).agg(
            F.sum("rental").alias("total_rental"),
            F.sum("return").alias("total_return")
        ).select(
            "window.start",
            "window.end",
            "stationId",
            "total_rental",
            "total_return"
        )
        return hourly_summary
        
    def save_to_s3(self, hourly_summary, batch_id):
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
