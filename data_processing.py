from pyspark.sql.functions import col, from_json, window, expr
from datetime import datetime, timedelta

class DataProcessor:
    def __init__(self, schema):
        self.schema = schema

    def transform_kafka_df(self, kafka_df):
        value_df = kafka_df.select(from_json(col("value"), self.schema).alias("data"))
        return value_df.select("data.*")

    def aggregate_data(self, json_df):
        json_df = json_df.withColumn("event_time", col("timestamp").cast("timestamp")).drop("timestamp")
        json_df.createOrReplaceTempView("bike_stations")
        query = """
        SELECT
            stationId,
            window(event_time, '1 hour') as window,
            MAX(event_time) as event_time,
            FIRST(stationName) as stationName,
            FIRST(rackTotCnt) as rackTotCnt,
            MAX(parkingBikeTotCnt) - MIN(parkingBikeTotCnt) as diff
        FROM
            bike_stations
        GROUP BY
            stationId, window(event_time, '1 hour')
        """
        result_df = json_df.sql(query)
        return result_df.selectExpr(
            "stationId",
            "window.end as event_time",
            "stationName",
            "rackTotCnt",
            "IF(diff > 0, diff, 0) as return",
            "IF(diff < 0, -diff, 0) as rental"
        )

    def save_to_s3(self, batch_df, batch_id):
        previous_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        year = previous_hour.strftime('%Y')
        month = previous_hour.strftime('%m')
        day = previous_hour.strftime('%d')
        hour = previous_hour.strftime('%H')

        bucket_name = 'ddareungidatabucket'
        folder_name = 'hourly_summary'
        path = f"s3a://{bucket_name}/{folder_name}/{year}/{month}/{day}/{hour}"
        batch_df.write.mode("overwrite").parquet(path)
