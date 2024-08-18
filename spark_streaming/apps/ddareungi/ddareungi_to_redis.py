from spark_streaming.apps.ddareungi.ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame

class DdareungiToRedis(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)


    def _main(self):
        spark = self.create_spark_session(redis_master_name='mymaster')
        query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", ','.join(self.source_topic_lst)) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as value") \
            .writeStream \
            .foreachBatch(lambda df, epoch_id: self.process_batch(df, epoch_id)) \
            .outputMode("update") \
            .start()

        query.awaitTermination()

    def process_batch(self, df: DataFrame, epoch_id):
        self.logger.write_log('INFO', 'microbatch start', epoch_id)

        value_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("value"))
        value_cnt = value_df.count()
        flattened_df = value_df.select("value.*")
        self.redis_config.write_to_redis(
            df=flattened_df,
            table_nm='bike_stations',
            key_col_lst=['stationId']
        )
        self.logger.write_log('INFO', f'Redis 전송 완료({value_cnt})', epoch_id)
        self.logger.write_log('INFO', 'microbatch start', epoch_id)