from spark_streaming.apps.ddareungi.ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame

class DdareungiToRedis(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.logger.write_log('INFO', f'Initializing {app_name}')

    def _main(self):
        self.logger.write_log('INFO', 'Starting _main method')
        spark = self.create_spark_session(redis_master_name='mymaster')
        self.logger.write_log('INFO', 'Spark session created')

        self.logger.write_log('INFO', f'Setting up Kafka stream with topics: {self.source_topic_lst}')
        query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", ','.join(self.source_topic_lst)) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        
        self.logger.write_log('INFO', 'Kafka stream setup complete')

        self.logger.write_log('INFO', 'Starting streaming query')
        query = query.writeStream \
            .foreachBatch(lambda df, epoch_id: self.process_batch(df, epoch_id)) \
            .outputMode("update") \
            .start()

        self.logger.write_log('INFO', 'Streaming query started, awaiting termination')
        query.awaitTermination()

    def process_batch(self, df: DataFrame, epoch_id):
        self.logger.write_log('INFO', f'Processing microbatch {epoch_id}')
        
        self.logger.write_log('INFO', 'Parsing JSON data')
        value_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("value"))
        value_cnt = value_df.count()
        self.logger.write_log('INFO', f'Parsed {value_cnt} records')

        self.logger.write_log('INFO', 'Flattening DataFrame')
        flattened_df = value_df.select("value.*")

        self.logger.write_log('INFO', 'Writing to Redis')
        self.redis_config.write_to_redis(
            df=flattened_df,
            table_nm='bike_stations',
            key_col_lst=['stationId']
        )

        self.logger.write_log('INFO', f'Redis transmission complete ({value_cnt} records)', epoch_id)
        self.logger.write_log('INFO', f'Microbatch {epoch_id} processing complete')

if __name__ == "__main__":
    app = DdareungiToRedis("DdareungiToRedis")
    app.logger.write_log('INFO', 'Application initialized, starting main process')
    app._main()
    app.logger.write_log('INFO', 'Application terminated')