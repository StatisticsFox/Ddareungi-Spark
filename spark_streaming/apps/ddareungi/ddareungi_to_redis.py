from ddareungi_base_class import DdareungiBaseClass
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame

class Logger:
    # Logger 클래스에서 write_log 메서드를 수정하여 epoch_id를 선택적으로 받도록 설정
    def write_log(self, log_level, message, epoch_id=None):
        if epoch_id is not None:
            print(f'[{log_level}] {message} (Epoch: {epoch_id})')
        else:
            print(f'[{log_level}] {message}')

class DdareungiToRedis(DdareungiBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        # 초기화 시에는 epoch_id 필요 없음
        self.logger.write_log('INFO', f'Initializing {app_name}')

    def _main(self):
        self.logger.write_log('INFO', 'Starting _main method')  # epoch_id 없음
        spark = self.create_spark_session(redis_master_name='mymaster')
        self.logger.write_log('INFO', 'Spark session created')  # epoch_id 없음

        self.logger.write_log('INFO', f'Setting up Kafka stream with topics: {self.source_topic_lst}')  # epoch_id 없음
        query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", ','.join(self.source_topic_lst)) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")

        self.logger.write_log('INFO', 'Kafka stream setup complete')  # epoch_id 없음

        self.logger.write_log('INFO', 'Starting streaming query')  # epoch_id 없음
        query = query.writeStream \
            .foreachBatch(lambda df, epoch_id: self.process_batch(df, epoch_id)) \
            .outputMode("update") \
            .start()

        self.logger.write_log('INFO', 'Streaming query started, awaiting termination')  # epoch_id 없음
        query.awaitTermination()

    def process_batch(self, df: DataFrame, epoch_id):
        # epoch_id가 포함된 로그 출력
        self.logger.write_log('INFO', f'Processing microbatch', epoch_id)

        self.logger.write_log('INFO', 'Parsing JSON data', epoch_id)
        value_df = df.select(F.from_json(F.col("value"), self.ddareungi_schema).alias("value"))
        value_cnt = value_df.count()
        self.logger.write_log('INFO', f'Parsed {value_cnt} records', epoch_id)

        self.logger.write_log('INFO', 'Flattening DataFrame', epoch_id)
        flattened_df = value_df.select("value.*")

        self.logger.write_log('INFO', 'Writing to Redis', epoch_id)
        self.redis_config.write_to_redis(
            df=flattened_df,
            table_nm='bike_stations',
            key_col_lst=['stationId']
        )

        self.logger.write_log('INFO', f'Redis transmission complete ({value_cnt} records)', epoch_id)
        self.logger.write_log('INFO', f'Microbatch {epoch_id} processing complete', epoch_id)

if __name__ == "__main__":
    app = DdareungiToRedis("DdareungiToRedis")
    # 초기화 시 epoch_id 없이 로그 기록
    app.logger.write_log('INFO', 'Application initialized, starting main process')
    app._main()
    app.logger.write_log('INFO', 'Application terminated')
