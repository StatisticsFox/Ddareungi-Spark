from kafka_Spark_config import KafkaConfig
from redis_config import RedisConfig
from data_schema import get_schema
from data_processing import DataProcessor

# Kafka와 Redis 설정
kafka_config = KafkaConfig(topic_name="bike-station-info", 
                           bootstrap_servers="172.31.30.11:9092,172.31.39.201:9092,172.31.52.183:9092")
redis_config = RedisConfig(
    sentinel_hosts=[
        ('172.31.16.56', 26379),
        ('172.31.46.109', 26379),
        ('172.31.48.186', 26379)
    ],
    master_name='mymaster',
    redis_auth='mypass'
)

# Spark 세션 생성
spark = kafka_config.create_spark_session("kafka_streaming")
kafka_df = kafka_config.read_from_kafka(spark)

# 데이터 처리
schema = get_schema()
data_processor = DataProcessor(schema=schema)
flattened_df = data_processor.transform_kafka_df(kafka_df)

# Redis에 데이터를 쓰는 함수
def write_to_redis(batch_df, batch_id):
    batch_df.write \
    .format("org.apache.spark.sql.redis") \
    .option("table", "bike_stations") \
    .option("key.column", "stationId") \
    .mode("append") \
    .save()

# 스트리밍 쿼리 정의 및 시작
query = (
    flattened_df.writeStream
    .outputMode("update")
    .foreachBatch(write_to_redis)
    .start()
)

query.awaitTermination()
