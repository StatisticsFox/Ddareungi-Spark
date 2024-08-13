from pyspark.sql import SparkSession
from data_schema import get_schema
from kafka_Spark_config import KafkaConfig
from redis_config import RedisConfig
from pyspark.sql import functions as F
import redis.sentinel
import os

# 환경변수에서 sentinel_hosts 가져오기
# sentinel_hosts_env = os.getenv('SENTINEL_HOSTS') # ['172.31.16.56:26379', '172.31.46.109:26379', '172.31.48.186:26379']
# master_name = os.getenv('MASTER_NAME') # mymaster
# redis_auth = os.getenv('REDIS_AUTH')

sentinel_hosts = [
('172.31.16.56', 26379),
('172.31.46.109', 26379),
('172.31.48.186', 26379)
]
master_name = 'mymaster' # os.getenv('MASTER_NAME')
redis_auth = 'mypass' # os.getenv('REDIS_AUTH')

# sentinel_hosts = [(host.split(':')[0], int(host.split(':')[1])) for host in sentinel_hosts_env.split(',')]
redis_config = RedisConfig(sentinel_hosts, master_name, redis_auth)
master_host, master_port = redis_config.get_redis_master(sentinel_hosts, master_name, redis_auth)

# Spark 세션 생성
spark = (
    SparkSession
    .builder
    .appName("kafka_streaming")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.redislabs:spark-redis_2.12:3.0.0')
    .config("spark.redis.host", master_host)
    .config("spark.redis.port", master_port)
    .config("spark.redis.auth", redis_auth)
    .master("local[*]")
    .getOrCreate()
)

# Kafka 메시지의 값을 읽기
kafka_config = KafkaConfig(topic_name="bike-station-info", 
                           bootstrap_servers="172.31.30.11:9092,172.31.39.201:9092,172.31.52.183:9092")
kafka_df = kafka_config.read_from_kafka(spark)

# JSON 문자열을 StructType으로 변환하기 위한 스키마 정의
schema = get_schema()

# JSON 데이터를 스키마에 맞게 변환
value_df = kafka_df.select(F.from_json(F.col("value"), schema).alias("value"))
flattened_df = value_df.select("value.*")

# 스트리밍 쿼리 정의 및 시작
query = (
    flattened_df.writeStream
    .outputMode("update")
    .foreachBatch(RedisConfig.write_to_redis)
    .start()
)

# 스트리밍 쿼리 실행 대기
query.awaitTermination()