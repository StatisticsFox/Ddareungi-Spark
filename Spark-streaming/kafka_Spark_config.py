# kafka_spark_config.py
from pyspark.sql import SparkSession

class KafkaConfig:
    def __init__(self, topic_name, bootstrap_servers):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers

    def get_key(self):
        with open("/home/ubuntu/your_access_key_id.bin", "r", encoding="UTF-8") as api_key_file:
            your_access_key_id = api_key_file.read().strip()

        with open("/home/ubuntu/your_secret_access_key.bin", "r", encoding="UTF-8") as api_key_file:
            your_secret_access_key = api_key_file.read().strip()
        
        return your_access_key_id, your_secret_access_key
    
    def create_spark_session(self, app_name, your_access_key_id, your_secret_access_key):
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
            .config("spark.hadoop.fs.s3a.access.key", your_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", your_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def read_from_kafka(self, spark):
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic_name)
            .option("startingOffsets", "earliest")
            .load()
        )
        return kafka_df.selectExpr("CAST(value AS STRING) as value")