from pyspark.sql import SparkSession

class KafkaConfig:
    def __init__(self, topic_name, bootstrap_servers):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers

    def create_spark_session(self, app_name):
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
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
