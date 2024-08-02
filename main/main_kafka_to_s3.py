from config.kafka_Spark_config import KafkaConfig
from Data.data_schema import get_schema
from Data.data_processing import DataProcessor

# Kafka 설정
kafka_config = KafkaConfig(topic_name="bike-station-info", 
                           bootstrap_servers="kafak_node1:9092,kafak_node2:9092,kafak_node3:9092")

# Spark 세션 생성
spark = kafka_config.create_spark_session("kafka_streaming_sql")
kafka_df = kafka_config.read_from_kafka(spark)

# 데이터 처리
schema = get_schema()
data_processor = DataProcessor(schema=schema)
flattened_df = data_processor.transform_kafka_df(kafka_df)
aggregated_df = data_processor.aggregate_data(flattened_df)

# 스트리밍 쿼리 실행
query = (
    aggregated_df.writeStream
    .foreachBatch(data_processor.save_to_s3)
    .outputMode("update")
    .start()
)

query.awaitTermination()
