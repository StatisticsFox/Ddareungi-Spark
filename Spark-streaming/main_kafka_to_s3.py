# test_main_kafka_to_S3.py
from kafka_spark_config import KafkaConfig
from data_schema import get_schema
from data_processing import DataProcessor

kafka_config = KafkaConfig(topic_name="bike-station-info", 
                           bootstrap_servers="kafak_node1:9092,kafak_node2:9092,kafak_node3:9092")

your_access_key_id, your_secret_access_key = kafka_config.get_key()
spark = kafka_config.create_spark_session("kafka_streaming_DF", your_access_key_id, your_secret_access_key)
kafka_df = kafka_config.read_from_kafka(spark)

schema = get_schema()
data_processor = DataProcessor(schema=schema, spark=spark)
flattened_df = data_processor.transform_kafka_df(kafka_df)

def process_batch(batch_df, batch_id):
    hourly_summary = data_processor.update_state(batch_df, batch_id)
    data_processor.save_to_s3(hourly_summary, batch_id)

query = (
    flattened_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .start()
)

query.awaitTermination()