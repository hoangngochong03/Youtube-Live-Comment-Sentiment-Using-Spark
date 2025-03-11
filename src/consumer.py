from kafka import KafkaConsumer
import json

# Kafka consumer 
consumer = KafkaConsumer(
    'Youtubelive-comments',  # Topic name
    bootstrap_servers='localhost:9092',
    group_id='start_retrieve_comment',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages from Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")
