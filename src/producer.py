import pytchat
from kafka import KafkaProducer
import json
import time
from datetime import datetime
# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# YouTube live stream URL
video_url = "https://www.youtube.com/watch?v=VHuyVXuvfak"
video_id = video_url.split("v=")[-1].split("&")[0]
# Create a pytchat object to listen to the live chat
chat = pytchat.create(video_url)

# Kafka topic
topic = 'Youtubelive-comments'
while chat.is_alive():
    for c in chat.get().items:
        # message to be sent to Kafka
        message = {
            'video_id':video_id,
            'author': c.author.name,
            'raw_comment': c.message,
            'timestamp':c.datetime,
        }
        
        # Send the message to Kafka topic
        producer.send(topic, message)
        print(f"Sent message: {message}") # show messagew
        producer.flush()

    time.sleep(1)
