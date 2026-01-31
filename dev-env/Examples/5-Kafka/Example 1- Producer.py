import json
from kafka import KafkaProducer

from time import sleep

# Topics/Brokers
topic1 = 'kafka-tst-01'
brokers = ['course-kafka:9092']


producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# The send() method creates the topic
data = {'name':'avi','price':500,'target':'sergei'}
producer.send(topic1, value=data)
producer.flush()

# # One more example
# producer.send(topic1, key=b'event#2', value=b'This is a Kafka-Python basic tutorial')
# producer.flush()
