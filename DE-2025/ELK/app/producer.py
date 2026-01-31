from kafka import KafkaProducer

from time import sleep

# Topics/Brokers
topic= 'kafka-tst-07'
brokers = "course-kafka:9092"


producer = KafkaProducer(
    bootstrap_servers=brokers
)

# The send() method creates the topic
producer.send(topic, value=b'Hello, World!!!!')
producer.flush()

# # One more example
producer.send(topic, key=b'event#2', value=b'This is a Kafka-Python basic tutorial')
producer.flush()
