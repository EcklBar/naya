from kafka import KafkaConsumer
from datetime import datetime

# In this example we will illustrate a simple producer-consumer integration
topic3 = 'kafka-tst-07'
brokers = "course-kafka:9092"

# First we set the consumer, and we use the KafkaConsumer class to create a generator of the messages.
consumer = KafkaConsumer(topic3,bootstrap_servers=brokers)

for message in consumer:
    dt_object = datetime.fromtimestamp(message.timestamp/1000)
    print("the value is: " ,str(message.value))
    print("the offset is: ",str(message.offset))
    print("the time is: ",dt_object)
