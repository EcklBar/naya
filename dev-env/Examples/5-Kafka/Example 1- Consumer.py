from kafka import KafkaConsumer
from datetime import datetime

topic1 = 'kafka-tst-01'
brokers = ['course-kafka:9092']

# First we set the consumer, and we use the KafkaConsumer class to create a generator of the messages.
consumer = KafkaConsumer(topic1, bootstrap_servers=brokers)

for message in consumer:
    print(message.value)
    # dt_object = datetime.fromtimestamp(message.timestamp/1000)
    # print("the value is: " ,str(message))
