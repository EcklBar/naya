from kafka import KafkaConsumer

# In this example we will illustrate a simple producer-consumer integration

topic1 = 'kafka-tst-02'
brokers = ['course-kafka:9092']

# First we set the consumer,
consumer = KafkaConsumer(topic1,bootstrap_servers=bootstrapServers)

# print the value of the consumer
# we run the consumer to fetch the message scoming from topic1.
for message in consumer:
    print(str(message.value))
