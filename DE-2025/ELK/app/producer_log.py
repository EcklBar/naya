from kafka import KafkaProducer

from time import sleep
import json
# Topics/Brokers

topic2 = 'kafka-tst-02'
brokers = ["course-kafka:9092"]

source_file = '/home/naya/kafka/srcFiles/srcFile.log'

# First we set the producer.
producer = KafkaProducer(
    bootstrap_servers = brokers,
    client_id = 'producer',
    acks = 1,
    compression_type = None,
    retries = 3,
    reconnect_backoff_ms = 50,
    reconnect_backoff_max_ms= 1000)


# Send the data
with open(source_file, 'r') as f:
    while True:
        lines = f.readlines()  # returns list of strings
        # print(lines)
        if not lines:
            sleep(1)
            f.seek(f.tell())
        else:
            print(lines)
            producer.send(topic=topic2, value=json.dumps(lines).encode('utf-8'))
            # producer.send(topic=topic2, value=json.dumps(lines).encode('utf-8'))
            producer.flush()
            sleep(3)
        # print(f.tell())
