from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
import uuid


def generate_sensor_data():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("DataGenerator") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Load data from Parquet file into a DataFrame
    cars_df = spark.read.parquet('s3a://spark/data/dims/cars')
    cars_df.cache()

    # Convert the DataFrame records to JSON format
    cars_rdd = cars_df.toJSON()

    # Set up a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='course-kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Output KAFKA topic
    topic = "sensors-sample"

    while True:
        # Collect cars data
        cars_list = cars_rdd.collect()

        # Generate event for each car
        for car_json in cars_list:
            # Parse JSON string to dictionary
            car = json.loads(car_json)

            event = {
                "event_id": str(uuid.uuid4()),
                "event_time": datetime.now().isoformat(),
                "car_id": car["car_id"],
                "speed": random.randint(0, 200),
                "rpm": random.randint(0, 8000),
                "gear": random.randint(1, 7)
            }

            # Send to Kafka
            producer.send(topic, value=event)

        producer.flush()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generated {len(cars_list)} events")

        # Sleep for 1 second
        time.sleep(1)

if __name__ == "__main__":
    generate_sensor_data()
