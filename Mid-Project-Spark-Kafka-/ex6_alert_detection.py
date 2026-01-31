from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def detect_alerts():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('AlertDetection') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
        .getOrCreate()

    # Define the schema
    sensor_schema = T.StructType([
        T.StructField('event_id', T.StringType()),
        T.StructField('event_time', T.StringType()),
        T.StructField('car_id', T.IntegerType()),
        T.StructField('driver_id', T.IntegerType()),
        T.StructField('brand_name', T.StringType()),
        T.StructField('model_name', T.StringType()),
        T.StructField('color_name', T.StringType()),
        T.StructField('speed', T.IntegerType()),
        T.StructField('rpm', T.IntegerType()),
        T.StructField('gear', T.IntegerType()),
        T.StructField('expected_gear', T.IntegerType())
    ])

    # Read from Kafka
    stream_df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'course-kafka:9092') \
        .option('subscribe', 'samples-enriched') \
        .option('startingOffsets', 'latest') \
        .load() \
        .select(F.col('value').cast(T.StringType()))

    parsed_df = stream_df \
        .withColumn('parsed_json', F.from_json(F.col('value'), sensor_schema)) \
        .select(F.col('parsed_json.*'))

    alerts_df = parsed_df.filter(
        (F.col('speed') > 120) |
        (F.col('expected_gear') != F.col('gear')) |
        (F.col('rpm') > 6000)
    )

    kafka_output = alerts_df.select(
        F.to_json(F.struct('*')).alias('value')
    )

    query = kafka_output \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'course-kafka:9092') \
        .option('topic', 'alert-data') \
        .option('checkpointLocation', '/tmp/checkpoint/alerts') \
        .start()

    query.awaitTermination()
    spark.stop()

if __name__ == '__main__':
    detect_alerts()
