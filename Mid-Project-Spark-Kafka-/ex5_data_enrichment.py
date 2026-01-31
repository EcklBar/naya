from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def enrich_data():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("DataEnrichment") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .getOrCreate()

    # Define the schema
    sensor_schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("car_id", T.IntegerType()),
        T.StructField("speed", T.IntegerType()),
        T.StructField("rpm", T.IntegerType()),
        T.StructField("gear", T.IntegerType())
    ])

    # Read from Kafka
    stream_df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'course-kafka:9092') \
        .option('subscribe', 'sensors-sample') \
        .option('startingOffsets', 'latest') \
        .load() \
        .select(F.col('value').cast(T.StringType()))

    parsed_df = stream_df \
        .withColumn('parsed_json', F.from_json(F.col('value'), sensor_schema)) \
        .select(F.col('parsed_json.*'))

    cars_df = spark.read.parquet('s3a://spark/data/dims/cars')
    models_df = spark.read.parquet('s3a://spark/data/dims/car_models')
    colors_df = spark.read.parquet('s3a://spark/data/dims/car_colors')

    enriched_df = parsed_df.join(cars_df, 'car_id')

    enriched_df = enriched_df.join(models_df, enriched_df.model_id == models_df.model_id, 'left').drop(models_df.model_id)

    enriched_df = enriched_df.join(colors_df, enriched_df.color_id == colors_df.color_id, 'left').drop(colors_df.color_id)

    enriched_df = enriched_df.withColumn('expected_gear', F.round(F.col('speed') / 30))

    output_df = enriched_df.select(
        F.col('event_id'),
        F.col('event_time'),
        F.col('car_id'),
        F.col('driver_id'),
        F.col('car_brand').alias('brand_name'),
        F.col('car_model').alias('model_name'),
        F.col('color_name'),
        F.col('speed'),
        F.col('rpm'),
        F.col('gear'),
        F.col('expected_gear')
    )

    kafka_output = output_df.select(
        F.to_json(F.struct('*')).alias('value')
    )

    query = kafka_output \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'course-kafka:9092') \
        .option('topic', 'samples-enriched') \
        .option('checkpointLocation', '/tmp/checkpoint/enrichment') \
        .start()

    print("Data enrichment started. Processing stream...")
    query.awaitTermination()
    spark.stop()

if __name__ == '__main__':
    enrich_data()
