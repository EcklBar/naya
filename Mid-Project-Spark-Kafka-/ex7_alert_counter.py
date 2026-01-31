from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def count_alerts():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("DataEnrichment") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .getOrCreate()

    # Define the schema
    alert_schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("car_id", T.IntegerType()),
        T.StructField("driver_id", T.IntegerType()),
        T.StructField("brand_name", T.StringType()),
        T.StructField("model_name", T.StringType()),
        T.StructField("color_name", T.StringType()),
        T.StructField("speed", T.IntegerType()),
        T.StructField("rpm", T.IntegerType()),
        T.StructField("gear", T.IntegerType()),
        T.StructField("expected_gear", T.IntegerType())
    ])

    # Read from Kafka
    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("subscribe", "alert-data") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON from Kafka
    alerts_df = stream_df.select(
        F.col("timestamp"),
        F.from_json(F.col("value").cast("string"), alert_schema).alias("data")
    ).select("timestamp", "data.*")

    # Perform aggregations over 15-minute windows
    aggregated_df = alerts_df \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(F.window(F.col("timestamp"), "15 minutes")) \
        .agg(
            F.count("*").alias("num_of_rows"),
            F.count(F.when(F.col("color_name") == "Black", 1)).alias("num_of_black"),
            F.count(F.when(F.col("color_name") == "White", 1)).alias("num_of_white"),
            F.count(F.when(F.col("color_name") == "Gray", 1)).alias("num_of_silver"),
            F.max("speed").alias("maximum_speed"),
            F.max("gear").alias("maximum_gear"),
            F.max("rpm").alias("maximum_rpm")
        )

    # Write to console
    query = aggregated_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("Alert counter started. Aggregating data over 15-minute windows...")
    query.awaitTermination()

if __name__ == "__main__":
    count_alerts()
