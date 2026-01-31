from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configuration
NUM_CARS = 20

def generate_cars():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("CarsGenerator") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Define the schema
    schema = T.StructType([
        T.StructField("car_id", T.IntegerType(), False),
        T.StructField("driver_id", T.IntegerType(), False),
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("color_id", T.IntegerType(), False)
    ])

    # Generate cars using PySpark functions (no Python loops!)
    df = spark.range(NUM_CARS)  # Create rows based on NUM_CARS constant

    # Generate car_id (7 digits): use monotonically_increasing_id + random offset
    df = df.withColumn("car_id",
        (F.monotonically_increasing_id() + 1000000 + (F.rand() * 100000).cast("int")).cast("int"))

    # Generate driver_id (9 digits): random between 100000000-999999999
    df = df.withColumn("driver_id",
        (F.rand() * 900000000 + 100000000).cast("int"))

    # Generate model_id (1-7): random integer
    df = df.withColumn("model_id",
        (F.rand() * 7 + 1).cast("int"))

    # Generate color_id (1-7): random integer
    df = df.withColumn("color_id",
        (F.rand() * 7 + 1).cast("int"))

    # Select only the required columns (drop the id from range)
    df = df.select("car_id", "driver_id", "model_id", "color_id")

    # Apply the schema to ensure nullable=False and correct types
    df = spark.createDataFrame(df.rdd, schema=schema)

    # Show DataFrame and schema
    df.show()
    df.printSchema()

    # Save to S3
    df.write.parquet('s3a://spark/data/dims/cars', mode='overwrite')

    # Stop the Spark session to release resources
    spark.stop()

if __name__ == "__main__":
    generate_cars()