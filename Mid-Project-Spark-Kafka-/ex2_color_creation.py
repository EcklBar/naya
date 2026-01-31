from pyspark.sql import SparkSession
from pyspark.sql import types as T


def create_car_colors():
    # Initialize SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("ColorCreation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Define the schema
    schema = T.StructType([
        T.StructField("color_id", T.IntegerType(), False),
        T.StructField("color_name", T.StringType(), False)
    ])

    # Create data
    data = [
        (1, "Black"),
        (2, "Red"),
        (3, "Gray"),
        (4, "White"),
        (5, "Green"),
        (6, "Blue"),
        (7, "Pink")
    ]

    # Create DataFrame with manually defined schema
    df = spark.createDataFrame(data, schema=schema)

    # Show DataFrame and schema
    df.cache()
    df.show()
    df.printSchema()

    # Save to S3
    df.write.parquet('s3a://spark/data/dims/car_colors', mode='overwrite')

    # Stop the Spark session to release resources
    spark.stop()

if __name__ == "__main__":
    create_car_colors()