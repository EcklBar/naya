from pyspark.sql import SparkSession
from pyspark.sql import types as T

def create_car_model():
    # Initialize SparkSession
    spark = SparkSession \
    	.builder \
        .master("local[*]") \
        .appName('ModelCreation') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Define the schema
    schema = T.StructType([
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("car_brand", T.StringType(), False),
        T.StructField("car_model", T.StringType(), False)
    ])

    # Create data
    data = [
		(1, "Mazda", "3"),
		(2, "Mazda", "6"),
		(3, "Toyota", "Corolla"),
		(4, "Hyundai", "i20"),
		(5, "Kia", "Sportage"),
		(6, "Kia", "Rio"),
		(7, "Kia", "Picanto")
	]

    # Create DataFrame with manually defined schema
    df = spark.createDataFrame(data, schema=schema)

    # Show DataFrame and schema
    df.show()
    df.printSchema()

    # Save to S3
    df.write.parquet('s3a://spark/data/dims/car_models', mode='overwrite')

	# Stop the Spark session to release resources
    spark.stop()

if __name__ == "__main__":
    create_car_model()