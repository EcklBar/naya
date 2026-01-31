'''
Run the verse generator and send it to Kafka.
Each file should have the following structure: {'chapter': i, 'verse': j, 'text': text}

1. Add new column with number of “God” in verse.
2. Filter only verses with number of “God” greater than 1 .
3. Filter only verses that contain “and” .
4. Add new column with number of words in verse.
5. Add new column If the number of words greater than 14,”long”,if between  7 to 14, “medium”, otherwise “small”
6. Remove the column with number of “God” in verse.

'''


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType
from pyspark.sql import functions as F

topics = "bible"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('ex6_store_results') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .getOrCreate()

# ReadStream from kafka
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'course-kafka:9092')\
    .option("subscribe", topics)\
    .load()

df_kafka = df_kafka.select(col("value").cast("string"))


schema = StructType() \
    .add("chapter", StringType()) \
    .add("verse", StringType()) \
    .add("text", StringType())

# change json to dataframe with schema
df_kafka = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")

# Add new column with number of “God” in verse.
df_kafka = df_kafka.withColumn('GodCount', size(split(col('text'), 'God')) - 1)

# Filter only verses with number of “God” greater than 1
df_kafka= df_kafka.where(col('GodCount')>1)

# Filter only verses that contain “and”
df_kafka= df_kafka.filter(df_kafka.text.like('%and%'))

df_kafka= df_kafka.withColumn('Counter_words', size(split(col('text'), ' ')) - 1)


# Add new column If the number of words greater than 14,”long”,if between  7 to 14, “medium”, otherwise “small”
df_kafka= df_kafka.withColumn('length', F.when(col('Counter_words')>14, "long").when(col('Counter_words')>7, 'medium').otherwise('small'))


# Remove the column with number of “God” in verse
df_kafka= df_kafka.drop('GodCount')


df_kafka \
    .writeStream \
    .format("console") \
    .start()\
    .awaitTermination()
