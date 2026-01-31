'''
How many visitors purchased a dessert?
What is the average table?
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(r"/home/naya/Notebooks/dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#################################################

dessert\
    .where(dessert.purchase)\
    .agg({'num_of_guests': 'sum', 'table': 'mean'})\
    .show()