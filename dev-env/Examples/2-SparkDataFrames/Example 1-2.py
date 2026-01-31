'''
Each record in the "dessert" dataset describes a group visit at a restaurant. Read the data and answer the questions below.
drop the id
change columns:
'day.of.week' -> 'weekday'
'num.of.guest's -> 'num_of_guests'
'dessert' -> 'purchase'
'hour' ->  'shift'
'''

from pyspark import SparkContext
sc = SparkContext.getOrCreate()


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Read a csv file

# option 1
dessert = spark.read.csv(r"/home/naya/Notebooks/dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

dessert.show(5)
dessert.printSchema()


#option 2


dessert_rdd = sc\
    .textFile(r"/home/naya/Notebooks/dessert.csv")\
    .map(lambda line: line.split(','))\
    .zipWithIndex()\
    .filter(lambda tup: tup[1] > 0)\
    .map(lambda tup: [tup[0][1],           # weekday
                      int(tup[0][2]),      # num_of_guests
                      tup[0][3],           # shift
                      int(tup[0][4]),      # table
                      tup[0][5]=='TRUE'])  # purchase

columns = ['weekday', 'num_of_guests', 'shift', 'table', 'purchase']
dessert = spark.createDataFrame(dessert_rdd,
                                schema=columns)
dessert.show(5)