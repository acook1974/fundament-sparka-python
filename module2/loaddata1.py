from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    pizzaDF = spark.read.csv("data/pizza_data.csv", header=True, inferSchema=True)
    pizzaDF.show()
    pizzaDF.printSchema()

finally:
    spark.stop()