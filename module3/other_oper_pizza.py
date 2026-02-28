from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, avg, sum, count, desc
from pyspark.sql.types import DoubleType

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    pizzaDF = spark.read\
      .option("header", "true")\
      .csv("data/pizza_data.csv")

    pizzaDF.show()
    pizzaDF.printSchema()
    print(f"Number of pizzas: {pizzaDF.count()}")

    pizzaCleanDF = pizzaDF.withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType()))
    pizzaCleanDF.show(truncate = False)
    pizzaCleanDF.printSchema()
    print(f"Number of pizzas: {pizzaCleanDF.count()}")

    companiesWithAvgPricesDF = pizzaCleanDF.groupBy("Company")\
      .agg(avg(col("Price")).alias("avgPrice"))
    companiesWithAvgPricesDF.show()

    companiesWithSumPricesDF = pizzaCleanDF.groupBy("Company")\
      .agg(sum(col("Price")).alias("sumPrice"))
    companiesWithSumPricesDF.show()

    companiesWithPizzaCountDF = pizzaCleanDF.select("Company", "Pizza Name")\
      .dropDuplicates()\
      .groupBy("Company")\
      .agg(count(col("Pizza Name")).alias("countPizza"))\
      .orderBy(desc(col("countPizza")))
    companiesWithPizzaCountDF.show()

finally:
    spark.stop()