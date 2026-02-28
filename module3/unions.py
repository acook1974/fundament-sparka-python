from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    pizza1DF = spark.read.option("header", "true").csv("data/pizza_data_half.csv")
    pizza2DF = spark.read.option("header", "true").csv("data/pizza_data_half2.csv")

    pizzaUnionDF = pizza1DF.union(pizza2DF)
    pizzaUnionDFByName = pizza1DF.unionByName(pizza2DF)
    
    pizza1DFCount = pizza1DF.count()
    pizza2DFCount = pizza2DF.count()
    pizzaUnionCount = pizzaUnionDF.count()

    print(f"Number of pizzas in pizza1DF: {pizza1DFCount}")
    print(f"Number of pizzas in pizza2DF: {pizza2DFCount}")
    print(f"Number of pizzas in pizzaUnionDF: {pizzaUnionCount}")

    pizzaUnionDF.show()
    pizzaUnionDFByName.show()

finally:
    spark.stop()