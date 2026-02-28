from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    pizzaDF = spark.read.csv("data/pizza_data.csv", header=True, inferSchema=True)
    
    pizzaDFWithFiltered = pizzaDF.filter(pizzaDF["Type"].contains("Cheese"))
    pizzaCount = pizzaDFWithFiltered.count()

    pizzaDFWithFiltered.show(n=pizzaCount, truncate=False)
    pizzaDFWithFiltered.printSchema()

    print(f"Number of pizzas: {pizzaCount}")

finally:
    spark.stop()