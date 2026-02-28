from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

numbersList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

try:
    
    print("numbersList:")
    numbersRDD = spark.sparkContext.parallelize(numbersList)
    print(numbersRDD.collect())
    
    print("numbersSum:")
    numbersSum = numbersRDD.sum()
    print(f"Suma: {numbersSum}")

    print("numbersReduce:")
    numbersReduce = numbersRDD.reduce(lambda a, b: a * b)
    print(f"Redukcja: {numbersReduce}")

finally:
    spark.stop()