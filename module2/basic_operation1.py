from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

nameList = ["Johnatan", "Jakub", "Adrian", "Jill", "Jack", "Jill", "Adam", "Alex", "Steve"]

try:

    print("nameRDD:")
    nameRDD = spark.sparkContext.parallelize(nameList)
    for name in nameRDD.take(5):
        print(name)
    
    print("namesBig:")
    namesBig = nameRDD.map(lambda name: name.upper())
    for name in namesBig.take(5):
        print(name)

    print("namesFiltered:")
    namesFiltered = namesBig.filter(lambda name: len(name) < 5)
    for name in namesFiltered.take(5):
        print(name)

finally:
    spark.stop()