from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

peopleData = [
    ("John", "Doe", "M", 25),
    ("Jane", "Smith", "F", 30),
    ("Jim", "Beam", "M", 35),
    ("Alice", "Johnson", "F", 28),
    ("Bob", "Brown", "M", 32),
    ("Charlie", "Davis", "M", 29),
    ("Diana", "Evans", "F", 27),
    ("Ethan", "Foster", "M", 31),
    ("Fiona", "Garcia", "F", 26),
    ("George", "Harris", "M", 33),
    ("Helen", "Wilson", "F", 24),
]

try:

    peopleRDD = spark.sparkContext.parallelize(peopleData)
    for person in peopleRDD.take(5):
        print(person)

    sumAgeMen = peopleRDD.filter(lambda person: person[2] == "M").map(lambda person: person[3]).sum()
    sumAgeWomen = peopleRDD.filter(lambda person: person[2] == "F").map(lambda person: person[3]).sum()
    agesRDD = peopleRDD.map(lambda person: person[3])

    print(f"Sum of ages of men: {sumAgeMen}")
    print(f"Sum of ages of women: {sumAgeWomen}")
    print(f"Min age: {agesRDD.min()}")
    print(f"Max age: {agesRDD.max()}")

finally:
    spark.stop()