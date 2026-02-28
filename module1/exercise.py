from pyspark.sql import SparkSession

people = [
    ("Artur", "Kowalski", "65123012345", 62, "M", 12300),
    ("Anna", "Nowak", "65123012346", 58, "F", 10200),
    ("Piotr", "Janicki", "65123012347", 45, "M", 15000),
    ("Barbara", "Bukowska", "65123012348", 38, "F", 12000),
    ("Tomasz", "Pasierbicki", "65123012349", 25, "M", 18000),
    ("Ewa", "Animowski", "65123012350", 22, "F", 11000),
    ("Jan", "Kutek", "65123012351", 18, "M", 13000),
    ("Maria", "Misiek", "65123012352", 15, "F", 14000),
]

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

print(spark)

try:
    people_df = spark.createDataFrame(people, ["first_name", "last_name", "pesel", "age", "gender", "salary"])

    people_df.show()

    ile = people_df.count()
    people_df.select("last_name", "gender", "salary").show(ile)

finally:
    spark.stop()