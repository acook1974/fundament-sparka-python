from pyspark.sql import SparkSession

people = [
    ("John", "Doe", 29, "Male"),
    ("Jane", "Doe", 30, "Female"),
    ("Jim", "Beam", 31, "Male"),
    ("Jill", "Smith", 32, "Female"),
    ("Jack", "Daniels", 33, "Male"),
    ("Jill", "Smith", 34, "Female"),
    ("Jack", "Daniels", 35, "Male"),
    ("Jill", "Smith", 36, "Female"),
    ("Jack", "Daniels", 37, "Male"),
]

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

print(spark)

try:
    people_df = spark.createDataFrame(people, ["first_name", "last_name", "age", "gender"])

    people_df.show()
    people_df.printSchema()

    people_df.select("first_name", "last_name").show()
finally:
    spark.stop()