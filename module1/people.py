from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

print(spark)

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

people_df = spark.createDataFrame(people, ["first_name", "last_name", "age", "gender"])

people_df.show()
people_df.printSchema()

people_df.select("first_name", "last_name").show()