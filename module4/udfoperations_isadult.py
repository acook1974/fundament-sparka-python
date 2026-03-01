from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

def isAdult(df: DataFrame) -> DataFrame:
    return df.withColumn("isAdult", col("age") >= 18)

try:

    people = [("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82)]

    peopleDF = spark.createDataFrame(people, ["id", "firstName", "lastName", "age"])

    peopleWithAdultsDF = peopleDF.transform(isAdult)
    peopleWithAdultsDF.show()

finally:
    spark.stop()