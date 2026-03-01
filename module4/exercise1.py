from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, trim, avg, call_udf
from pyspark.sql.types import IntegerType
import re

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

def countWordsUDF(description: str) -> int:
    if description is None:
        return 0
    return len(re.split(r'\s+', description.strip()))

try:

    # 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)
    netflixDF = spark.read.option("header", "true").csv("data/netflix_titles.csv")
    print("--- 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)")
    netflixDF.show()

    # 2. Policz średnią długość opisu filmu licząc w wyrazach.
    averageDescriptionLengthDF = netflixDF.withColumn("descriptionLength", size(split(trim(col("description")), "\\s+")).cast(IntegerType()))\
      .agg(avg(col("descriptionLength")).alias("averageDescriptionLength"))
    print("--- 2. Policz średnią długość opisu filmu licząc w wyrazach.")
    averageDescriptionLengthDF.show()

    # 3. Policz średnią długość opisu filmu licząc w wyrazach wykorzystująć UDF.
    spark.udf.register("countWordsUDF", countWordsUDF, IntegerType())

    averageDescriptionLengthUDF = netflixDF.withColumn("descriptionLength", call_udf("countWordsUDF", col("description")))\
      .agg(avg(col("descriptionLength")).alias("averageDescriptionLength"))
    print("--- 3. Policz średnią długość opisu filmu licząc w wyrazach wykorzystująć UDF.")
    averageDescriptionLengthUDF.show()

finally:
    spark.stop()