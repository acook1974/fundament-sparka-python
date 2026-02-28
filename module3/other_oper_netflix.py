from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, avg, sum, count, desc, split, explode, trim, sha2
from pyspark.sql.types import DoubleType

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    netflixDF = spark.read\
      .option("header", "true")\
      .csv("data/netflix_titles.csv")

    netflixDF.show(truncate = False)
    netflixDF.printSchema()
    print(f"Number of netflix data: {netflixDF.count()}")
    
    netflixWithoutNullsDF = netflixDF.na.fill("N/A")
    netflixWithoutNullsDF.show(truncate = False)
    netflixWithoutNullsDF.printSchema()
    print(f"Number of netflix data: {netflixWithoutNullsDF.count()}")

    explodedListendInStatsDF = netflixDF.withColumn("listed_in", split(col("listed_in"), ","))\
      .select(col("show_id"), explode(col("listed_in")).alias("singleListedIn"))\
      .withColumn("singleListedIn", trim(col("singleListedIn")))
    explodedListendInStatsDF.show(truncate = False)

    listedInStatsDF = explodedListendInStatsDF.groupBy("singleListedIn")\
      .agg(count(col("show_id")).alias("count"))\
      .orderBy(desc(col("count")))
    listedInStatsDF.show(truncate = False)
    listedInStatsDF.printSchema()

    hashedDirectorDF = netflixWithoutNullsDF.withColumn("directorHash", sha2(col("director"), 256))
    hashedDirectorDF.show(truncate = False)
    hashedDirectorDF.printSchema()

    hashedDirectorDF.write\
      .parquet("data/netflix_hashed_directors.parquet")

finally:
    spark.stop()