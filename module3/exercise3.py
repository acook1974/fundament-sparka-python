from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, regexp_replace
from pyspark.sql.types import DoubleType

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    # 1. Wczytaj do Dataframe’a plik z danymi o pizzach (pizza_data.csv)
    pizzaDF = spark.read.option("header", "true").csv("data/pizza_data.csv")
    print("--- 1. Wczytaj do Dataframe’a plik z danymi o pizzach (pizza_data.csv)")
    pizzaDF.show()
    print(f"Number of pizza data: ${pizzaDF.count()}")

    # 2. Zbadaj strukturę danych (schema, liczba rekordów itd)
    print("--- 2. Zbadaj strukturę danych (schema, liczba rekordów itd)")
    pizzaDF.printSchema()

    # 3. Wybierz średnie pizze i oblicz średnią, minimalną i maksymalną cenę
    mediumPizzaDF = pizzaDF.filter(col("Size").contains("Medium"))\
      .filter(col("Price").isNotNull())\
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType()))\
      .agg(avg(col("Price")).alias("avgPrice"), min(col("Price")).alias("minPrice"), max(col("Price")).alias("maxPrice"))
    mediumPizzaDF.show()

    mediumPizzaDF.printSchema()
finally:
    spark.stop()