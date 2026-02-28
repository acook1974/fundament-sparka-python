from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, IntegerType
from pyspark.sql.functions import call_udf, lit

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

def InterestCapitalizationUDF(startCapital: int, yearsAmount: int, interestRate: int) -> float:
    return float(startCapital * (1 + interestRate / 100) ** yearsAmount)

try:

    spark.udf.register("InterestCapitalizationUDF", InterestCapitalizationUDF, DoubleType())

    peopleDF = spark.read\
      .option("header", "true")\
      .csv("data/money_saving.csv")\
      .toDF("id", "startCapital", "interestRate")

    peopleWithMoneyDF = peopleDF.withColumn("startCapital", col("startCapital").cast(LongType()))\
      .withColumn("interestRate", col("interestRate").cast(IntegerType()))\
      .withColumn("10years", call_udf("interestCapitalizationUDF", col("startCapital"), lit(10), col("interestRate")))\
      .withColumn("20years", call_udf("interestCapitalizationUDF", col("startCapital"), lit(20), col("interestRate")))\
      .withColumn("40years", call_udf("interestCapitalizationUDF", col("startCapital"), lit(40), col("interestRate")))\
      .withColumn("60years", call_udf("interestCapitalizationUDF", col("startCapital"), lit(60), col("interestRate")))

    peopleWithMoneyDF.show()

finally:
    spark.stop()