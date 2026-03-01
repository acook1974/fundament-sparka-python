import math
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, IntegerType
from pyspark.sql.functions import udf, col, lit, call_udf

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

def capitalization_and_contribution_udf(
    start_capital: int, years_amount: int, interest_rate: int, contribution: int
) -> float:
    if any(v is None for v in (start_capital, years_amount, interest_rate, contribution)):
        return float("nan")
    if start_capital < 0 or years_amount < 0 or interest_rate < 0 or contribution < 0:
        return float("nan")
    interest_rate_per_year = interest_rate / 100.0
    growth_factor = (1 + interest_rate_per_year) ** years_amount
    final_amount = (
        start_capital * growth_factor
        + contribution * (growth_factor - 1) / interest_rate_per_year
    )
    return round(final_amount, 2)

try:

    spark.udf.register("capitalizationAndContributionUDF", capitalization_and_contribution_udf, DoubleType())

    peopleDF = spark.read\
      .option("header", "true")\
      .csv("data/money_saving.csv")

    peopleWithMoneyDF = peopleDF.withColumn("money", col("money").cast(LongType()))\
      .withColumn("interest", col("interest").cast(IntegerType()))\
      .withColumn("10years", call_udf("capitalizationAndContributionUDF", col("money"), lit(10), col("interest"), lit(1000)))\
      .withColumn("20years", call_udf("capitalizationAndContributionUDF", col("money"), lit(20), col("interest"), lit(1000)))\
      .withColumn("40years", call_udf("capitalizationAndContributionUDF", col("money"), lit(40), col("interest"), lit(1000)))\
      .withColumn("60years", call_udf("capitalizationAndContributionUDF", col("money"), lit(60), col("interest"), lit(1000)))\

    peopleWithMoneyDF.show()

finally:
    spark.stop()