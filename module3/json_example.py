from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

json_list = [
    """{"name": "marek","time": 1469501675}""", 
    """{"name": "kasia","time": 1469501623}"""
]

dataSchema = StructType([
    StructField("name", StringType(), True),
    StructField("time", IntegerType(), True)
])

try:
    
    jsonDF = spark.createDataFrame([(s,) for s in json_list], ["value"])
    jsonDF.show(truncate = False)
    jsonDF.printSchema()

    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).show(truncate = False)
    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).printSchema()

    fromJsonDF = jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).select("jsonData.*")
    fromJsonDF.show(truncate = False)
    fromJsonDF.printSchema()

finally:
    spark.stop()