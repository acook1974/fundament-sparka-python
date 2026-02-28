from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

peopleData = [
    ("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82)
]

jobsData = [("1", "teacher"), ("2", "programmer"), ("3", "teacher"), ("4", "architect"), ("5", "director"),
      ("6", "director"), ("7", "architect"), ("8", "programmer"), ("9", "programmer"), ("10", "unemployed"),
      ("11", "teacher"), ("12", "director"), ("13", "programmer"), ("19", "programmer"), ("20", "teacher")]

try:

    peopleDF = spark.createDataFrame(peopleData, ["id", "firstName", "lastName", "age"])
    jobsDF = spark.createDataFrame(jobsData, ["id", "job"])

    peopleDF.show()
    jobsDF.show()

    innerJoin = peopleDF.join(jobsDF, "id", "left")
    innerJoin2 = peopleDF.join(jobsDF, "id", "inner")
    leftJoin = peopleDF.join(jobsDF, "id", "left_outer")
    rightJoin = peopleDF.join(jobsDF, "id", "right_outer")
    fullJoin = peopleDF.join(jobsDF, "id", "full")
    crossJoin = peopleDF.crossJoin(jobsDF)
    semiJoin = peopleDF.join(jobsDF, "id", "left_semi")
    antiJoin = peopleDF.join(jobsDF, "id", "left_anti")

    innerJoin.show()
    innerJoin2.show()
    leftJoin.show()
    rightJoin.show()
    fullJoin.show(40)
    crossJoin.show(100)
    print(crossJoin.count())
    semiJoin.show()
    antiJoin.show()

finally:
    spark.stop()