from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, explode, trim, regexp_extract, asc, desc

spark = SparkSession.builder\
    .appName("fundament-sparka")\
    .master("local[*]")\
    .getOrCreate()

try:

    # 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)
    netflixDF = spark.read.option("header", "true").csv("data/netflix_titles.csv")
    print("--- 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)")
    netflixDF.show()

    # 2. Zbadaj strukturę danych (schema, liczba rekordów itd)
    netflixDF.printSchema()
    print("--- 2. Zbadaj strukturę danych (schema, liczba rekordów itd)")
    print(f"Number of netflix data: ${netflixDF.count()}")

    # 3. Zamień nulle na napisy „NULL”
    netflixWithoutNullsDF = netflixDF.na.fill("NULL")
    print("--- 3. Zamień nulle na napisy „NULL”")
    netflixWithoutNullsDF.show()

    # 4. Zbadaj ile jest filmów z podziałem na rodzaje (kolumna type)
    netflixWithTypeDF = netflixDF.groupBy("type")\
      .agg(count(col("show_id")).alias("countFilms"))\
      .filter(col("type").isNotNull())
    print("--- 4. Zbadaj ile jest filmów z podziałem na rodzaje (kolumna type)")
    netflixWithTypeDF.show()

    # 5. Zbadaj ile tytułów nakręciłi poszczególni directorzy (kolumna director)
    netflixWithDirectorDF = netflixDF\
      .withColumn("director", split(col("director"), ","))\
      .select(col("title"), explode(col("director")).alias("singleDirector"))\
      .withColumn("singleDirector", trim(col("singleDirector")))\
      .distinct()\
      .groupBy("singleDirector")\
      .agg(count(col("title")).alias("countFilms"))\
      .orderBy(desc(col("countFilms")))
    print("--- 5. Zbadaj ile tytułów nakręcili poszczególni directorzy (kolumna director)")
    netflixWithDirectorDF.show()

    # 6. Zrób statystyki z podziałem na lata – kiedy nakręcono ile filmów (wyświetl w kolejności chronologicznej).
    netflixWithYearDF = netflixDF.groupBy("release_year")\
      .agg(count(col("show_id")).alias("countFilms"))\
      .filter(regexp_extract(col("release_year"), "^\\d{4}$", 0) != "")\
      .orderBy(asc(col("release_year")))
    print("--- 6. Zrób statystyki z podziałem na lata – kiedy nakręcono ile filmów (wyświetl w kolejności chronologicznej).")
    netflixWithYearDF.show()

    # 7. Określ ile jest filmów przypisanych do poszczególnych gatunków (listed_in)
    listedInStatsDF = netflixDF.withColumn("listed_in", split(col("listed_in"), ","))\
      .select(col("show_id"), explode(col("listed_in")).alias("singleListedIn"))\
      .withColumn("singleListedIn", trim(col("singleListedIn")))\
      .groupBy("singleListedIn")\
      .agg(count(col("show_id")).alias("countFilms"))\
      .orderBy(asc(col("singleListedIn")))
    print("--- 7. Określ ile jest filmów przypisanych do poszczególnych gatunków (listed_in)")
    listedInStatsDF.show(truncate = False)

finally:
    spark.stop()