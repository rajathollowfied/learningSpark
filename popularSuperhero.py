import findspark,codecs
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlFunc
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

spark = SparkSession.builder.appName("popularHero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(),True),
    StructField("name", StringType(),True)
])

names = spark.read.options(delimiter = ' ').schema(schema).csv("D:\Learning\GIT\datasets\Marvel+Names")
lines = spark.read.text("D:\Learning\GIT\datasets\Marvel+Graph")

connections = lines.withColumn("id",sqlFunc.split(sqlFunc.col("value")," ")[0])\
    .withColumn("connections",sqlFunc.size(sqlFunc.split(sqlFunc.col("value")," "))-2)

connections.createOrReplaceTempView("connections")
names.createOrReplaceTempView("names")

popularHero = spark.sql("SELECT ID,SUM(CONNECTIONS) AS CNT FROM CONNECTIONS GROUP BY ID ORDER BY CNT DESC LIMIT 1")
popularHero.createOrReplaceTempView("popularHero")

heroWithName = spark.sql("SELECT a.NAME,b.CNT FROM NAMES a \
                         INNER JOIN POPULARHERO b   \
                         ON a.ID=b.ID")

print("The most popular Super-Hero is",heroWithName.collect()[0][0],"with",str(heroWithName.collect()[0][1]),"connections.")

spark.stop()