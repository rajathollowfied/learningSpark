import findspark
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

popularHero = spark.sql("SELECT ID,SUM(CONNECTIONS) AS CNCTNS FROM CONNECTIONS GROUP BY ID  HAVING SUM(CONNECTIONS) < 2 ORDER BY CNCTNS ASC")

heroWithName = popularHero.join(names,"id")
heroWithName.show()
results = heroWithName.collect()
print("The most obscure heroes are - ")
for hero in results:
    print(hero)

spark.stop()