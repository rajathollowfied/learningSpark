from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlFunc
from pyspark.sql.types import StructField,StructType,IntegerType,LongType

spark = SparkSession.builder.appName("famousMovie").getOrCreate()

schema = StructType ([  \
    StructField("userID",IntegerType(), True),    \
    StructField("movieID",IntegerType(),True),    \
    StructField("rating",IntegerType(),True),     \
    StructField("timeStamp",LongType(),True)  \
])

moviesDF = spark.read.options(delimiter = '\t').schema(schema).csv("D:\Learning\GIT\datasets\ml-100k//u.data").crea

famousMovie = spark.sql("")

spark.stop()