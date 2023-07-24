import findspark,codecs
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlFunc
from pyspark.sql.types import StructField,StructType,IntegerType,LongType

spark = SparkSession.builder.appName("famousMovie").getOrCreate()

def loadMovieNames():
    movieNames = {}
    with codecs.open("D:\Learning\GIT\datasets\ml-100k//u.ITEM","r",encoding='ISO-8859-1',errors='ignore') as titleFile:
        for line in titleFile:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

schema = StructType ([  \
    StructField("userID",IntegerType(), True),    \
    StructField("movieID",IntegerType(),True),    \
    StructField("rating",IntegerType(),True),     \
    StructField("timeStamp",LongType(),True)  \
])

titleDict = spark.sparkContext.broadcast(loadMovieNames())

moviesDF = spark.read.options(delimiter = '\t').schema(schema).csv("D:\Learning\GIT\datasets\ml-100k//u.data")
moviesDF.createOrReplaceTempView("movies")
famousMovies = spark.sql(\
    "select movieID,count(1) as cnt from movies\
        group by movieID order by count(1) desc"\
            )

def lookUpTitle(movieID):
    return titleDict.value[movieID]

lookUpTitleUDF = sqlFunc.udf(lookUpTitle)

famousMoviesWithTitles = famousMovies.withColumn(\
    "movieTitle",lookUpTitleUDF(sqlFunc.col("movieID"))\
        )

famousMoviesWithTitles.show(10, False)

spark.stop()