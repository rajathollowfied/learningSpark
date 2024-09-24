import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

conf = SparkConf().setMaster("local").setAppName("BFS")
sc = SparkContext.getOrCreate(conf = conf)
ss = SparkSession.builder.appName("ssBFS").getOrCreate()


startCharacterID = 11619
targetCharacterID = 1289

counter = sc.accumulator(0)

def createStartingRdd():
    inputFile = sc.textFile("D:\Learning\GIT\datasets\Marvel+Graph")
    return inputFile.map(initMapper)
    
def initMapper(lines):
    fields = lines.split()
    heroID = int(fields[0])
    connections = []
    
    for i in fields[1:]:
        connections.append(int(i))
    
    color = 'WHITE'
    distance = 9999
    
    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
        
    return (heroID,(connections, distance, color))

def bfsMapper(rdd):
    characterID = rdd[0]
    connectionData = rdd[1]
    connections = connectionData[0]
    distance = connectionData[1]
    color = connectionData[2]
    
    results = []
    
    if(color == 'GRAY'):
        for i in connections:
            newCharacterID = i
            newDistance = distance+1
            newColor = 'GRAY'
            if(targetCharacterID==i):
                counter.add(1)
            newEntry = (newCharacterID,([],newDistance,newColor))
            results.append(newEntry)
        color = 'BLACK'
    results.append((characterID,(connections, distance, color)))
    return results
    

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 100):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMapper)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (counter.value > 0):  
        print("Hit the target character! From " + str(counter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)

#print(str(mappedRDD.count()) +" "+ str(counter.value))


#inputFile.collect() ['5988 748 1722 3752 4655.........'*n lines]
