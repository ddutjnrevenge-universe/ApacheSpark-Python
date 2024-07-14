from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

# ID of characters we wish to find the degree of separation between
startCharacterID = 2664 #"IRON MAN/TONY STARK "
# targetCharacterID = 859 #"CAPTAIN AMERICA" - 1 step
# try a more unpopular character
# targetCharacterID = 903 #"CARLY"
# startCharacterID = 903 #"CARLY"
# startCharacterID = 997 #"CHAN, SARAH"
targetCharacterID = 97 #"CH'OD"
# startCharacterID =10678
# targetCharacterID =10679 
# Accumulator to signal when we find the target character during BFS traversal
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
    return (heroID, (connections, distance, color))

def createStartingRdd():
    inputFile = sc.textFile("data/Marvel-Graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GRAY'):
        for connection in connections: 
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #color node black if already processed
        color = 'BLACK'

    #  emit input node so we don't lose it
    results.append( (characterID, (connections, distance, color)) )
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

    # see if one is the original node with its connections
    # if so preserve them
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # preserve minimum distance
    if (distance1 < distance):
        distance = distance1
    if (distance2 < distance):
        distance = distance2
    
    # preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2
    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2
    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1
    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
    
    return (edges, distance, color)

# Main program
iterationRDD = createStartingRdd()

for iteration in range(0,20):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices to darken or reducne distances in Reduce stage
    #  If we encounter the node we're looking for as a GRAY node
    #  increment our accumulator to signal that we're done
    mapped = iterationRDD.flatMap(bfsMap)
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) + " different direction(s).")
        break
    # reduce combines data for each charID, preserve darkest node andd shortest path
    iterationRDD = mapped.reduceByKey(bfsReduce)

# hitCounter value is the number of paths to the target character
# Intepretation of iteration times: The number of steps to reach the target character
