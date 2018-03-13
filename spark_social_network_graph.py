from pyspark import SparkContext, SparkConf
conf=SparkConf().setMaster('local').setAppName("Heroes")
sc=SparkContext(conf=conf)

# the two, we wish to find the degree of separation between
startID=5306  #SpiderMan
targetID=14  #Adam

#accumulator (=alarm) ;), signal when we're done
hitCounter=sc.accumulator(0)

# bfs node
def toBFS(line):
    fields=line.split()
    ID=int(fields[0]) 
    connected=[]
    for i in fields[1:]:
        connected.append(int(i))
        
    color='WHITE'
    distance=9999
    
    if ID==startID:
        color='GRAY'
        distance=0
    
    return (ID, (connected, distance, color))

def startRDD():
    inputFile=sc.textFile('file:///sparkcourse/marvel-graph.txt')
    return inputFile.map(toBFS)

def bfsMap(node):
    ID=node[0]
    data=node[1]
    connected=data[0]
    distance=data[1]
    color=data[2]
    
    #expand?
    
    results=[]
    if color=='GRAY':
        for i in connected:
            newID=i
            newDistance=distance+1
            newColor='GRAY'
            if targetID==i:
                hitCounter.add(1)
            
            newEntry=(newID, ([], newDistance, newColor))
            results.append(newEntry)
        
        #if processed:
        color='BLACK'
    # don't lose the input node!
    
    results.append((ID, (connected, distance, color)))
    return results
    
def bsfReduce(data1, data2):
    edges1=data1[0]
    edges2=data2[0]
    distance1=data1[1]
    distance2=data2[1]
    color1=data1[2]
    color2=data2[2]
    
    distance=9999
    color='WHITE'
    edges=[]
    
    # See if one is the original node with its connections.
    # If so preserve them.

    if len(edges1)>0:
        edges.extend(edges1)
    if len(edges2)>0:
        edges.extend(edges2)
    
    #preserve min. distance
    if distance1<distance:
        distance=distance1
    if distance2<distance:
        distance=distance2
    
    #keep darkest color
    if color1=='WHITE' and (color2=='GRAY' or color2=='BLACK'):
        color=color2
    
    if color1=='GRAY' and color2=='BLACK':
        color=color2
    
    if color2=='WHIE' and (color1=='GRAY' or color1=='BLACK'):
        color=color1
        
    if color2=='GRAY' and color1=='BLACK':
        color=color1
        
    return (edges, distance, color)

iterationRDD=startRDD()

for i in range(10): # no raczej nie będzie dłuższych distance
    print('running iteration: ',i+1, sep='')
    
    #if we find the wanted node as GRAY, increment accumulator
    mapped=iterationRDD.flatMap(bfsMap)
    
    #mapped.count() - act. to update tha accumulator
    print('Processing ', mapped.count(), ' values.', sep='')
    
    if(hitCounter.value>0):
        print('Done! Its: ', hitCounter.value)
        
        break
    iterationRDD=mapped.reduceByKey(bsfReduce)
        