from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster('local').setAppName('FriendsByAge')
sc=SparkContext(conf=conf)

def parseLine(line):
    fields=line.split(',')
    age=int(fields[2])
    numFriends=int(fields[3])
    return (age, numFriends)

lines=sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd=lines.map(parseLine)
totalByAge=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averagesByAge=totalByAge.mapValues(lambda x:x[0]/x[1])
results=averagesByAge.collect()
a=results.sort()

for result in results:
    print ('People in age of: ', result[0], ' have in average: {:.0f}'.format(result[1]), ' friends', sep='')