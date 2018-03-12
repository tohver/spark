
from pyspark import SparkConf, SparkContext

def MovieNames():
	names={}
	with open("ml-100k/u.ITEM") as f:
		for line in f:
			field=line.split('|')
			names[int(field[0])]=field[1]
	return names


conf=SparkConf().setMaster('local').setAppName('temp_min')
sc=SparkContext(conf=conf)

nameDict=sc.broadcast(MovieNames())


lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
rdd=lines.map (  lambda x: (  int(x.split()[1]), 1  )  )

counter=rdd.reduceByKey(lambda x,y: x+y)
revert=counter.map( lambda x: (x[1], x[0]))
sorted=revert.sortByKey()
named=sorted.map(lambda x : (nameDict.value[x[1]], x[0]))
 

result=named.collect()

for i in result:
	print(i)