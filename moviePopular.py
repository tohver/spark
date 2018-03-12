
from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster('local').setAppName('temp_min')
sc=SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
rdd=lines.map (  lambda x: (  int(x.split()[1]), 1  )  )

counter=rdd.reduceByKey(lambda x,y: x+y)
revert=counter.map( lambda x: (x[1], x[0]))
result=revert.sortByKey().collect()

for i in result:
	print(i)