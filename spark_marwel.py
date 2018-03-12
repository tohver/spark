            
from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster('local').setAppName('heroes')
sc=SparkContext(conf=conf)
 
def spl(line):
    a=line.split()
    return (int(a[0], len(a)-1))

def names(line):
    a=line.split('\"')
    return (int(a[0]), a[1].encode('utf8'))

names = sc.textFile("file:///SparkCourse/Marvel-names.txt")
namesRdd=names.map(names)

lines= sc.textFile("file:///SparkCourse/Marvel-graph.txt")
rows=lines.map(spl)

friends=rows.reduceByKey(lambda x,y: x+y)
rev=friends.map(lambda x: (x[1],x[0]))

popular=rev.max()
popName=namesRdd.lookup(popular[1])[0]

prnt (popName,' is the most popular superhero, with ', str(popular[0]),' " friends"', sep='')