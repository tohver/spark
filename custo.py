from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster('local').setAppName('temp_min')
sc=SparkContext(conf=conf)

def spl(line):
    a=line.split(',')
    custID=a[0]
    amouth=a[2]
    return (int(custID), float(amouth))

a=sc.textFile('file:///SparkCourse/customer-orders.csv')
data=a.map(spl).reduceByKey(lambda x,y: x+y)
datasorted=data.map(lambda x: (x[1], x[0])).sortByKey()

b=datasorted.collect()

for i in b:
    print ('Customer ID: ', i[1], ' spent: {:.2f}'.format(i[0]), ' USD', sep='')
    