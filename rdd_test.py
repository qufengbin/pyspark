import os
import sys
import logging
sys.path.append('/usr/local/spark/python/')
try:
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
sc = SparkContext()

# 转化操作
# map()
# map_rdd=sc.textFile('file:///usr/local/spark/README.md')
# print(map_rdd.take(5))
# map_rdd_new=map_rdd.map(lambda x:x.split(' '))
# print(map_rdd_new.take(5))

# flatMap()
# flat_map_rdd=sc.textFile('file:///usr/local/spark/README.md')
# print(flat_map_rdd.take(5))
# map_rdd_new=flat_map_rdd.flatMap(lambda x:x.split(' '))
# print(map_rdd_new.take(5))

# filter()
# licenses = sc.textFile('file:///usr/local/spark/README.md')
# words = licenses.flatMap(lambda x:x.split(' '))
# print(words.take(5))
# lowercase = words.map(lambda x:x.lower())
# print(lowercase.take(5))
# longwords = lowercase.filter(lambda x:len(x) > 12)
# print(longwords.take(5))

# distinct()
# licenses = sc.textFile('file:///usr/local/spark/README.md')
# words = licenses.flatMap(lambda x : x.split(' '))
# lowercase = words.map(lambda x : x.lower())
# allwords = lowercase.count()
# diswords = lowercase.distinct().count()
# print ("Total words : {} ,Distinct words: {}".format(allwords,diswords))

# groupBy()
# licenses = sc.textFile('file:///usr/local/spark/README.md')
# words = licenses.flatMap(lambda x: x.split(' ')).filter(lambda x:len(x) > 0)
# groupbyfirstletter = words.groupBy(lambda  x: x[0].lower)
# print(groupbyfirstletter.take(1))

# sortBy()
# readme = sc.textFile('file:///usr/local/spark/README.md')
# words = readme.flatMap(lambda x:x.split(' ')).filter(lambda x:len(x) > 0)
# sortbyfirstletter = words.sortBy(lambda x:x[0].lower(),ascending=False)
# print(sortbyfirstletter.take(5))

# 行动操作
# count()
# licenses = sc.textFile('file:///usr/local/spark/licenses')
# words = licenses.flatMap(lambda x: x.split(' '))
# print(words.count())

# collect()
# licenses = sc.textFile('file:///usr/local/spark/licenses')
# words = licenses.flatMap(lambda x: x.split(' '))
# print(words.collect())

# take()
# licenses = sc.textFile('file:///usr/local/spark/licenses')
# words = licenses.flatMap(lambda x: x.split(' '))
# print(words.take(5))

# top()
# licenses = sc.textFile('file:///usr/local/spark/licenses')
# words = licenses.flatMap(lambda x: x.split(' '))
# print(words.distinct().top(5))

# first()
# readme = sc.textFile('file:///usr/local/spark/README.md')
# words = readme.flatMap(lambda x:x.split(' ')).filter(lambda x:len(x) > 0)
# print(words.distinct().first())
# print(words.distinct().take(1))

# reduce(),fold()
# numbers = sc.parallelize([1,2,3,4,5,6,7,8,9])
# print(numbers.reduce(lambda x,y: x+y))
# print(numbers.fold(0,lambda x,y: x+y))
# empty = sc.parallelize([])
# print(empty.reduce(lambda x,y: x+y))
# print(empty.getNumPartitions())
# print(empty.fold(1,lambda x,y: x+y))

# foreach()
# def printfunc(x):
#     print(x)
# licenses = sc.textFile('file:///usr/local/spark/licenses')
# longwords = licenses.flatMap(lambda x: x.split(' ')).filter(lambda x: len(x) > 12)
# longwords.foreach(lambda x: printfunc(x))

# PairRDD
# keys()
# kvpairs = sc.parallelize([('city','Beijing'),('state','SHIGEZHUANG'),('zip','000000'),('country','China')])
# print(kvpairs.keys().collect())

# values()
# kvpairs = sc.parallelize([('city','Beijing'),('state','SHIGEZHUANG'),('zip','000000'),('country','China')])
# print(kvpairs.values().collect())

# keyBy()
locations = sc.parallelize([('city','Beijing',1),('state','SHIGEZHUANG',2),('zip','000000',3),('country','China',4)])
bylocno = locations.keyBy(lambda x: x[2])
print(bylocno.collect())