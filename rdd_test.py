import sys
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
# locations = sc.parallelize([('city','Beijing',1),('state','SHIGEZHUANG',2),('zip','000000',3),('country','China',4)])
# bylocno = locations.keyBy(lambda x: x[2])
# print(bylocno.collect())

# mapValues(),flatMapValues()
# locwtemps = sc.parallelize(['Beijing,71|72|73|72|70','Shanghai,46|42|40|37|39','Tianjin,50|48|51|43|44'])
# kvpairs = locwtemps.map(lambda x: x.split(','))
# print('kvpairs: ',kvpairs.take(4))
# locwtemplist = kvpairs.mapValues(lambda x: x.split('|')).mapValues(lambda x: [int(s) for s in x])
# print('locwtemplist: ',locwtemplist.take(3))
# locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')).map(lambda x:(x[0],int(x[1])))
# print('locwtemps: ',locwtemps.take(4))

# groupByKey()
# locwtemps = sc.parallelize(['Beijing,71|72|73|72|70','Shanghai,46|42|40|37|39','Tianjin,50|48|51|43|44'])
# kvpairs = locwtemps.map(lambda x: x.split(','))
# print('kvpairs: ',kvpairs.collect())
# locwtemplist = kvpairs.mapValues(lambda x: x.split('|')).mapValues(lambda x: [int(s) for s in x])
# print('locwtemplist: ',locwtemplist.collect())
# locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')).map(lambda x:(x[0],int(x[1])))
# print('locwtemps: ',locwtemps.collect())
# grouped = locwtemps.groupByKey()
# print('grouped: ',grouped.collect())
# avgtemps = grouped.mapValues(lambda x: sum(x)/len(x))
# print('avgtemps: ',avgtemps.collect())

# reduceByKey()
# locwtemps = sc.parallelize(['Beijing,71|72|73|72|70','Shanghai,46|42|40|37|39','Tianjin,50|48|51|43|44'])
# kvpairs = locwtemps.map(lambda x: x.split(','))
# locwtemplist = kvpairs.mapValues(lambda x: x.split('|')).mapValues(lambda x: [int(s) for s in x])
# locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')).map(lambda x:(x[0],int(x[1])))
# temptups = locwtemps.mapValues(lambda x: (x,1))
# print('temptups: ',temptups.collect())
# inputstoavg = temptups.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
# print('inputstoavg: ',inputstoavg.collect())
# averages = inputstoavg.map(lambda x:(x[0],x[1][0]/x[1][1]))
# print('averages: ',averages.collect())

# foldByKey()
# locwtemps = sc.parallelize(['Beijing,71|72|73|72|70','Shanghai,46|42|40|37|39','Tianjin,50|48|51|43|44'])
# kvpairs = locwtemps.map(lambda x: x.split(','))
# locwtemplist = kvpairs.mapValues(lambda x: x.split('|')).mapValues(lambda x: [int(s) for s in x])
# locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')).map(lambda x:(x[0],int(x[1])))
# maxbycity = locwtemps.foldByKey(0,lambda x,y: x if x > y else y)
# print('maxbycity: ',maxbycity.collect())

# sortByKey()
# locwtemps = sc.parallelize(['Beijing,71|72|73|72|70','Shanghai,46|42|40|37|39','Tianjin,50|48|51|43|44'])
# kvpairs = locwtemps.map(lambda x: x.split(','))
# locwtemplist = kvpairs.mapValues(lambda x: x.split('|')).mapValues(lambda x: [int(s) for s in x])
# locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')).map(lambda x:(x[0],int(x[1])))
# sortedbykey = locwtemps.sortByKey()
# print('sortedbykey: ',sortedbykey.collect())
# sortedbyval = locwtemps.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
# print('sortedbyval: ',sortedbyval.collect())

# 连接操作
stores = sc.parallelize([(100,'Beijing'),(101,'Shanghai'),(102,'Tianjin'),(103,'Taiyuan')])
salespeople = sc.parallelize([(1,'Tom',100),(2,'Karen',100),(3,'Paul',101),(4,'Jimmy',102),(5,'Jack',None)])

# join()
# print(salespeople.keyBy(lambda x: x[2]).join(stores).collect())

# leftOuterJoin()
# leftjoin = salespeople.keyBy(lambda x: x[2]).leftOuterJoin(stores)
# print("leftjoin: ",leftjoin.collect())
# print(leftjoin.filter(lambda x: x[1][1] is None).map(lambda x: "salesperson " + x[1][0][1] + " has no store").collect())

# rightOuterJoin()
# print(
# salespeople.keyBy(lambda x: x[2])\
#     .rightOuterJoin(stores)\
#     .filter(lambda x: x[1][0] is None)\
#     .map(lambda x: x[1][1] + " store has no salespeople")\
#     .collect()
# )

# fullOuterJoin()
# print(
#     salespeople.keyBy(lambda x: x[2]).fullOuterJoin(stores).filter(lambda x: x[1][0] is None or x[1][1] is None).collect()
# )

# cogroup()
# print(salespeople.keyBy(lambda x: x[2]).cogroup(stores).take(1))
# print('----------------')
# print(salespeople.keyBy(lambda x: x[2]).cogroup(stores).mapValues(lambda x: [item for sublist in x for item in sublist]).collect())

# cartesian()
print(salespeople.keyBy(lambda x: x[2]).cartesian(stores).collect())
print('----------------')
print(salespeople.keyBy(lambda x: x[2]).cartesian(stores).count())