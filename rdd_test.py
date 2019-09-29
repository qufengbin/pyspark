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
licenses = sc.textFile('file:///usr/local/spark/README.md')
words = licenses.flatMap(lambda x:x.split(' '))
print(words.take(5))
lowercase = words.map(lambda x:x.lower())
print(lowercase.take(5))
longwords = lowercase.filter(lambda x:len(x) > 12)
print(longwords.take(5))