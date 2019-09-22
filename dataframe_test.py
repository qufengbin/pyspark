import os
import sys
import logging
sys.path.append('/usr/local/spark/python/')
try:
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    from pyspark.sql.types import Row
    from pyspark.sql.functions import col
    from pyspark.sql.functions import desc
    from pyspark.sql.functions import avg
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

print("=====================simple dataframe test==================")\

print("生成数据list")
rows =[Row(name="Jack", age="37", group="qe"),
    Row(name="John", age="25", group="qe"),
    Row(name="Tom", age="34", group="qa"),
    Row(name="Phoenix", age="29", group="qe"),
    Row(name="Rose", age="23", group="qa")]
sc = SparkContext()
sqlContext = SQLContext(sc)

print("####生成DataFrame####")
data = sqlContext.createDataFrame(rows)

print("####展示数据####")
data.show()

print("####打印schema模式名称####")
data.printSchema()

print("####选择某一列查看####")
data.select("name").show()
data.select("name","age",col("group").alias("Group")).show()

print("####根据条件筛选显示####")
data.filter(col("age") >= 32).orderBy("name").show()
data.select("name","age").where(col("age") >= 32).orderBy("name").show()
data.select("name","age").where(col("age") >= 32).orderBy(desc("name"),"age").show()

print("####对DataFrame进行分组和函数计算####")
data.groupBy("group").count().show()
data.groupBy("group").agg(avg("age")).show()
