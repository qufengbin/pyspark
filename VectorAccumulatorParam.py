import sys
sys.path.append('/usr/local/spark/python/')
try:
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    from pyspark import AccumulatorParam
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

sc = SparkContext()

class VectorAccumulatorParam(AccumulatorParam):

    def zero(self,value):
        dict1={}
        for i in range(0,len(value)):
            dict1[i] = 0
        return dict1

    def addInPlace(self,val1,val2):
        for i in val1.keys():
            val1[i] += val2[i]
        return val1


rdd1 = sc.parallelize([{0:0.3,1:0.8,2:0.4},{0:0.2,1:0.4,2:0.2}])
vector_acc = sc.accumulator({0:0,1:0,2:0},VectorAccumulatorParam())

def mapping_fn(x):
    global vector_acc
    vector_acc += x

rdd1.foreach(mapping_fn)
print(vector_acc.value)