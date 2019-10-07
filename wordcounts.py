import sys,re
sys.path.append('/usr/local/spark/python/')
try:
    from pyspark import SparkContext,SparkConf
    from pyspark.sql import SQLContext
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
conf = SparkConf().setAppName('Word Counts')
sc = SparkContext(conf=conf)

# 检查命令行参数
if(len(sys.argv) != 3):
    print("""
本程序统计文档中各单词的出现次数，并返回出现频次最高的 5 个单词的计数

用法：wordcounys.py <输入文件或目录> <输出目录>
    """
    )
    sys.exit(1)
else:
    inputpath = sys.argv[1]
    outputdir = sys.argv[2]

# 对词频计数并排序
wordcounts = sc.textFile("file://" + inputpath)\
    .filter(lambda line: len(line) > 0)\
    .flatMap(lambda line: re.split('\W+',line))\
    .filter(lambda word: len(word) > 0)\
    .map(lambda word: (word.lower(),1))\
    .reduceByKey(lambda v1,v2: v1+v2)\
    .map(lambda x:(x[1],x[0]))\
    .sortByKey(ascending = False)\
    .persist()

wordcounts.saveAsTextFile("file://" + outputdir)
top5words = wordcounts.take(5)
justwords = []
for wordsandcounts in top5words:
    justwords.append(wordsandcounts[1])

print("The top five words are : " + str(justwords))
print("Check the complete output in " + outputdir)
