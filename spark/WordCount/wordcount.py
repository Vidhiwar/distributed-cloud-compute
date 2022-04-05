
import pyspark as ps
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('SimpleApp')
sc = SparkContext(conf=conf)

rdd = sc.textFile('input.txt')
rdd_words = rdd.flatMap(lambda x: x.split(" "))

rdd_map = rdd_words.map(lambda x: (x.lower(),1))
rdd_reduce = rdd_map.reduceByKey(lambda x,y: x+y)

print(rdd_reduce.collect())






