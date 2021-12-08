# How to run :
# $SPARK_HOME/bin/spark-submit  ./004_WordCount.py
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Word Count")
sc = SparkContext(conf=conf)

if __name__ == '__main__':
    print("calculating words count ...")
    input_ = sc.textFile("data/spark.txt")
    words = input_.flatMap(lambda line: line.split())
    word_count = words.map(lambda word: (word, 1))
    word_count = word_count.reduceByKey(lambda a, b: a+b)
    word_count = word_count.sortBy(lambda x: x[1]).collect()
    print(word_count)
    