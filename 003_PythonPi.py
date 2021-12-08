# How to run :
# $SPARK_HOME/bin/spark-submit  ./003_PythonPi.py
import random
from time import time
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("Spark Pi")
sc = SparkContext(conf=conf)

if __name__ == '__main__':
    print("Let's calculate pi")
    partition = 2
    n = int(1e7) * partition 
    in_square = in_circle = 0
    
    def f(_): 
        x = random.random() 
        y = random.random() 
        dist = (x ** 2 + y ** 2) **.5
        return 1 if dist < 1 else 0
    
    count  = sc.parallelize(range(n), partition)\
             .map(f).reduce(lambda a, b: a+b)
    
    print(f"Pi = {4 * (count / n)}")