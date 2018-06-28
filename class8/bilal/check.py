import os
os.environ["SPARK_HOME"] = "/usr/local/Cellar/spark-2.3.1-bin-hadoop2.7"

from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("/Users/BilalMustafa/PycharmProjects/bilal/input.txt", 1)
    with open("/Users/BilalMustafa/PycharmProjects/bilal/input.txt") as f:
        list1 = f.readline().split(',')

    nums = sc.parallelize(list1)

    counts = lines.flatMap(lambda x: x.split(',')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    counts.saveAsTextFile("wordcount output")

    #nums = sc.parallelize(counts)
    nums.saveAsTextFile("output1")
    sc.stop()