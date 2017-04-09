import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("./book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda (x, y): (y, x)).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = float(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (count > 1000):
        print word + ":\t\t" + "{:.0f}".format(count)
