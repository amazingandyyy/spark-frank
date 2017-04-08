from pyspark import SparkConf, SparkContext
import collections # python built-in pkgs

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("./ml-100k/u.data") # every line will be RDD
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
# print("result", sorted(result.items()))
sortedResults = collections.OrderedDict(sorted(result.items()))
# print("sortedResults", sortedResults.items())
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
