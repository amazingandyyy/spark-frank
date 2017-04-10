from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    amount = float(fields[2])
    return (customerID, amount)

input = sc.textFile("./customer-orders.csv")
rdd = input.map(parseLine)
countedData = rdd.reduceByKey(lambda x, y: x+ y)
results = countedData.collect()

for result in results:
    print result[0] + "\t${:.2f}".format(result[1])