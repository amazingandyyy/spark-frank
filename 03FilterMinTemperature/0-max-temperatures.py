from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)

lines = sc.textFile("./1800.csv")
rdd = lines.map(parseLine)
stationMaxTemps = rdd.filter(lambda x: "TMAX" in x[1]).map(lambda x: (x[0], x[2]))

maxTemps = stationMaxTemps.reduceByKey(lambda x, y: max(x,y))
# reduceByKey will apply function to value for each unique key

results = maxTemps.collect()

print("Max Temp of the year:")
for result in results:
    print(result[0] + "\t{:.3f}C".format(result[1]))