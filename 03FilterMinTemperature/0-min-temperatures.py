from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)

lines = sc.textFile("./1800.csv")
rdd = lines.map(parseLine)
stationMinTemps = rdd.filter(lambda x: "TMIN" in x[1]).map(lambda x: (x[0], x[2])) # only left TMIN and remove entryType

minTemps = stationMinTemps.reduceByKey(lambda x, y: min(x,y))
# reduceByKey will apply function to value for each unique key

results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.3f}C".format(result[1]))