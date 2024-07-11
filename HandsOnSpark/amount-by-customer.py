# Find total amount spent by each customer
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("AmountByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPrice(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

input = sc.textFile("data/customer-orders.csv")
# map each line to a tuple of (customerID, amount)
parsedLines = input.map(extractCustomerPrice)
# add up the amounts for each customer
totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y).sortByKey()
# collect the results
results = totalByCustomer.collect()
# print the results
for result in results:
    print(result)