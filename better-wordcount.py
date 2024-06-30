import re #importing regular expression module
from pyspark import SparkConf, SparkContext
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("D:/Data_Engineering/Apache_Spark/book.txt")
words = input.flatMap(normalizeWords)
# wordCounts = words.countByValue()
# convert each word to a key/value pair with vallue of 1 and count all up with reduceByKey
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y) 
#flip word,count to count,word and sort by count
wordCountsSorted = wordCounts.map(lambda x:(x[1],x[0])).sortByKey()

#collect the results
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t" + count)


# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if cleanWord:
#         print(cleanWord, count)
