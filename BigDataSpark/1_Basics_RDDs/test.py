import sys
from pyspark import SparkContext

if __name__ == "__main__":
    def myFunc(s):
        words = s.split(" ")
        return len(words)

    sc = SparkContext(appName="test")
    lines = sc.textFile("data.txt").map(myFunc)
    output = lines.collect()
    
    for line in output:
        print(line)
    
    sc.stop()
