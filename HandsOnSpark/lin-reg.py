from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LinearReg").getOrCreate()

    inputLines = spark.sparkContext.textFile("data/regression.txt")
    data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    lin_reg = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    model = lin_reg.fit(trainingDF)

    fullPredictions = model.transform(testDF).cache()

    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    prediction_label = predictions.zip(labels).collect()

    for prediction in prediction_label:
        print(prediction)

    spark.stop()

