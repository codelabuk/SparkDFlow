from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = "Hello Spark Hello python Hello Airflow Hello Docker And Hello CodeLabuk"

words = spark.sparkContext.parallelize(text.split(" "))
worCounts = (words.map(lambda word: (word, 1)).
             reduceByKey(lambda a,b: a+b))

for wc in worCounts.collect():
    print(wc[0], wc[1])

spark.stop()
