import os
os.environ["SPARK_HOME"] = "C:\\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 3 second
sc = SparkContext("local[2]", "NetworkCharacterFreqency")
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port, like localhost:6000
lines = ssc.socketTextStream("localhost", 6000)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word length in each batch
pairs = words.map(lambda word: (len(word), word))
wordCounts = pairs.reduceByKey(lambda x, y: x + ',' + y)

# Print the elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
