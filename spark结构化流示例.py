# coding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName('StructuredWordCount').getOrCreate()

# create dataframe representing the stream of input lines
# from connection to localhost: 9999
lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()

# split the lines into words
words = lines.select(explode(split(lines.value, ' ')).alias('word'))

# generate running word count
wordCounts = words.groupBy('word').count()

# start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode('complete').format('console').start()

# await spark streaming termination
# query.awaitTermination()
