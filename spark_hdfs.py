import os
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('read_hdfs').getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 10000)

json_data=spark.read.json('hdfs://192.168.32.11:9000/user/root/data/profile.json')
json_data.show()
json_data.first()

json_data.write.save('hdfs://lan.node1:9000/user/root/backup/profile.bk')

properties_data=json_data.select('properties')
properties_data.show()
properties_data.write.text("hdfs://192.168.3.171:9000/user/root/backup/properties.log")

json_data.select("event").write.text("hdfs://192.168.3.171:9000/user/root/backup/event.log")