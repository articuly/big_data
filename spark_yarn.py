import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('test_for_yarn').master('yarn').getOrCreate()
# spark.yarn.jars='hdfs://0.0.0.0:9000/user/root/jars/*.jar'

# yarn模式下， 相对路径是相对于hadoop的/user/root目录
json_data = spark.read.json("data/profile.json")

#####################################################################
# 更复杂的一些操作
# json_data_properties = json_data.select(col("distinct_id").alias("id"), "properties.$city", "properties.$ip")
# new_json_data = json_data.distinct().drop("properties").join(json_data_properties.distinct(),
#                                                   json_data.distinct_id==json_data_properties.id)
######################################################################

json_data.createOrReplaceTempView('mytable')
# spark.catalog.cacheTable("mytable")
res = spark.sql("select * from mytable limit 20")
res.show()
# for r in res.collect():
#     print(r)

# spark-submit --master yarn spark_yarn.py
