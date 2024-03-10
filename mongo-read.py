#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split("|")
    return Row(user_id = int(fields[0])), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()
    
    lines = spark.sparkContext.TextFile("hdfs:///user/maria_dev/mobgo_db/movies.user.txt")
    
    user = lines.map(parseInput)
    
    userDataset = spark.createDataFrame(user)
    
    userDataset.write.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/moviesdata.users")\
    .mode("append").save()

