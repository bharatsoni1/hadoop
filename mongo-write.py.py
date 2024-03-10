#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()
    
    readUsers = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri", "mongodb://127.0.0.1/moviesdata.users").load()
    
    readUsers.createOrReplaceTempView("users")
    
    readUsers.printSchema()
    
    sqlDF = spark.sql("""
    SELECT occupation, count(user_id) as cnt_usr
    FROM user
    GROUP BY occupation
    ORDER BY cnt_usr DESC
    """)
    
    sqlDF.show()

