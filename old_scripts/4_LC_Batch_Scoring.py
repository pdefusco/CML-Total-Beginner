#!/usr/bin/env python
# coding: utf-8

# In[15]:


get_ipython().system('pip3 install sklearn')


# In[16]:


from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.sql import functions as F


# In[17]:


import random
import numpy as np
from sklearn import neighbors
from pyspark.mllib.stat import Statistics


# In[18]:


import pandas as pd
import numpy as np


# In[19]:


from pyspark.sql import SparkSession


# In[20]:


spark = SparkSession    .builder    .appName("PythonSQL")    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")    .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-2/")    .getOrCreate()


# In[81]:


df = spark.sql("SELECT * FROM default.LC_Table")


# Baseline Feature Exploration

# In[82]:


#Creating list of categorical and numeric features
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in') or item[1].startswith('dou')]


# In[83]:


df = df.select(*num_cols)


# In[84]:


df = df.dropna()


# In[85]:


df = df.select(['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal', 'funded_amnt', 'is_default'])


# In[86]:


#Creates a Pipeline Object including One Hot Encoding of Categorical Features  
def make_pipeline(spark_df, num_att):        
     
    for c in spark_df.columns:
        spark_df = spark_df.withColumn(c, spark_df[c].cast("float"))
    
    stages= []

    #Assembling mixed data type transformations:
    assembler = VectorAssembler(inputCols=num_att, outputCol="features")
    stages += [assembler]    
    
    #Scaling features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    stages += [scaler]
    
    #Logistic Regression
    lr = LogisticRegression(featuresCol='scaledFeatures', labelCol='is_default', maxIter=10, regParam=0.3, elasticNetParam=0.4)
    stages += [lr]
    
    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(spark_df)
    out_df = pipelineModel.transform(spark_df)
    
    return out_df, pipelineModel


# In[87]:


df_model, pipelineModel = make_pipeline(df, df.columns)


# In[88]:


input_data = df_model.rdd.map(lambda x: (x["is_default"], float(x['probability'][1])))


# In[89]:


predictions = spark.createDataFrame(input_data, ["is_default", "probability"])
predictions.write.format('parquet').mode("overwrite").saveAsTable('default.LC_simpl_scores')


# In[90]:


#Saving pipeline to S3:
#pipelineModel.write().overwrite().save("s3a://demo-aws-2/datalake/pdefusco/pipeline")


# In[91]:


df.dtypes


# In[93]:


df.take(1)


# In[ ]:




