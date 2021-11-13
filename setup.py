###### SETUP ######

# This script allows you to automatically move the data frmo the local project's data folder into the Data Lake
# Just execute the script by pressing the Run All button under the Run tab
# Execution of this script is optional. 
# You can follow the instructions in the README if you want to complete the same steps while learning more about CML

### Installing Requirements
!pip3 install -r requirements.txt

import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import yaml

from pyspark.sql import SparkSession

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct Cloud Storage URL is:{}".format(storage))

os.environ['STORAGE'] = storage


spark = SparkSession\
    .builder\
    .appName("LC_Baseline_Model")\
    .config("spark.yarn.access.hadoopFileSystems",os.environ["STORAGE"])\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-2")\
    .getOrCreate()
    

df = spark.read.option('inferschema','true').csv(
  "data/LoanStats_2015_subset_071821.csv",
  header=True,
  sep=',',
  nullValue='NA'
)    
  
    
df\
  .write.format("parquet")\
  .mode("overwrite")\
  .saveAsTable(
    'default.test_table'
)

df.write.csv(str(os.environ["STORAGE"])+"/test_data.csv")
  
spark.stop()

#.config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
#