#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.sql.types import DateType, IntegerType
from pyspark.sql import functions as F 


# In[2]:


## Starting the master cluser
# !./spark-3.5.1-bin-hadoop3/sbin/start-master.sh


# In[3]:


## Creating a local sparksessin
spark = SparkSession.builder \
        .master("spark://Sabins-MacBook-Air.local:7077") \
        .appName("project") \
        .getOrCreate()


# In[4]:


spark


# In[5]:


## Now let's start the spark worker cluster
# !./spark-3.5.1-bin-hadoop3/sbin/start-worker.sh spark://Sabins-MacBook-Air.local:7077


# ### Reading the Dataset

# In[6]:


file_path = "invoice_train.csv"

invoice = spark.read.csv(file_path, header=True)
invoice.show(2)


# In[7]:


# Total number of entries
invoice.count()


# In[8]:


# Checking the type of the dataframe
type(invoice)


# In[9]:


# lets check the spark database
print(spark.catalog.listTables())


# In[10]:


invoice.printSchema()


# In[11]:


## We can see that all the feilds are of StringType(). We should change the datatype.
## 'client_id' and 'counter_type' have alphanumeric value so theie datatype is correct
## We should change the datatype of other columns
invoice.columns


# In[12]:


# Changing the datatype of column
invoice = invoice \
.withColumn('invoice_date', F.to_date(invoice.invoice_date)) \
.withColumn('tarif_type', invoice['tarif_type'].cast(IntegerType())) \
.withColumn('counter_number', invoice['counter_number'].cast(IntegerType())) \
.withColumn('counter_statue', invoice['counter_statue'].cast(IntegerType())) \
.withColumn('counter_code', invoice['counter_code'].cast(IntegerType())) \
.withColumn('reading_remarque', invoice['reading_remarque'].cast(IntegerType())) \
.withColumn('counter_coefficient', invoice['counter_coefficient'].cast(IntegerType())) \
.withColumn('consommation_level_1', invoice['consommation_level_1'].cast(IntegerType())) \
.withColumn('consommation_level_2', invoice['consommation_level_2'].cast(IntegerType())) \
.withColumn('consommation_level_3', invoice['consommation_level_3'].cast(IntegerType())) \
.withColumn('consommation_level_4', invoice['consommation_level_4'].cast(IntegerType())) \
.withColumn('old_index', invoice['old_index'].cast(IntegerType())) \
.withColumn('new_index', invoice['new_index'].cast(IntegerType())) \
.withColumn('months_number', invoice['months_number'].cast(IntegerType())) 


# In[13]:


invoice.printSchema()


# In[14]:


invoice.filter(invoice.invoice_date.isNotNull())


# In[15]:


invoice.describe().show()


# ### Handling Null values

# In[16]:


invoice = invoice.na.drop()
invoice.count()


# ## Drop Duplicates

# In[17]:


invoice = invoice.dropDuplicates()
invoice.count()


# ### Features Extraction and Joining

# In[18]:


# Extracting year form data column
invoice = invoice.withColumn("year", F.year(invoice['invoice_date']))
invoice.columns


# In[19]:


invoice.show(4)


# In[38]:


# Calculating total transaction count for each client it
df_transaction_count = invoice\
                        .groupby(['client_id']).count() \
                        .withColumnRenamed('count', 'transaction_count')


# In[39]:


df_transaction_count.show(5)


# In[51]:


# Joining
invoice = invoice \
            .join(df_transaction_count, "client_id", how="inner") 


# In[52]:


invoice.show(5)


# ### Data partitioning

# In[53]:


# Let's repartition the dataset so that all the clusters can be utilized
data = invoice.repartition(10)


# In[54]:


# Let's save the dataset
data.write.csv("cleaned_data/invoice_partitioned", header=True)


# In[55]:


# let's check the output
get_ipython().system('ls ./data/invoice_partitioned')

## Data is successfully partitioned

