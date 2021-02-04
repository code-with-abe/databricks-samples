# Databricks notebook source
# MAGIC %md ## DataOps Data Collection
# MAGIC 
# MAGIC **This notebook collects the following data into a Delta Lake database**
# MAGIC </p>
# MAGIC If you have multiple workspaces you will need to create copies of this notebook and run each in a separate workspace
# MAGIC </p>
# MAGIC   * Collect Table and Column metadata
# MAGIC   * Collect Notebooks metadata
# MAGIC   * Cluster Metadata
# MAGIC   * Jobs Metadata and Run details

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Setup Connectivity to Databricks using Rest API

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# DOMAIN is the URL of your Databricks Workspace
# https://docs.microsoft.com/en-us/azure/databricks/workspace/workspace-details#workspace-url

DOMAIN = 'https://adb-xxxxxxxxxxxx.azuredatabricks.net'

# TOKEN is the personal access token to connect to Databricks
# https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication

TOKEN = 'dapi76xxxxxxxxxxxxxxxxxxxx'


# COMMAND ----------

# MAGIC %sql
# MAGIC -- we need this to avoid certain errors while converting pandas to spark dataframe
# MAGIC set spark.sql.execution.arrow.pyspark.enabled=False

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Setup DeltaLake to store data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dataops location '/mnt/adls/datalake/';
# MAGIC USE dataops;

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Collect Database and Table details

# COMMAND ----------

# DBTITLE 0,Get Database Metadata
# Get all databases
databases = spark.sql("show databases")
databases.createOrReplaceTempView("databases")

# Get all tables in each database
table_list = []
for db in databases.rdd.collect():
  databases = spark.sql("use "+db.namespace)
  tables = spark.sql("show tables")
  tables = tables.select("database","tableName").filter("database <>''")
  table_list.extend(tables.rdd.collect())
  
lSchema = StructType([StructField("database", StringType())\
                     ,StructField("tableName", StringType())])

tablesTmp = spark.createDataFrame(table_list,schema=lSchema)

# After getting initial list of tables, describe each table to get more details about each table
table_details = []
for tbl in tablesTmp.rdd.collect():
  tblDetails = spark.sql("describe detail "+tbl.database+"."+tbl.tableName)
  table_details.extend(tblDetails.rdd.collect())

tSchema = StructType([StructField("format", StringType())\
                      ,StructField("id", StringType())\
                      ,StructField("name", StringType())\
                      ,StructField("description", StringType())\
                      ,StructField("location", StringType())\
                      ,StructField("createdAt", DateType())\
                      ,StructField("lastModified", DateType())\
                      ,StructField("partitionColumns", StringType())\
                      ,StructField("numFiles", IntegerType())\
                      ,StructField("sizeInBytes", IntegerType())\
                      ,StructField("properties", StringType())\
                      ,StructField("minReaderVersion", StringType())\
                      ,StructField("minWriterVersion", StringType())])
tabledetailsTmp = spark.createDataFrame(table_details,schema=tSchema)

# Create a temporary table for further analysis
tabledetailsTmp.createOrReplaceTempView("tabledetailsTmp") 


# COMMAND ----------

# MAGIC %sql
# MAGIC select split(name,"\\.")[0] as databaseName,split(name,'\\.')[1] as tableName,
# MAGIC        format as tableFormat,description,location,createdAt,partitionColumns,
# MAGIC        numFiles,round((sizeInBytes/1024)/1024,2) sizeinBytes  from tabledetailsTmp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table if it already doesn't exists
# MAGIC drop table if exists dataops.dbr_tables;
# MAGIC 
# MAGIC create table if not exists dataops.dbr_tables
# MAGIC using delta
# MAGIC as 
# MAGIC select distinct split(name,"\\.")[0] as databaseName,split(name,'\\.')[1] as tableName,
# MAGIC        format as tableFormat,description,location,createdAt,partitionColumns,
# MAGIC        numFiles,sizeInBytes  from tabledetailsTmp

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dataops.dbr_tables

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Collect Column level details

# COMMAND ----------

col_list=[]
for tbl in tablesTmp.rdd.collect():
  tblColumns = spark.sql("describe table "+tbl.database+"."+tbl.tableName)
  tblColumns = tblColumns.filter("data_type <>''")
  
  for col in tblColumns.rdd.collect():
    databaseName = tbl.database
    tableName = tbl.tableName
    colName = col.col_name
    dataType = col.data_type 
    col_list.append([databaseName,tableName,colName,dataType])
    
cSchema = StructType([StructField("databaseName", StringType())\
                     ,StructField("tableName", StringType())\
                     ,StructField("colName", StringType())\
                     ,StructField("dataType", StringType())])

columnsTmp = spark.createDataFrame(col_list,schema=cSchema)
columnsTmp.createOrReplaceTempView("columnsTmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table if it already doesn't exists
# MAGIC drop table if exists dataops.dbr_columns;
# MAGIC 
# MAGIC create table if not exists dataops.dbr_columns
# MAGIC using delta
# MAGIC as 
# MAGIC select * from columnsTmp

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dataops.dbr_columns

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Collect Notebook details

# COMMAND ----------

# DBTITLE 0,Get Notebook Metadata
import json 
import pandas as pd 
from pandas.io.json import json_normalize

end_point = '/api/2.0/workspace/list'
path_list = []
notebook_list = []
path = '/'

dfSchema = StructType([StructField("object_id", StringType())\
                      ,StructField("object_type", StringType())\
                      ,StructField("object_path", StringType())])

# Recursive function to navigate through each path and gather notebook and language information
def analyze_path(path):
  response = requests.get(DOMAIN + end_point,headers={'Authorization': 'Bearer ' + TOKEN}, json={"path": path})
  data = response.text
  pdf = pd.read_json(data, orient='records')
  if pdf.shape[0]>0:
      pdf2 = json_normalize(pdf['objects'])
      objects = spark.createDataFrame(pdf2[["object_id","object_type","path"]],schema=dfSchema)
      notebook_paths = objects.filter("object_type = 'NOTEBOOK'")
      if notebook_paths.count()>0:
        nb = pdf2[pdf2['object_type']=="NOTEBOOK"]
        nbSchema = StructType([StructField("object_id", StringType())\
                          ,StructField("object_type", StringType())\
                          ,StructField("object_path", StringType())\
                          ,StructField("object_language", StringType())])
        notebooks = spark.createDataFrame(nb[["object_id","object_type","path","language"]],schema=nbSchema)
        try:
          for row in notebooks.rdd.collect():
            #print("notebook directory {} ".format(row.object_path))
            notebook_list.append([row.object_id,row.object_path,row.object_language])
        except Exception as e:
           print(e)

      object_paths = objects.filter("object_type = 'DIRECTORY'")
      #print("object count: "+str(object_paths.count()))
      if object_paths.count()>0:
        try:
          for row in object_paths.rdd.collect():
            #print("Searching directory {} ".format(row.object_path))
            path_list.append([row.object_id,row.object_path])
            analyze_path(row.object_path)
        except Exception as e:
           print(e)
  
analyze_path(path)

# COMMAND ----------

# Create Temporary Tables
nbSchema = StructType([StructField("notebook_id", StringType())\
                      ,StructField("notebook_path", StringType())\
                      ,StructField("notebook_language", StringType())])
notebooks = spark.createDataFrame(notebook_list,schema=nbSchema)
notebooks.createOrReplaceTempView("notebooks")

dirSchema = StructType([StructField("directory_id", StringType())\
                      ,StructField("directory_path", StringType())])
paths = spark.createDataFrame(path_list,schema=dirSchema)
paths.createOrReplaceTempView("directories")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table if it already doesn't exists
# MAGIC drop table if exists dataops.dbr_notebooks;
# MAGIC 
# MAGIC create table if not exists dataops.dbr_notebooks
# MAGIC using delta
# MAGIC as select * from notebooks

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dataops.dbr_notebooks

# COMMAND ----------

# DBTITLE 1,Get Cluster Details
end_point = '/api/2.0/clusters/list'

response = requests.get(
  DOMAIN + end_point,
  headers={'Authorization': 'Bearer ' + TOKEN}
 )
data = response.text
pdf = pd.read_json(data, orient='records')
pdf2 = json_normalize(pdf['clusters'])
clSchema = StructType([StructField("cluster_id", StringType())\
                      ,StructField("cluster_name", StringType())\
                      ,StructField("spark_version", StringType())\
                      ,StructField("driver_node_type_id", StringType())\
                      ,StructField("node_type_id", StringType())\
                      ,StructField("num_workers", StringType())\
                      ,StructField("cluster_cores", StringType())\
                      ,StructField("cluster_memory_mb", StringType())\
                      ,StructField("start_time", StringType())\
                      ,StructField("last_state_loss_time", StringType())])
clusters = spark.createDataFrame(pdf2[["cluster_id","cluster_name","spark_version","driver_node_type_id","node_type_id","num_workers","cluster_cores","cluster_memory_mb","start_time","last_state_loss_time"]],schema=clSchema)
clusters.createOrReplaceTempView("clusters")
display(clusters)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table if it already doesn't exists
# MAGIC drop table if exists dataops.dbr_clusters;
# MAGIC 
# MAGIC create table if not exists dataops.dbr_clusters
# MAGIC using delta
# MAGIC as select * from clusters

# COMMAND ----------

# DBTITLE 1,Get Job Run Details
end_point = '/api/2.0/jobs/runs/list?limit=50'

response = requests.get(
  DOMAIN + end_point,
  headers={'Authorization': 'Bearer ' + TOKEN}
 )
data = response.text
pdf = pd.read_json(data, orient='records')
pdf2 = json_normalize(pdf['runs'])
jbSchema = StructType([StructField("job_id", StringType())\
                      ,StructField("run_id", StringType())\
                      ,StructField("start_time", StringType())\
                      ,StructField("end_time", StringType())\
                      ,StructField("run_type", StringType())\
                      ,StructField("task.notebook_task.notebook_path", StringType())\
                      ,StructField("cluster_spec.new_cluster.spark_version", StringType())\
                      ,StructField("cluster_spec.new_cluster.node_type_id", StringType())])
jobs = spark.createDataFrame(pdf2[["job_id","run_id","start_time","end_time","run_type","task.notebook_task.notebook_path",\
                                       "cluster_spec.new_cluster.spark_version","cluster_spec.new_cluster.node_type_id"]],schema=jbSchema)
jobs.createOrReplaceTempView("jobs")
display(jobs)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table if it already doesn't exists
# MAGIC --drop table dbr_jobs;
# MAGIC 
# MAGIC create table if not exists dataops.dbr_jobs
# MAGIC using delta
# MAGIC as select * from jobs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new records
# MAGIC merge into dataops.dbr_jobs as t
# MAGIC using 
# MAGIC jobs as s
# MAGIC on t.job_id = s.job_id  and t.run_id = s.run_id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM dataops.dbr_jobs RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dataops.dbr_jobs
