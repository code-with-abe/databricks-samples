# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks to Azure Purview Uploader - Schema Only
# MAGIC This notebook shows how you can upload databricks entities to Azure Purview

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Connect to Azure Purview using Service Principal
# MAGIC 
# MAGIC This code is based samples provided on github 
# MAGIC https://github.com/wjohnson/pyapacheatlas
# MAGIC </p>
# MAGIC Before running this notebook you need to install the package <b>pyapacheatlas</b> and <b>purviewcli</b> from PyPI on the databricks cluster
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/libraries/cluster-libraries

# COMMAND ----------

# Add your credentials here or set them as environment variables
# This is the service principal which has access to your purview account
import os

v_tenant_id = "xxxxxxxxxxxxxxxx"
v_client_id = "xxxxxxxxxxxxxxxx"
v_client_secret = "xxxxxxxxxxxxxxxxxxxxxx"
v_data_catalog_name = "purview-account-name"
# This is the workspace URL without the https:// for your databricks workspace
# This is used as the prefix for all databricks objects.
v_databricks_domain = "adb-xxxxxxxxxxxx.azuredatabricks.net"



os.environ['PURVIEW_NAME']=v_data_catalog_name
os.environ['AZURE_TENANT_ID']=v_tenant_id
os.environ['AZURE_CLIENT_ID']=v_client_id
os.environ['AZURE_CLIENT_SECRET']=v_client_secret

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Setup connectivity to Azure Purview

# COMMAND ----------

# DBTITLE 0,Connect to Azure Purview using Service Principal
# Databricks notebook source
import argparse
import json
import os
import time

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess, TypeCategory
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef, RelationshipTypeDef
from pyapacheatlas.readers import ExcelConfiguration, ExcelReader


# The above cell gets the v_tenant_id,v_client_id etc. 

auth = ServicePrincipalAuthentication(
    tenant_id = v_tenant_id, 
    client_id = v_client_id, 
    client_secret = v_client_secret
)

# Create a client to connect to your service.
client = PurviewClient(
    account_name = v_data_catalog_name,
    authentication = auth
)

guid = GuidTracker()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Collect Databricks Schema

# COMMAND ----------

# Table metadata
import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

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
# MAGIC select * from tabledetailsTmp limit 10

# COMMAND ----------

# Column Metadata
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
# MAGIC select * from columnsTmp limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Upload Spark/Deltalake Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view dbr_tables as
# MAGIC select distinct split(name,"\\.")[0] as databaseName,split(name,'\\.')[1] as tableName,
# MAGIC        format as tableFormat,description,location,createdAt,partitionColumns,
# MAGIC        numFiles,sizeInBytes  
# MAGIC from tabledetailsTmp where split(name,"\\.")[0] = 'acrdb' -- Set this value to which every database you want to upload to purview

# COMMAND ----------

tbls = spark.sql("select * from dbr_tables")

for tbl in tbls.rdd.collect():
  print("Uploading table: "+tbl.tableName)
  time.sleep(5)
  table_entity = AtlasEntity(
  name=tbl.tableName,
  # The below schema used to create qualified name can be changed per your standards  
  qualified_name = "databricks://"+v_databricks_domain+"/"+tbl.databaseName+"/"+tbl.tableName,
  typeName="databricks_table",
  attributes = {"format":tbl.tableFormat,"location":tbl.location,"num_files":tbl.numFiles,"size":tbl.sizeInBytes},
  guid=guid.get_guid(),
  )
  client.upload_entities(table_entity)



# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Assign Glossary Terms to Tables

# COMMAND ----------

# Set the below variable to the glossary terms you want to assign to all the tables.
# You need to first create the glossary terms you want use here in the Azure Purview Portal else the below code won't work
glossaryTerms = ["Azure Consumption"]

# Create a temporary file with GUID of the Glossary term for future processing
# The path should start wtih /dbfs and you can store it in Blob Storage using mount points i.e. /dbfs/mnt...
glossaryTermsFile = open("/dbfs/mnt/datafiles/purview/glossaryTermsTemp.txt", "w")

for term in glossaryTerms:

    dbEntity = client.get_entity(
            qualifiedName=[term+"@Glossary"],
            typeName="AtlasGlossaryTerm"
        )
    dbEntity.get("entities")[0]["guid"]
    glossaryTermsFile.write(dbEntity.get("entities")[0]["guid"]+"\n")



glossaryTermsFile.close()

# COMMAND ----------

# Create a temporary file with all the GUIDs of the tables
# Ensure the dbr_tables table has the tables to which you want to attach the glossary terms
tbls = spark.sql("select * from dbr_tables") 

# The path should start wtih /dbfs and you can store it in Blob Storage using mount points i.e. /dbfs/mnt...
TablesFile = open("/dbfs/mnt/datafiles/purview/TablesTemp.txt", "w")
for tbl in tbls.rdd.collect():
  #print(tbl.tableName)
  dbEntity = client.get_entity(
        qualifiedName=["databricks://"+v_databricks_domain+"/"+tbl.databaseName+"/"+tbl.tableName],
        typeName="databricks_table"
    )
  #print(dbEntity.get("entities")[0]["guid"])
  TablesFile.write(dbEntity.get("entities")[0]["guid"]+"\n")
TablesFile.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Use PurviewCLI to assign Glossary Terms to Tables

# COMMAND ----------

# MAGIC %sh
# MAGIC while IFS= read -r table; do
# MAGIC     echo "Processing Table GUID: $table"
# MAGIC 
# MAGIC     while IFS= read -r term; do
# MAGIC         echo "Processing Term GUID: $term"
# MAGIC         pv glossary createAssignedEntities --termGuid $term --guid $table
# MAGIC     done < /dbfs/mnt/datafiles/purview/glossaryTermsTemp.txt
# MAGIC     
# MAGIC done < /dbfs/mnt/datafiles/purview/TablesTemp.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Upload Table Columns

# COMMAND ----------

# Ensure you set the right filters in the below query to process only the databases and tables you want to be processed
tblCols = spark.sql("select databaseName,tableName,colName,dataType from columnsTmp where databaseName = 'acrdb' and tableName = 'annual_financials' ")

input_columns = []
for tblCol in tblCols.rdd.collect():  
  tableEntity = client.get_entity(
        qualifiedName=["databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName],
        typeName="databricks_table"
    )
  time.sleep(5)
  temp_column = AtlasEntity(
    name = tblCol.colName,
    typeName = "databricks_column",
    qualified_name = "databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName+"#"+tblCol.colName,
    guid=guid.get_guid(),
    attributes = {"data_type":tblCol.dataType},
    relationshipAttributes = {"table":tableEntity.get("entities")[0]}
  )
  print("Uploading Column: "+"databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName+"#"+tblCol.colName)
  client.upload_entities(temp_column)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. ONLY FOR CLEANUP---Delete Glossary Term assignments

# COMMAND ----------

# Create a temporary file with all the GUIDs of the tables
# tbls = spark.sql("""select * from dbr_tables""")

# TablesFile = open("/dbfs/mnt/datafiles/purview/TablesTemp.txt", "w")
# glossaryTermsFile = open("/dbfs/mnt/datafiles/purview/glossaryTermsTemp.txt", "w")
# for tbl in tbls.rdd.collect():
#   #print(tbl.tableName)
#   dbEntity = client.get_entity(
#         qualifiedName=["databricks://"+v_databricks_domain+"/"+tbl.databaseName+"/"+tbl.tableName],
#         typeName="databricks_table"
#     )
#   for rel in dbEntity.get("entities")[0]["relationshipAttributes"]["meanings"]:
#       #print(rel["guid"]+","+rel["relationshipGuid"])
#       glossaryTermsFile.write(rel["guid"]+","+rel["relationshipGuid"]+"\n")
#   #print(dbEntity.get("entities")[0]["guid"])
#   TablesFile.write(dbEntity.get("entities")[0]["guid"]+"\n")

# glossaryTermsFile.close()
# TablesFile.close()

# COMMAND ----------

#%sh
# while IFS= read -r table; do
#     echo "Processing Table GUID: $table"

#     while IFS=',' read -r -a array; do
    
#         echo "Processing Term GUID: ${array[0]}"
#         echo "Processing TermRel GUID: ${array[1]}"
#         pv glossary deleteAssignedEntities --termGuid ${array[0]} --guid $table --relationshipGuid ${array[1]}

#     done < /dbfs/mnt/datafiles/purview/glossaryTermsTemp.txt
    
# done < /dbfs/mnt/datafiles/purview/TablesTemp.txt
