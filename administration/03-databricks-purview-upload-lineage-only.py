# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks to Azure Purview Lineage
# MAGIC This notebook shows how you can upload databricks entities and lineage to Azure Purview

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Connect to Azure Purview using Service Principal
# MAGIC 
# MAGIC This code is based samples provided on github 
# MAGIC https://github.com/wjohnson/pyapacheatlas
# MAGIC </p>
# MAGIC Before running this notebook you need to install the package pyapacheatlas and purviewcli from PyPI on the databricks cluster
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
# MAGIC ##### 2. Get supporting functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we need this to avoid certain errors while converting pandas to spark dataframe
# MAGIC set spark.sql.execution.arrow.pyspark.enabled=False

# COMMAND ----------

# Recursive function to navigate through each path and gather notebook and language information
end_point = '/api/2.0/workspace/list'
path_list = []
notebook_list = []

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

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Setup connectivity to Azure Purview

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
# MAGIC ##### 4. Collect Notebook Metadata

# COMMAND ----------

# # Path to scan
path = '/Shared'
  
analyze_path(path)

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

# Create a temporary file with notebook paths
dirs = spark.sql("select * from directories where directory_path like '%ETL%'")

# notebooksFile = open(adls_dir+"/notebooksTemp.txt", "w")
# for dir in dirs.rdd.collect():
#   print(dir.directory_path)
#   notebooksFile.write(dir.directory_path+"\n")
# notebooksFile.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Scan Notebooks

# COMMAND ----------

# MAGIC %sh
# MAGIC nbpath="/Shared/02-ETL-Jobs"
# MAGIC echo "Processing notebook path: $nbpath"
# MAGIC rm -r /dbfs/mnt/datafiles/purview/notebooks/
# MAGIC databricks workspace export_dir $nbpath /dbfs/mnt/datafiles/purview/notebooks/ --profile premium

# COMMAND ----------

# Notebook metadata
import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

notebook_mnt = "/mnt/datafiles/purview/notebooks"
dirs = spark.sql("select * from notebooks where notebook_path like '%ETL%'")
notebook_list = []
for dir in dirs.rdd.collect():
  #print(dir.notebook_path.split('/')[-1])
  nbname = dir.notebook_path.split('/')[-1]
  df = spark.read.text(notebook_mnt+"/"+nbname+".py")
  cnt = df.filter("lower(value) like '%create %' or  lower(value) like '%merge %' or  lower(value) like '%insert %' or lower(value) like '%spark.write%'").count()
  if cnt > 0:
    notebook_list.append(dir.notebook_path)
notebook_list

# COMMAND ----------

# Get Databricks Tables in purview
import json
import os
tables = []
search = client.search_entities("databricks_table")
for page in search:
  tblln = []
  tname = page["qualifiedName"].split('/')[-1]
  tblln.insert(0,tname.lower())
  tblln.insert(1,page["qualifiedName"])
  tables.append(tblln)


# COMMAND ----------

# Get ADLS files in purview
import json
import os
adlsfiles = []
filter_setup = {"typeName": "azure_datalake_gen2_path"}
search = client.search_entities("abedatalake01",search_filter=filter_setup)
for page in search:
  flln = []
  fname = page["qualifiedName"].split('/')[-1]
  if fname.find("csv") > 0 :
    flln.insert(0,fname.lower())
    flln.insert(1,page["qualifiedName"])
    adlsfiles.append(flln)


# COMMAND ----------

# Scan each notebook for Databricks tables and ADLS files

nbList = []

for nb in notebook_list:
  # parse out the notebook name
  nbname = nb.split('/')[-1]
  # initialize source and target
  nbMapping = []
  src = "file"
  trg = "table"
  
  print("Searching notebook: "+nbname)
  nbMapping.insert(0,nb)
  
  # read notebook from ADLS backup into a dataframe
  df = spark.read.text(notebook_mnt+"/"+nbname+".py").withColumn("linenum", monotonically_increasing_id())
  
  # iterate through all purview files to check which one appears in the notebook

  for fl in adlsfiles:
    fileCnt = df.filter("lower(value) like '%"+fl[0]+"%'").count()
    if fileCnt>0:
      # add the table to the mapping list
      nbMapping.insert(1,fl[1])  
      break
  # iterate through all purview databricks tables to check which appear

  for tbl in tables:
    tableCnt = df.filter("lower(value) like '%"+tbl[0]+" %'").count()
    if tableCnt>0:
      # add the table to the mapping list
      nbMapping.insert(2,tbl[1])
      break

  nbList.append(nbMapping)
  
df = spark.createDataFrame(nbList)
pdf = df.toPandas()
pdf.columns = ['notebook', 'source', 'target']
pdf.to_csv(adls_dir+"/notebook_mapping.csv",index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Upload Notebook mapping into Purview

# COMMAND ----------

maps = spark.read.option("header","true").csv("/mnt/datafiles/purview/notebook_mapping.csv")
for map in maps.rdd.collect():
  nbname = map.notebook.split('/')[-1]
  print("Adding :"+nbname)
  InputEntity = client.get_entity(
        qualifiedName=[map.source],
        typeName= 'azure_datalake_gen2_path'
    )
  OutputEntity = client.get_entity(
        qualifiedName=[map.target],
        typeName="databricks_table"
    )
  job_process = AtlasProcess(
  name=nbname,
  qualified_name = "databricks://"+v_databricks_domain+"/notebooks/"+nbname,
  typeName="databricks_job",
  guid=guid.get_guid(),
  attributes = {"job_type":"notebook","notebook_path":map.notebook},
  inputs = [InputEntity.get("entities")[0]],
  outputs = [OutputEntity.get("entities")[0]] )

  client.upload_entities(job_process)

  
