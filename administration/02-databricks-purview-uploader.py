# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks to Azure Purview Uploader
# MAGIC This notebook shows how you can upload databricks entities and lineage to Azure Purview

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Connect to Azure Purview using Service Principal
# MAGIC The below cell runs a notebook that assigns the service principal credentials to variables which are used to connect to Azure Purview
# MAGIC This code is based samples provided on github 
# MAGIC https://github.com/wjohnson/pyapacheatlas

# COMMAND ----------

# Add your credentials here or set them as environment variables
# This is the service principal which has access to your purview account
v_tenant_id = "xxxxxxxxxxxxxxxx"
v_client_id = "xxxxxxxxxxxxxxxx"
v_client_secret = "xxxxxxxxxxxxxxxxxxxxxx"
v_data_catalog_name = "purview-account-name"
# This is the workspace URL without the https:// for your databricks workspace
# This is used as the prefix for all databricks objects.
v_databricks_domain = "adb-xxxxxxxxxxxx.azuredatabricks.net"


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
# MAGIC ##### 2. Setup Custom Entity Types

# COMMAND ----------

# Set up the new entity types to capture delta lake tables and databricks jobs

# Databricks Table
databricks_table_type = EntityTypeDef(
  name="databricks_table",
  attributeDefs=[
    AtlasAttributeDef(name="format",defaultValue="spark",isOptional=True).to_json(),
    AtlasAttributeDef(name="location",isOptional=True).to_json(),
    AtlasAttributeDef(name="num_files",isOptional=True).to_json(),
    AtlasAttributeDef(name="size",isOptional=True).to_json()
  ],
  superTypes = ["DataSet"],
  options = {"schemaElementAttribute":"columns"}
 )

# Databricks Column
databricks_column_type = EntityTypeDef(
  name="databricks_column",
  attributeDefs=[
    AtlasAttributeDef(name="data_type")
  ],
  superTypes = ["DataSet"],
)

# Databricks Jobs
databricks_job_type = EntityTypeDef(
  name="databricks_job",
  attributeDefs=[
    AtlasAttributeDef(name="job_type",isOptional=False).to_json(),
    AtlasAttributeDef(name="notebook_path",isOptional=True).to_json(),
    AtlasAttributeDef(name="schedule",defaultValue="adHoc").to_json()
  ],
  superTypes = ["Process"]
)

# Databricks Column to Table Relationship
databricks_column_to_table = RelationshipTypeDef(
  name="databricks_column_to_table",
  relationshipCategory="COMPOSITION",
  endDef1={
          "type": "databricks_table",
          "name": "columns",
          "isContainer": True,
          "cardinality": "SET",
          "isLegacyAttribute": False
      },
  endDef2={
          "type": "databricks_column",
          "name": "table",
          "isContainer": False,
          "cardinality": "SINGLE",
          "isLegacyAttribute": False
      }
)

# Upload these types
typedef_results = client.upload_typedefs(
  {"entityDefs":[databricks_table_type.to_json(), databricks_column_type.to_json(), databricks_job_type.to_json()],
   "relationshipDefs":[databricks_column_to_table.to_json()]
  }, 
  force_update=True)

print(typedef_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Upload Spark/Deltalake Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure you have run the 01-collect-databricks-metadata notebook before running the rest of the notebook
# MAGIC --https://github.com/code-with-abe/databricks-samples/blob/main/administration/01-collect-databricks-metadata.py
# MAGIC 
# MAGIC select databaseName,tableFormat,tableName,location,numFiles,sizeInBytes from dataops.dbr_tables

# COMMAND ----------

tbls = spark.sql("select databaseName,tableFormat,tableName,location,numFiles,sizeInBytes from dataops.dbr_tables")

input_tables = []
for tbl in tbls.rdd.collect():
  #print("Processing table: "+tbl.tableName)
  table_entity = AtlasEntity(
  name=tbl.tableName,
  qualified_name = "databricks://"+v_databricks_domain+"/"+tbl.databaseName+"/"+tbl.tableName,
  typeName="databricks_table",
  attributes = {"format":tbl.tableFormat,"location":tbl.location,"num_files":tbl.numFiles,"size":tbl.sizeInBytes},
  guid=guid.get_guid(),
  )
  input_tables.append(table_entity)
  time.sleep(2)

batch = input_tables
client.upload_entities(batch=batch)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Upload Spark/Deltalake Table Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure you have run the 01-collect-databricks-metadata notebook before running the rest of the notebook
# MAGIC --https://github.com/code-with-abe/databricks-samples/blob/main/administration/01-collect-databricks-metadata.py
# MAGIC 
# MAGIC select databaseName,tableName,colName,dataType from dataops.dbr_columns where databaseName = 'acctdb' and tableName = 'account_financials_annual'

# COMMAND ----------

tblCols = spark.sql("select databaseName,tableName,colName,dataType from dataops.dbr_columns where databaseName = 'acctdb' and tableName = 'account_financials_annual'")

input_columns = []
for tblCol in tblCols.rdd.collect():
  time.sleep(2)
  tableEntity = client.get_entity(
        qualifiedName=["databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName],
        typeName="databricks_table"
    )
  #print("Processing table: "+"databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName)
  temp_column = AtlasEntity(
    name = tblCol.colName,
    typeName = "databricks_column",
    qualified_name = "databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName+"#"+tblCol.colName,
    guid=guid.get_guid(),
    attributes = {"data_type":tblCol.dataType},
    relationshipAttributes = {"table":tableEntity.get("entities")[0]}
  )
  #print("Processing Column: "+"databricks://"+v_databricks_domain+"/"+tblCol.databaseName+"/"+tblCol.tableName+"#"+tblCol.colName)
  input_columns.append(temp_column)

batch = input_columns
client.upload_entities(batch=batch)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Upload Lineage from Notebooks

# COMMAND ----------

# First lookup the qualified name of the source
InputEntity = client.get_entity(
        qualifiedName=["https://abedatalake01.dfs.core.windows.net/datafiles/demos/loan_risk_data.csv"],
        typeName= 'azure_datalake_gen2_path'
    )

# COMMAND ----------

# First lookup the qualified name of the target
OutputEntity = client.get_entity(
        qualifiedName=["databricks://adb-xxxxxx.azuredatabricks.net/demo/loans_sample"],
        typeName="databricks_table"
    )

# COMMAND ----------

# Databricks Jobs
job_process = AtlasProcess(
  name="job201",
  qualified_name = "databricks://adb-xxxxxxxxxxxx.19.azuredatabricks.net/jobs/job201",
  typeName="databricks_job",
  guid=guid.get_guid(),
  attributes = {"job_type":"notebook","notebook_path":"/Shared/jobs/job201"},
  inputs = [InputEntity.get("entities")[0]],
  outputs = [OutputEntity.get("entities")[0]]
)

batch = [ job_process.to_json()] 
# Upload all entities!
#client.upload_entities(batch=batch)
