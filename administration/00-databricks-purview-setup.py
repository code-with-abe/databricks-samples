# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks to Azure Purview Setup
# MAGIC This notebook shows how you can setup databricks entities and lineage in Azure Purview

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
v_tenant_id = "xxxxxxxxxxxxxxxx"
v_client_id = "xxxxxxxxxxxxxxxx"
v_client_secret = "xxxxxxxxxxxxxxxxxxxxxx"
v_data_catalog_name = "purview-account-name"
# This is the workspace URL without the https:// for your databricks workspace
# This is used as the prefix for all databricks objects.
v_databricks_domain = "adb-xxxxxxxxxxxx.azuredatabricks.net"

# The below setup is required for leveraging Purview CLI through Shell commands
os.environ['PURVIEW_NAME']=v_data_catalog_name
os.environ['AZURE_TENANT_ID']=v_tenant_id
os.environ['AZURE_CLIENT_ID']=v_client_id
os.environ['AZURE_CLIENT_SECRET']=v_client_secret

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup Databricks CLI for exporting notebooks to Azure Blob Storage
# MAGIC The below setup is required to leverage Databricks CLI to export noteboook code to Azure Blob Storage mount points for Lineage Analysis

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade databricks-cli
# MAGIC echo "[personal]">~/.databrickscfg
# MAGIC echo "host = https://adb-xxxxxxxxxxxxxxxxxxxxx.azuredatabricks.net/">>~/.databrickscfg
# MAGIC echo "token = dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">>~/.databrickscfg

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
# MAGIC ##### 2. Setup Custom Entity Types
# MAGIC Setup custom entities to capture Databricks Tables, Columns and Jobs

# COMMAND ----------

# DBTITLE 0,databricks-table entity type
# Set up the new entity types to capture delta lake tables and databricks jobs

# Databricks Table
databricks_table_type = EntityTypeDef(
  name="databricks_table",
  attributeDefs=[
    AtlasAttributeDef(name="format",defaultValue="parquet",isOptional=True).to_json(),
    AtlasAttributeDef(name="location",isOptional=True).to_json(),
    AtlasAttributeDef(name="num_files",isOptional=True).to_json(),
    AtlasAttributeDef(name="size",isOptional=True).to_json()
  ],
  superTypes = ["DataSet"],
  options = {"schemaElementAttribute":"columns"}
 )
typedef_results = client.upload_typedefs({"entityDefs":[databricks_table_type.to_json()]},  force_update=True)
print(typedef_results)

# COMMAND ----------

# DBTITLE 1,databricks-column entity type
# Databricks Column
databricks_column_type = EntityTypeDef(
  name="databricks_column",
  attributeDefs=[
    AtlasAttributeDef(name="data_type")
  ],
  superTypes = ["DataSet"],
)

typedef_results = client.upload_typedefs({"entityDefs":[databricks_column_type.to_json()]},  force_update=True)
print(typedef_results)



# COMMAND ----------

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

typedef_results = client.upload_typedefs({"relationshipDefs":[databricks_column_to_table.to_json()]},  force_update=True)
print(typedef_results)

# COMMAND ----------

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

typedef_results = client.upload_typedefs({"entityDefs":[databricks_job_type.to_json()]},  force_update=True)
print(typedef_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Delete Entity Types

# COMMAND ----------

# client.delete_type(name="databricks_job")
# client.delete_type(name="databricks_column_to_table")
# client.delete_type(name="databricks_column")
# client.delete_type(name="databricks_table")

