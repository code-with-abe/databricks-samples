# Databricks notebook source
# MAGIC %md
# MAGIC ### Delete Azure Purview Objects
# MAGIC This notebook shows how you can delete existing Purview objects

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Setup Connectivity to Azure Purview

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



# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Create Client

# COMMAND ----------

# Databricks notebook source
import argparse
import json
import os

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

# Search for the entity you want to delete
import json
import os
search = client.search_entities("loan_risk_data.csv")
for page in search:
  print(json.dumps(page, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Bulk delete upto 50 entities

# COMMAND ----------

import json
import os
search = client.search_entities("databricksassets")
for page in search:
  print(page["id"])
  delete_response = client.delete_entity(guid=page["id"])
  print(json.dumps(delete_response, indent=2))
     

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Delete a specific Entity

# COMMAND ----------

search = client.search_entities("databricks://adb-2578185452046759.19.azuredatabricks.net")
for page in search:
  print(page["id"])
  delete_response = client.delete_entity(guid=page["id"])
  print(json.dumps(delete_response, indent=2))
  

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Delete Custom Entity Types

# COMMAND ----------

client.delete_type(name="databricks_job")
client.delete_type(name="databricks_column_to_table")
client.delete_type(name="databricks_column")
client.delete_type(name="databricks_table")
