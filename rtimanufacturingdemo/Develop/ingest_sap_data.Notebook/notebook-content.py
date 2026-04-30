# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080",
# META       "default_lakehouse_name": "manufacturing_data",
# META       "default_lakehouse_workspace_id": "ce753ac1-7233-4889-b54d-f0ca9df04e06",
# META       "known_lakehouses": [
# META         {
# META           "id": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
manu_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
target = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{manu_lh}/Tables/landing_sap"
source = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{manu_lh}/Files/data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = ['I_ADDRESS',
 'I_CUSTOMER',
 'I_EQUIPMENT',
 'I_EQUIPMENTTEXT',
 'I_PLANT',
 'I_PRODUCT',
 'I_PRODUCTDESCRIPTION',
 'I_SUPPLIER']

print("🚀 Ingesting SAP Data")

for t in tables:
    df = spark.read.format("parquet").load(f"{source}/{t}")
    df.write.mode("overwrite").format("delta").save(f"{target}/{t}")
    print(f"✅ Written table: {t}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
