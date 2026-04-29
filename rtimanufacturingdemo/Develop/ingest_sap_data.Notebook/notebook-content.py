# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

%pip install msfabricpysdkcore -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from msfabricpysdkcore import FabricClientCore
fcc = FabricClientCore()
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
manu_lh = fcc.get_lakehouse(ws_id, lakehouse_name="manufacturing_data").id
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
 
for t in tables:
    df = spark.read.format("parquet").load(f"{source}/{t}")
    df.write.mode("overwrite").format("delta").save(f"{target}/{t}")
    print(f"Written table: {t}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
