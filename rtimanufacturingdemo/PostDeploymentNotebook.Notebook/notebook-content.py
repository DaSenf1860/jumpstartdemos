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

# MARKDOWN ********************

# # Run this notebook after deployment to adjust configurations and to ingest data


# CELL ********************

%pip install msfabricpysdkcore -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from msfabricpysdkcore import FabricClientCore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ingest_sap_data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run process_masterdata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("🚀 Creating One Lake Shortcuts")

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
fcc = FabricClientCore()
kqldb_id = fcc.get_kql_database(workspace_id = ws_id, kql_database_name="machinedata").id
manu_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
table_names = ["production_quality", "sensors_parsed"]
for table_name in table_names:
    fcc.create_shortcut(workspace_id=ws_id,
                        item_id=manu_lh,
                        path="/Tables/machinedata",
                        name=table_name,
                        target={"oneLake": {"itemId": kqldb_id,
                                            "path": f"Tables/{table_name}",
                                            "workspaceId": ws_id}})
    print(f"✅ Created One Lake Shortcut to mirrored KQL table {table_name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orchestration_pipeline = fcc.get_data_pipeline(ws_id, data_pipeline_name = "Pipeline_orchestration")
operation = fcc.run_on_demand_item_job(workspace_id = ws_id, item_id=orchestration_pipeline.id, job_type="Pipeline")
operation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run simulate_machine_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
