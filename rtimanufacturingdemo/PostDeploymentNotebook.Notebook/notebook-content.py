# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080",
# META       "default_lakehouse_name": "ManufacturingData",
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

# ## 🚀 Run this notebook after deployment
# 
# After deployment just click **"Run all"** on this notebook. **It takes up to 5 minutes** until the RealtimeDashboard is showing data as master data needs to be processed initially. **It takes additional 5 minutes** to build up the historical dataset for the Power BI Report. So either check the monitoring tab to see what is happening or grab a coffee ☕
# 
# Then start exploring:
# - the `RealtimeDashboard` in the Reporting folder let´s you analyze the streaming data processed with Realtime Intelligence
# - the `ManufacturingOperationsReport` in the Reporting folder is a Power BI Report built on the collected OEE data from the last months
# - the `TalkToManufacturingData`-DataAgent in the AI folder lets you talk to your Streaming and Lakehouse in natural language
# 
# From there dive deeper in the mechanics of the Eventhouse and Spark Notebooks which do the magic in the background.
# 
# ### On this notebook
# 
# This notebook performs several post-deployment steps to prepare your environment and start data creation and transformation flows. Below is an overview of what each step does:
# 
# 1. **Install required Fabric SDK package 📦**  
#    The first code cell installs the `msfabricpysdkcore` Python package, which provides the `FabricClientCore` class used to interact with Microsoft Fabric items (Lakehouses, Data Pipelines, KQL databases, etc.) via code.
# 
# 2. **Import Fabric client library 🧩**  
#    The next cell imports `FabricClientCore` from the installed package so it can be used to look up items (like the KQL database and pipeline) and trigger jobs programmatically.
# 
# 3. **Ingest SAP data 📥**  
#    The `%run IngestSAPData` cell runs a separate notebook/script named `IngestSAPData`.  
#    This simulates connecting to a SAP ERP system and ingests landing data into the Manufacturing Data Lakehouse.
#    
# 4. **Process master data 🧹**  
#    The `%run ProcessMasterdata` cell runs the `ProcessMasterdata` notebook/script.  
#    This step transforms, and standardizes master data (e.g., materials, plants, equipment) so it can be reliably used in downstream analytics and KQL.
# 
# 5. **Create OneLake shortcuts to mirrored KQL tables 🔗**  
#    The next cell:
#    - Reads the current workspace ID from `notebookutils.runtime.context`.
#    - Uses `FabricClientCore` to locate the `ManufacturingRealtimeAnalytics` KQL database in this workspace.
#    - For each table in `table_names` (currently `production_quality` and `sensors_parsed`):
#      - Creates a OneLake shortcut in the `ManufacturingData` Lakehouse.
#      - Points that shortcut to the corresponding mirrored KQL table path under `Tables/` in the `ManufacturingRealtimeAnalytics` KQL database.  
#    This makes the mirrored KQL data directly accessible from the Lakehouse Files/Tables experience.
# 
# 6. **Run the orchestration pipeline on-demand 🧵**  
#    The following cell:
#    - Uses `FabricClientCore` to find the data pipeline named `OrchestrationPipeline` in the current workspace.
#    - Starts an **on-demand run** of that pipeline (`run_on_demand_item_job` with `job_type="Pipeline"`).  
#    This triggers the operations to build up a Power BI data model. It builds a history of events, refreshes the semantic model and starts a notebook which updates the tables with the new event data using Spark Structured Streaming. The spark job stops after two hours. You can cancel also the notebook job earlier in the monitoring tab.
# 
# 7. **Simulate machine data 🤖📊**  
#    The last cell `%run SimulateMachineData` executes the `SimulateMachineData` notebook/script.  
#    This generates and ingests synthetic quality testbench and sensor data, so that dashboards, KQL queries, and reports have data to work with right after deployment. The simulation stops after two hours. You can cancel also the notebook job earlier in the monitoring tab.
# 
# You can run the notebook top to bottom after deployment to fully wire up data sources, transformations, shortcuts, and pipelines for your manufacturing scenario.


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

%run IngestSAPData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ProcessMasterdata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("🚀 Creating One Lake Shortcuts")

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
fcc = FabricClientCore()
kqldb_id = fcc.get_kql_database(workspace_id = ws_id, kql_database_name="ManufacturingRealtimeAnalytics").id
manu_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080" # this is automatically updated during deployment
table_names = ["production_quality", "sensors_parsed"]
for table_name in table_names:
    fcc.create_shortcut(workspace_id=ws_id,
                        item_id=manu_lh,
                        path="/Tables/ManufacturingRealtimeAnalytics",
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

orchestration_pipeline = fcc.get_data_pipeline(ws_id, data_pipeline_name = "OrchestrationPipeline")
operation = fcc.run_on_demand_item_job(workspace_id = ws_id, item_id=orchestration_pipeline.id, job_type="Pipeline")
operation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run SimulateMachineData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
