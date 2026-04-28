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

%pip install msfabricpysdkcore -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from msfabricpysdkcore import FabricClientCore

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, col, when
import requests

fcc = FabricClientCore()
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
manu_lh = fcc.get_lakehouse(ws_id, lakehouse_name="manufacturing_data").id
manu_data = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{manu_lh}/Tables"
manu_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eh = fcc.get_eventhouse(ws_id, eventhouse_name="machinedata")
eh_query_uri = eh.properties['queryServiceUri']
eh_query_uri

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 1. PLANTS/SITES - I_PLANT + I_ADDRESS for full location info
# ========================================

plant_df = spark.read.format("parquet").load(f"{manu_data}/landing_sap/I_PLANT")
address_full = spark.read.format("parquet").load(f"{manu_data}/landing_sap/I_ADDRESS")

# Map to manufacturing site IDs

site_ids = [1,2,3,4,5]
site_lats = [42.3314, 31.2304, -23.5505, 52.52, 48.1351]
site_longs = [-83.0458, 121.4737, -46.6333, 13.405, 11.582]

# Join PLANT with ADDRESS to get city, country, street, postal code
sap_plant_with_addr = (plant_df.alias("p")
    .join(address_full.alias("a"), col("p.ADDRESSID") == col("a.ADDRESSID"), "left")
    .withColumn("row_num", row_number().over(Window.orderBy("PLANT")))
    .withColumn("siteid", element_at(array(*[lit(x) for x in site_ids]), col("row_num")))
    .withColumn("latitude", element_at(array(*[lit(x) for x in site_lats]), col("row_num")))
    .withColumn("longitude", element_at(array(*[lit(x) for x in site_longs]), col("row_num")))
    .select(
        "siteid",
        col("p.PLANT").alias("sap_plant_code"),
        col("p.PLANTNAME").alias("sitename"),
        col("a.CITYNAME").alias("city"),
        col("a.COUNTRY").alias("country"),
        col("a.REGION").alias("region"),
        col("a.STREETNAME").alias("street"),
        col("a.POSTALCODE").alias("postal_code"),
        "latitude",
        "longitude",
        col("a.ADDRESSTIMEZONE").alias("addresstimezone"),
        col("p.SALESORGANIZATION").alias("salesorganization"),
        col("p.DEFAULTPURCHASINGORGANIZATION").alias("defaultpurchasingorganization"),
        col("p.FACTORYCALENDAR").alias("factorycalendar"),
        col("p.PLANTCATEGORY").alias("plantcategory"))
    .orderBy("sap_plant_code")
    .limit(5))

   

# Write your output as before
sap_plant_with_addr.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{manu_data}/masterdata/sites")
print(f" sites written: {sap_plant_with_addr.count()} rows")
display(sap_plant_with_addr)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 2. EQUIPMENT/MACHINES - I_EQUIPMENT (cat M) 
# ========================================
sap_equipment = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_EQUIPMENT")
equip_names = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_EQUIPMENTTEXT")

# Filter to category M (machinery) with construction year data
sap_equip_m = (sap_equipment
    .filter(col("EQUIPMENTCATEGORY") == "M")
    .filter(col("CONSTRUCTIONYEAR").isNotNull())
    .filter(col("CONSTRUCTIONYEAR") != "")
    .join(equip_names, "EQUIPMENT", "left")
    .filter(col("EQUIPMENTNAME").isNotNull())
    .orderBy("EQUIPMENT")
    .withColumn("row_num", row_number().over(Window.orderBy("EQUIPMENT"))))

machine_ids = [101, 102, 103, 104, 105, 106, 107, 108]
machine_sites = [1, 1, 5, 2, 2, 4, 3, 3]

equip_mapping = (sap_equip_m
    .withColumn("manufacturing_machine_id", element_at(array(*[lit(x) for x in machine_ids]), col("row_num")))
    .withColumn("site_id", element_at(array(*[lit(x) for x in machine_sites]), col("row_num")))
    .select(
        col("manufacturing_machine_id").cast("int").alias("machine_id"),
        col("EQUIPMENT").alias("equipment_number"),
        col("EQUIPMENTNAME").alias("machine_name"),
        "site_id",
        # SAP equipment fields
        col("EQUIPMENTCATEGORY").alias("equipment_category"),
        col("TECHNICALOBJECTTYPE").alias("technical_object_type"),
        col("ASSETMANUFACTURERNAME").alias("manufacturer"),
        col("MANUFACTURERPARTTYPENAME").alias("manufacturer_part_type"),
        col("CONSTRUCTIONYEAR").alias("construction_year"),
        col("CONSTRUCTIONMONTH").alias("construction_month"),
        col("SERIALNUMBER").alias("serial_number"),
        col("OPERATIONSTARTDATE").alias("operation_start_date"),
        col("ACQUISITIONVALUE").cast("double").alias("acquisition_value"),
        col("CURRENCY").alias("currency")
    ))

equip_mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{manu_data}/masterdata/machines")
print(f" machines written: {equip_mapping.count()} rows")
display(equip_mapping)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 3. PRODUCTS - Map SAP materials to manufacturing products
# ========================================
sap_products = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_PRODUCT")
sap_prod_desc = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_PRODUCTDESCRIPTION")

sap_desc_en = sap_prod_desc.filter(col("LANGUAGE") == "E").select("PRODUCT", "PRODUCTDESCRIPTION")

sap_prod_full = (sap_products.join(sap_desc_en, "PRODUCT", "left")
    .select(
        col("PRODUCT").alias("sap_material_number"),
        col("PRODUCTDESCRIPTION").alias("sap_description"),
        col("PRODUCTTYPE").alias("sap_product_type"),
        col("PRODUCTGROUP").alias("sap_product_group"),
        col("BASEUNIT").alias("sap_base_unit"),
        col("GROSSWEIGHT").cast("double").alias("sap_gross_weight"),
        col("NETWEIGHT").cast("double").alias("sap_net_weight"),
        col("DIVISION").alias("sap_division"),
        col("COUNTRYOFORIGIN").alias("sap_country_of_origin")
    )
    .filter(col("sap_product_type").isin("FERT", "HALB", "ROH"))
    .filter(col("sap_description").isNotNull())
    .dropDuplicates(["sap_material_number"])
    )
mfg_prod_ids = list(range(1000,1101))
sap_prod_numbered = sap_prod_full.withColumn("row_num", row_number().over(Window.orderBy("sap_material_number")))

product_mapping = (sap_prod_numbered
    .withColumn("manufacturing_product_id",
        when(col("row_num") <= len(mfg_prod_ids),
             element_at(array(*[lit(x) for x in mfg_prod_ids]), col("row_num")))
        .otherwise(col("row_num") + 1000))
    .select(
        col("manufacturing_product_id").cast("int").alias("product_id"),
        "sap_material_number", "sap_description", "sap_product_type",
        "sap_product_group", "sap_base_unit", "sap_gross_weight",
        "sap_net_weight", "sap_division", "sap_country_of_origin")
    .filter(col("product_id").isNotNull()))

product_mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{manu_data}/masterdata/products")
print(f" products written: {product_mapping.count()} rows")
display(product_mapping.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 5. SUPPLIERS - Select 10 SAP suppliers, link to manufacturing components
# ========================================
sap_suppliers = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_SUPPLIER")

sap_suppl_subset = (sap_suppliers
    .filter(col("SUPPLIERNAME").isNotNull())
    .filter(col("COUNTRY").isNotNull())
    .orderBy("SUPPLIER")
    .limit(10)
    .withColumn("row_num", row_number().over(Window.orderBy("SUPPLIER"))))

comp_ids = [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010]

supplier_mapping = (sap_suppl_subset
    .withColumn("primary_component_id",
        when(col("row_num") <= len(comp_ids),
             element_at(array(*[lit(x) for x in comp_ids]), col("row_num"))))
    .select(
        col("SUPPLIER").alias("sap_supplier_number"),
        col("SUPPLIERNAME").alias("sap_supplier_name"),
        col("COUNTRY").alias("sap_country"),
        col("CITYNAME").alias("sap_city"),
        col("REGION").alias("sap_region"),
        col("INDUSTRY").alias("sap_industry"),
        col("primary_component_id").cast("int")))

supplier_mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{manu_data}/masterdata/suppliers")
print(f" suppliers written: {supplier_mapping.count()} rows")
display(supplier_mapping)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 6. CUSTOMERS - Select 10 SAP customers for order-to-delivery demo
# ========================================
sap_customers = spark.read.format("delta").load(f"{manu_data}/landing_sap/I_CUSTOMER")

customer_subset = (sap_customers
    .filter(col("CUSTOMERNAME").isNotNull())
    .filter(col("COUNTRY").isNotNull())
    .orderBy("CUSTOMER")
    .limit(10)
    .select(
        col("CUSTOMER").alias("sap_customer_number"),
        col("CUSTOMERNAME").alias("sap_customer_name"),
        col("COUNTRY").alias("sap_country"),
        col("CITYNAME").alias("sap_city"),
        col("REGION").alias("sap_region"),
        col("INDUSTRY").alias("sap_industry")))

customer_subset.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{manu_data}/masterdata/customers")
print(f" customers written: {customer_subset.count()} rows")
display(customer_subset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def kusto_command(csl):
    token = notebookutils.credentials.getToken("kusto")
    headers = {"Authorization": "Bearer " + token}
    url = f"{eh_query_uri}/v1/rest/mgmt"
    body = {"csl": csl, "db": "machinedata"}
    resp = requests.post(url, json=body, headers=headers)
    return resp

### KQL

command = f"""
.create-or-alter external table machines_external (
    machine_id: int,
    equipment_number: string,
    machine_name: string,
    site_id: int,
    equipment_category: string,
    technical_object_type: string,
    manufacturer: string,
    manufacturer_part_type: string,
    construction_year: string,
    construction_month: string,
    serial_number: string,
    operation_start_date: datetime,
    acquisition_value: real,
    currency: string
)
kind = delta
(
    '{manu_data}/masterdata/machines;impersonate'
)
"""

resp = kusto_command(command)
if resp.status_code != 200:
    print(command, resp.text)

### KQL
command = """
.set-or-replace machines_internal <| external_table('machines_external')
"""

resp = kusto_command(command)
if resp.status_code != 200:
    print(command, resp.text)

### KQL

command = f"""
.create-or-alter external table sites_external (
    siteid: int,
    sap_plant_code: string,
    sitename: string,
    city: string,
    country: string,
    region: string,
    street: string,
    postal_code: string,
    latitude: real,
    longitude: real,
    addresstimezone: string,
    salesorganization: string,
    defaultpurchasingorganization: string,
    factorycalendar: string,
    plantcategory: string
)
kind = delta
(
    '{manu_data}/masterdata/sites;impersonate'
)
"""

resp = kusto_command(command)
if resp.status_code != 200:
    print(command, resp.text)

### KQL
command = """
.set-or-replace sites_internal <| external_table('sites_external')
"""

resp = kusto_command(command)
if resp.status_code != 200:
    print(command, resp.text)

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
