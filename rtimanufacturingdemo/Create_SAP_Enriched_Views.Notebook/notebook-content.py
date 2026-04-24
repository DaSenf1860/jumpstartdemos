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
# META       "default_lakehouse_workspace_id": "ce753ac1-7233-4889-b54d-f0ca9df04e06"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # SAP-Enriched Manufacturing Views
# Creates enriched Delta tables in ManufacturingData that join
# production data with SAP master data from sap_masterdata.
# Run this AFTER the Fabricate_SAP_MasterData notebook.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

mfg_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
mfg_base = f"abfss://{mfg_lh}@onelake.dfs.fabric.microsoft.com/{mfg_lh}/Tables"

sap_lh = "ed36b35e-461d-45e9-88b3-214aaf9e05ab"
sap_base = f"abfss://{sap_lh}@onelake.dfs.fabric.microsoft.com/{sap_lh}/Tables"

# Load manufacturing master data
products = spark.read.format("delta").load(f"{mfg_base}/masterdata/products")
machines = spark.read.format("delta").load(f"{mfg_base}/masterdata/machines")
sites = spark.read.format("delta").load(f"{mfg_base}/masterdata/sites")
components = spark.read.format("delta").load(f"{mfg_base}/masterdata/components")
production_records = spark.read.format("delta").load(f"{mfg_base}/masterdata/production_records")

# Load SAP mapping data
sap_products = spark.read.format("delta").load(f"{sap_base}/sap_products")
sap_plants = spark.read.format("delta").load(f"{sap_base}/sap_plants")
sap_equipment = spark.read.format("delta").load(f"{sap_base}/sap_equipment")
sap_suppliers = spark.read.format("delta").load(f"{sap_base}/sap_suppliers")
sap_customers = spark.read.format("delta").load(f"{sap_base}/sap_customers")

print("All source tables loaded successfully.")

# CELL ********************

# ========================================
# 1. ENRICHED SITES - Manufacturing sites + SAP plant & address info
# ========================================
enriched_sites = (sites.alias("s")
    .join(sap_plants.alias("p"), col("s.siteid") == col("p.site_id"), "left")
    .select(
        col("s.siteid"),
        col("s.sitename"),
        col("s.city"),
        col("s.country"),
        col("s.capacity"),
        col("s.establishedyear"),
        col("s.latitude"),
        col("s.longitude"),
        # SAP plant identity
        col("p.sap_plant_code"),
        col("p.sap_plant_name"),
        # SAP address (from I_ADDRESS)
        col("p.sap_city"),
        col("p.sap_country"),
        col("p.sap_region"),
        col("p.sap_street"),
        col("p.sap_postal_code"),
        col("p.sap_timezone"),
        # SAP org structure
        col("p.sap_sales_org"),
        col("p.sap_purchasing_org"),
        col("p.sap_factory_calendar"),
        col("p.sap_plant_category")
    ))

enriched_sites.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{mfg_base}/sap_enriched/enriched_sites")
print(f" enriched_sites: {enriched_sites.count()} rows")
display(enriched_sites)

# CELL ********************

# ========================================
# 2. ENRICHED MACHINES - Manufacturing machines + SAP equipment (cat M) details
# ========================================
enriched_machines = (machines.alias("m")
    .join(sap_equipment.alias("e"), col("m.machineid") == col("e.machine_id"), "left")
    .select(
        col("m.machineid"),
        col("m.machinename"),
        col("m.siteid"),
        col("m.status"),
        col("m.manufactureyear"),
        col("m.hourlycapacity"),
        col("m.city"),
        col("m.country"),
        col("m.operator"),
        # SAP equipment identity
        col("e.sap_equipment_number"),
        col("e.sap_equipment_name"),
        col("e.sap_equipment_category"),
        col("e.sap_technical_object_type"),
        # SAP manufacturer & construction
        col("e.sap_manufacturer"),
        col("e.sap_manufacturer_part_type"),
        col("e.sap_construction_year"),
        col("e.sap_construction_month"),
        col("e.sap_serial_number"),
        # SAP financials & operations
        col("e.sap_operation_start_date"),
        col("e.sap_acquisition_value"),
        col("e.sap_currency")
    ))

enriched_machines.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{mfg_base}/sap_enriched/enriched_machines")
print(f" enriched_machines: {enriched_machines.count()} rows")
display(enriched_machines)

# CELL ********************

# ========================================
# 3. ENRICHED PRODUCTS - Manufacturing products + SAP material info
# ========================================
enriched_products = (products.alias("p")
    .join(sap_products.alias("s"), col("p.productid") == col("s.product_id"), "left")
    .select(
        col("p.productid"),
        col("p.productname"),
        col("p.productcategory"),
        col("p.unitweight"),
        col("p.unitprice"),
        col("p.leadtimedays"),
        col("p.minimumstock"),
        col("s.sap_material_number"),
        col("s.sap_description"),
        col("s.sap_product_type"),
        col("s.sap_product_group"),
        col("s.sap_base_unit"),
        col("s.sap_gross_weight"),
        col("s.sap_net_weight"),
        col("s.sap_country_of_origin")
    ))

enriched_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{mfg_base}/sap_enriched/enriched_products")
print(f" enriched_products: {enriched_products.count()} rows")
display(enriched_products.limit(5))

# CELL ********************

# ========================================
# 4. ENRICHED COMPONENTS - Manufacturing components + SAP supplier info
# ========================================
enriched_components = (components.alias("c")
    .join(sap_suppliers.alias("s"), col("c.componentid") == col("s.primary_component_id"), "left")
    .select(
        col("c.componentid"),
        col("c.componentname"),
        col("c.componenttype"),
        col("c.unitcost"),
        col("c.supplierleaddays"),
        col("c.standardstock"),
        col("s.sap_supplier_number"),
        col("s.sap_supplier_name"),
        col("s.sap_country").alias("supplier_country"),
        col("s.sap_city").alias("supplier_city"),
        col("s.sap_industry").alias("supplier_industry")
    ))

enriched_components.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{mfg_base}/sap_enriched/enriched_components")
print(f" enriched_components: {enriched_components.count()} rows")
display(enriched_components.limit(10))

# CELL ********************

# ========================================
# 5. ENRICHED PRODUCTION RECORDS - Full context with SAP master data
# ========================================
enriched_production = (production_records.alias("pr")
    .join(sap_products.alias("sp"), col("pr.productid") == col("sp.product_id"), "left")
    .join(sap_plants.alias("pl"), col("pr.siteid") == col("pl.site_id"), "left")
    .join(sap_equipment.alias("eq"), col("pr.machineid") == col("eq.machine_id"), "left")
    .select(
        col("pr.productionid"),
        col("pr.productiondate"),
        col("pr.quantityproduced"),
        col("pr.defectiveunits"),
        col("pr.downtimeminutes"),
        col("pr.status"),
        # Product context
        col("pr.productid"),
        col("sp.sap_material_number"),
        col("sp.sap_description").alias("product_description"),
        col("sp.sap_product_type"),
        # Site context (now with address info)
        col("pr.siteid"),
        col("pl.sap_plant_code"),
        col("pl.manufacturing_site_name").alias("site_name"),
        col("pl.manufacturing_country").alias("site_country"),
        col("pl.sap_timezone").alias("site_timezone"),
        # Machine context (now with acquisition value & construction year)
        col("pr.machineid"),
        col("eq.sap_equipment_number"),
        col("eq.sap_equipment_name").alias("equipment_name"),
        col("eq.sap_manufacturer").alias("equipment_manufacturer"),
        col("eq.sap_construction_year").alias("equipment_year"),
        col("eq.sap_acquisition_value").alias("equipment_value")
    ))

enriched_production.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{mfg_base}/sap_enriched/enriched_production_records")
print(f" enriched_production_records: {enriched_production.count()} rows")
display(enriched_production.limit(5))

# CELL ********************

# ========================================
# SUMMARY
# ========================================
print("=" * 60)
print("SAP-ENRICHED VIEWS CREATED IN ManufacturingData")
print("=" * 60)

enriched_tables = [
    "sap_enriched/enriched_sites",
    "sap_enriched/enriched_machines",
    "sap_enriched/enriched_products",
    "sap_enriched/enriched_components",
    "sap_enriched/enriched_production_records"
]

for t in enriched_tables:
    df = spark.read.format("delta").load(f"{mfg_base}/{t}")
    print(f"  {t.split('/')[-1]}: {df.count()} rows, {len(df.columns)} columns")

print("\nKey enrichments:")
print("  - Sites: SAP plant + full address (city, street, postal, timezone)")
print("  - Machines: SAP equipment cat M (construction year, acquisition value, serial)")
print("  - Products: SAP material number, type (FERT/HALB/ROH), weight")
print("  - Components: SAP supplier name, country, industry")
print("  - Production: Full join across product, plant, equipment SAP data")
