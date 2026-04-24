# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ed36b35e-461d-45e9-88b3-214aaf9e05ab",
# META       "default_lakehouse_name": "sap_masterdata",
# META       "default_lakehouse_workspace_id": "ce753ac1-7233-4889-b54d-f0ca9df04e06"
# META     }
# META   }
# META }

# CELL ********************


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, col, when


# --- Source/Target paths ---
sap_ws = "61a1bf4e-b39b-43df-85b7-31f42adc3a52"
sap_lh = "c8df9f16-22f0-4a7a-b78c-263966054302"
sap_base = f"abfss://{sap_ws}@onelake.dfs.fabric.microsoft.com/{sap_lh}/Tables"

tgt_ws = "ce753ac1-7233-4889-b54d-f0ca9df04e06"
tgt_lh = "ed36b35e-461d-45e9-88b3-214aaf9e05ab"
tgt_base = f"abfss://{tgt_ws}@onelake.dfs.fabric.microsoft.com/{tgt_lh}/Tables"

mfg_ws = tgt_ws
mfg_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
mfg_base = f"abfss://{mfg_ws}@onelake.dfs.fabric.microsoft.com/{mfg_lh}/Tables"

print("Paths configured successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 1. PLANTS/SITES - I_PLANT + I_ADDRESS for full location info
# ========================================
sap_plants = spark.read.format("delta").load(f"{tgt_base}/I_PLANT")
sap_plants.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_PLANT")

sap_address = spark.read.format("delta").load(f"{sap_base}/I_ADDRESS")
sap_address.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_ADDRESS")

mfg_sites = spark.read.format("delta").load(f"{mfg_base}/masterdata/sites")

# Join PLANT with ADDRESS to get city, country, street, postal code
sap_plant_with_addr = (sap_plants.alias("p")
    .join(sap_address.alias("a"), col("p.ADDRESSID") == col("a.ADDRESSID"), "left")
    .select(
        col("p.PLANT"),
        col("p.PLANTNAME"),
        col("a.CITYNAME").alias("sap_city"),
        col("a.COUNTRY").alias("sap_country"),
        col("a.REGION").alias("sap_region"),
        col("a.STREETNAME").alias("sap_street"),
        col("a.POSTALCODE").alias("sap_postal_code"),
        col("a.ADDRESSTIMEZONE").alias("sap_timezone"),
        col("p.SALESORGANIZATION"),
        col("p.DEFAULTPURCHASINGORGANIZATION"),
        col("p.FACTORYCALENDAR"),
        col("p.PLANTCATEGORY")
    )
    .orderBy("PLANT")
    .limit(5)
    .withColumn("row_num", row_number().over(Window.orderBy("PLANT"))))

# Map to manufacturing site IDs
site_data = mfg_sites.orderBy("siteid").collect()
site_ids = [row.siteid for row in site_data]
site_names = [row.sitename for row in site_data]
site_cities = [row.city for row in site_data]
site_countries = [row.country for row in site_data]
site_lats = [row.latitude for row in site_data]
site_longs = [row.longitude for row in site_data]

plant_mapping = (sap_plant_with_addr
    .withColumn("manufacturing_site_id", element_at(array(*[lit(x) for x in site_ids]), col("row_num")))
    .withColumn("manufacturing_site_name", element_at(array(*[lit(x) for x in site_names]), col("row_num")))
    .withColumn("manufacturing_city", element_at(array(*[lit(x) for x in site_cities]), col("row_num")))
    .withColumn("country", element_at(array(*[lit(x) for x in site_countries]), col("row_num")))
    .withColumn("latitude", element_at(array(*[lit(x) for x in site_lats]), col("row_num")))
    .withColumn("longitude", element_at(array(*[lit(x) for x in site_longs]), col("row_num")))
    .select(
        col("manufacturing_site_id").cast("int").alias("siteid"),
        col("PLANT").alias("sap_plant_code"),
        col("PLANTNAME").alias("sitename"),
        "manufacturing_site_name",
        "manufacturing_city",
        "country",
        "latitude", "longitude",
        # SAP address fields
        "sap_city", "sap_country", "sap_region",
        "sap_street", "sap_postal_code", "sap_timezone",
        # SAP org fields
        col("SALESORGANIZATION").alias("sap_sales_org"),
        col("DEFAULTPURCHASINGORGANIZATION").alias("sap_purchasing_org"),
        col("FACTORYCALENDAR").alias("sap_factory_calendar"),
        col("PLANTCATEGORY").alias("sap_plant_category")
    ))


country_name_to_code = {
    "united states": "US", "usa": "US", "us": "US",
    "germany": "DE", "de": "DE",
    "france": "FR", "fr": "FR",
    "united kingdom": "GB", "uk": "GB", "gb": "GB",
    "canada": "CA", "ca": "CA",
    "china": "CN", "cn": "CN",
    "brazil": "BR", "br": "BR"
}

def get_country_code(country_name):
    if not country_name:
        return None
    name = country_name.strip().lower()
    return country_name_to_code.get(name, None)

get_country_code_udf = udf(get_country_code, StringType())

# Demo city-to-address dictionary (with Detroit, Berlin, Munich added)
demo_address_dict = {
    # USA cities
    "New York":      {"street":"123 Broadway Ave",     "postal":"10001",    "region":"NY"},
    "Chicago":       {"street":"456 Michigan Ave",     "postal":"60611",    "region":"IL"},
    "Los Angeles":   {"street":"789 Hollywood Blvd",   "postal":"90028",    "region":"CA"},
    "Dallas":        {"street":"321 Elm Street",       "postal":"75201",    "region":"TX"},
    "Seattle":       {"street":"888 Pike Place",       "postal":"98101",    "region":"WA"},
    "Detroit":       {"street":"456 Woodward Ave",     "postal":"48226",    "region":"MI"},
    # Germany cities
    "Berlin":        {"street":"Alexanderplatz 14",    "postal":"10178",    "region":"Berlin"},
    "Munich":        {"street":"Leopoldstrasse 99",    "postal":"80802",    "region":"Bavaria"},
    # China cities
    "Shanghai":      {"street":"100 Nanjing East Road","postal":"200001",   "region":"Shanghai"},
    "Beijing":       {"street":"58 Chang’an Avenue",   "postal":"100031",   "region":"Beijing"},
    "Shenzhen":      {"street":"9 Fuhua 3rd Road",     "postal":"518048",   "region":"Guangdong"},
    "Guangzhou":     {"street":"38 Tianhe Road",       "postal":"510620",   "region":"Guangdong"},
    "Chengdu":       {"street":"12 Chunxi Road",       "postal":"610000",   "region":"Sichuan"},
    # Brazil cities
    "São Paulo":     {"street":"Av. Paulista, 1500",   "postal":"01310-200","region":"SP"},
    "Rio de Janeiro":{"street":"Rua Visconde de Pirajá, 200","postal":"22410-000", "region":"RJ"},
    "Belo Horizonte":{"street":"Av. Afonso Pena, 1200","postal":"30130-003","region":"MG"},
    "Porto Alegre":  {"street":"Rua dos Andradas, 1000","postal":"90020-007","region":"RS"},
    "Salvador":      {"street":"Rua Chile, 18",         "postal":"40020-000","region":"BA"},
}

from pyspark.sql.functions import udf

def get_demo_street(city):
    return demo_address_dict.get(city, {}).get("street", "123 Main Street")

def get_demo_postal(city):
    return demo_address_dict.get(city, {}).get("postal", "00000")

def get_demo_region(city):
    return demo_address_dict.get(city, {}).get("region", "NA")

get_street_udf = udf(get_demo_street, StringType())
get_postal_udf = udf(get_demo_postal, StringType())
get_region_udf = udf(get_demo_region, StringType())

plant_mapping_fixed = (
    plant_mapping
        .withColumn("sitename", col("manufacturing_site_name"))
        .withColumn("sap_city", col("manufacturing_city"))
        .withColumn("sap_country", get_country_code_udf(col("country")))
        .withColumn("sap_street", get_street_udf(col("manufacturing_city")))
        .withColumn("sap_postal_code", get_postal_udf(col("manufacturing_city")))
        .withColumn("sap_region", get_region_udf(col("manufacturing_city")))
        .select(
        "siteid",
        "sap_plant_code",
        "sitename",
        "country",
        "latitude", "longitude",
        # SAP address fields
        "sap_city", "sap_country", "sap_region",
        "sap_street", "sap_postal_code", "sap_timezone",
        # SAP org fields
        "sap_sales_org",
        "sap_purchasing_org",
        "sap_factory_calendar",
        "sap_plant_category"
    ))


# Write your output as before
#plant_mapping_fixed.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
#    .save(f"{tgt_base}/sites")
print(f" sites written: {plant_mapping_fixed.count()} rows")
display(plant_mapping_fixed)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# 2. EQUIPMENT/MACHINES - I_EQUIPMENT (cat M) + I_FUNCTIONALLOCATION for plant link
# ========================================
sap_equipment = spark.read.format("delta").load(f"{sap_base}/I_EQUIPMENT")
sap_equipment.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_EQUIPMENT")

sap_equip_text = spark.read.format("delta").load(f"{sap_base}/I_EQUIPMENTTEXT")
sap_equip_text.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_EQUIPMENTTEXT")

sap_funcloc = spark.read.format("delta").load(f"{sap_base}/I_FUNCTIONALLOCATION")
sap_funcloc.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_FUNCTIONALLOCATION")

sap_funcloc_text = spark.read.format("delta").load(f"{sap_base}/I_FUNCTIONALLOCATIONTEXT")
sap_funcloc_text.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_FUNCTIONALLOCATIONTEXT")

mfg_machines = spark.read.format("delta").load(f"{mfg_base}/masterdata/machines")

# Get English equipment names
equip_names = (sap_equip_text
    .filter(col("LANGUAGE") == "E")
    .select("EQUIPMENT", col("EQUIPMENTNAME")))

# Filter to category M (machinery) with construction year data
sap_equip_m = (sap_equipment
    .filter(col("EQUIPMENTCATEGORY") == "M")
    .filter(col("CONSTRUCTIONYEAR").isNotNull())
    .filter(col("CONSTRUCTIONYEAR") != "")
    .join(equip_names, "EQUIPMENT", "left")
    .filter(col("EQUIPMENTNAME").isNotNull())
    .orderBy("EQUIPMENT")
    .limit(8)
    .withColumn("row_num", row_number().over(Window.orderBy("EQUIPMENT"))))

# Get machine info for mapping
machine_data = mfg_machines.orderBy("machineid").collect()
machine_ids = [int(row.machineid) for row in machine_data]
machine_names = [row.machinename for row in machine_data]
machine_sites = [int(row.siteid) for row in machine_data]
machine_statuses = [row.status for row in machine_data]
machine_capacities = [int(row.hourlycapacity) for row in machine_data]

equip_mapping = (sap_equip_m
    .withColumn("manufacturing_machine_id", element_at(array(*[lit(x) for x in machine_ids]), col("row_num")))
    .withColumn("manufacturing_machine_name", element_at(array(*[lit(x) for x in machine_names]), col("row_num")))
    .withColumn("site_id", element_at(array(*[lit(x) for x in machine_sites]), col("row_num")))
    .select(
        col("manufacturing_machine_id").cast("int").alias("machine_id"),
        col("EQUIPMENT").alias("sap_equipment_number"),
        col("EQUIPMENTNAME").alias("machine_name"),
        "site_id",
        # SAP equipment fields
        col("EQUIPMENTCATEGORY").alias("sap_equipment_category"),
        col("TECHNICALOBJECTTYPE").alias("sap_technical_object_type"),
        col("ASSETMANUFACTURERNAME").alias("sap_manufacturer"),
        col("MANUFACTURERPARTTYPENAME").alias("sap_manufacturer_part_type"),
        col("CONSTRUCTIONYEAR").alias("sap_construction_year"),
        col("CONSTRUCTIONMONTH").alias("sap_construction_month"),
        col("SERIALNUMBER").alias("sap_serial_number"),
        col("OPERATIONSTARTDATE").alias("sap_operation_start_date"),
        col("ACQUISITIONVALUE").cast("double").alias("sap_acquisition_value"),
        col("CURRENCY").alias("sap_currency")
    ))

equip_mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{tgt_base}/machines")
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
sap_products = spark.read.format("delta").load(f"{sap_base}/I_PRODUCT")
sap_products.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_PRODUCT")

sap_prod_desc = spark.read.format("delta").load(f"{sap_base}/I_PRODUCTDESCRIPTION")
sap_prod_desc.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_PRODUCTDESCRIPTION")

mfg_products = spark.read.format("delta").load(f"{mfg_base}/masterdata/products")

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
    .limit(100))

mfg_prod_ids = [row.productid for row in mfg_products.select("productid").orderBy("productid").collect()]
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
    .save(f"{tgt_base}/products")
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
sap_suppliers = spark.read.format("delta").load(f"{sap_base}/I_SUPPLIER")
sap_suppliers.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_SUPPLIER")

mfg_components = spark.read.format("delta").load(f"{mfg_base}/masterdata/components")

sap_suppl_subset = (sap_suppliers
    .filter(col("SUPPLIERNAME").isNotNull())
    .filter(col("COUNTRY").isNotNull())
    .orderBy("SUPPLIER")
    .limit(10)
    .withColumn("row_num", row_number().over(Window.orderBy("SUPPLIER"))))

comp_ids = [int(row.componentid) for row in mfg_components.orderBy("componentid").limit(10).collect()]

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
    .save(f"{tgt_base}/suppliers")
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
sap_customers = spark.read.format("delta").load(f"{sap_base}/I_CUSTOMER")
sap_customers.write.mode("overwrite").format("delta").save(f"{tgt_base}/I_CUSTOMER")

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
    .save(f"{tgt_base}/customers")
print(f" customers written: {customer_subset.count()} rows")
display(customer_subset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================
# SUMMARY
# ========================================
print("=" * 60)
print("SAP MASTER DATA FABRICATION COMPLETE")
print("=" * 60)

tables = ["sites", "machines",
          "products", "suppliers", "customers"]
for t in tables:
    df = spark.read.format("delta").load(f"{tgt_base}/{t}")
    print(f"  {t}: {df.count()} rows, {len(df.columns)} columns")

print("\nKey improvements in this version:")
print("  - Plants: Joined I_PLANT + I_ADDRESS for city/country/street/timezone")
print("  - Equipment: Filtered to category M (machinery) with construction year + acquisition value")
print("  - NEW: Functional locations hierarchy for plant drill-down")
print("\nThese tables are now available in the sap_masterdata lakehouse.")

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
