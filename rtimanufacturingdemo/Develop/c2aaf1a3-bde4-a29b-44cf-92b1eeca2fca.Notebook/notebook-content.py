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

# MARKDOWN ********************

# # Manufacturing Demo - Test Data Generation
# 
# This notebook generates normalized test data for a manufacturing system including production sites, machines, products, and production records.

# CELL ********************

%pip install msfabricpysdkcore -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from msfabricpysdkcore import FabricClientCore
fcc = FabricClientCore()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
manu_lh = fcc.get_lakehouse(ws_id, lakehouse_name="manufacturing_data").id
manufacturing_data = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{manu_lh}/Tables"
manufacturing_data


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

# ============================================================================
# TABLE 1: Production Sites
# ============================================================================
sites_data = {
    'siteid': [1, 2, 3, 4, 5],
    'sitename': ['Detroit Manufacturing', 'Shanghai Industrial', 'São Paulo Factory', 'Berlin Production', 'Munich Technical Center'],
    'city': ['Detroit', 'Shanghai', 'São Paulo', 'Berlin', 'Munich'],
    'country': ['USA', 'China', 'Brazil', 'Germany', 'Germany'],
    'capacity': [5000, 8000, 3500, 6500, 4200],  # units per day
    'establishedyear': [2010, 2015, 2008, 2012, 2018],
    'latitude': [42.3314, 31.2304, -23.5505, 52.5200, 48.1351],
    'longitude': [-83.0458, 121.4737, -46.6333, 13.4050, 11.5820]
}
sites_df = pd.DataFrame(sites_data)
print("\n=== PRODUCTION SITES ===")
print(sites_df)
print(f"Total sites: {len(sites_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 1B: Machine Types (Reference Table)
# ============================================================================
machine_types_data = {
    'MachineTypeID': [1, 2, 3, 4, 5],
    'MachineTypeName': ['CNC', 'Assembly', 'Stamping', 'Welding', 'Testing'],
    'Description': [
        'Computer Numerical Control routing and machining',
        'Automated assembly line equipment',
        'Metal stamping and pressing equipment',
        'Welding and joining equipment',
        'Quality testing and inspection equipment'
    ],
    'TypicalUnitPrice': [150000, 250000, 180000, 120000, 80000]  # USD
}
machine_types_df = pd.DataFrame(machine_types_data)
print("\n=== MACHINE TYPES ===")
print(machine_types_df)
print(f"Total machine types: {len(machine_types_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 1C: Machine Manufacturers (Reference Table)
# ============================================================================
manufacturers_data = {
    'ManufacturerID': [1, 2, 3, 4, 5],
    'ManufacturerName': ['Siemens', 'ABB Group', 'FANUC', 'Kuka', 'Yaskawa'],
    'Country': ['Germany', 'Switzerland', 'Japan', 'Germany', 'Japan'],
    'EstablishedYear': [1847, 1988, 1956, 1899, 1915],
    'Specialization': [
        'Industrial automation and CNC',
        'Robotics and electrical equipment',
        'CNC and industrial robots',
        'Welding and robotic solutions',
        'Motion control and robotics'
    ]
}
manufacturers_df = pd.DataFrame(manufacturers_data)
print("\n=== MACHINE MANUFACTURERS ===")
print(manufacturers_df)
print(f"Total manufacturers: {len(manufacturers_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 2: Production Machines
# ============================================================================

machine_ids = [101, 102, 103, 104, 105, 106, 107, 108]
link_to_manuals = [f"machine{mid}maintenance.pdf" for mid in machine_ids]

machines_data = {
    'machineid': machine_ids,
    'machinename': [
        'CNC Router A', 'CNC Router B', 'Assembly Line 1', 'Assembly Line 2',
        'Stamping Press 1', 'Welding Station', 'CNC Router C', 'Quality Tester'
    ],
    'siteid': [1, 1, 5, 2, 2, 4, 3, 3],
    'machinetypeid': [1, 1, 2, 2, 3, 4, 1, 5],  # References MachineTypes table
    'manufacturerid': [1, 3, 2, 2, 4, 4, 3, 1],  # References Manufacturers table
    'status': ['Active', 'Active', 'Active', 'Active', 'Maintenance', 'Active', 'Active', 'Active'],
    'manufactureyear': [2018, 2019, 2017, 2020, 2016, 2015, 2021, 2020],
    'hourlycapacity': [120, 120, 200, 200, 150, 100, 120, 180],
    'operator': ["amya@D365DemoTSCE69365762.OnMicrosoft.com","amya@D365DemoTSCE69365762.OnMicrosoft.com","alans@D365DemoTSCE69365762.OnMicrosoft.com",
        "alans@D365DemoTSCE69365762.OnMicrosoft.com","amya@D365DemoTSCE69365762.OnMicrosoft.com","amya@D365DemoTSCE69365762.OnMicrosoft.com","alans@D365DemoTSCE69365762.OnMicrosoft.com",
        "alans@D365DemoTSCE69365762.OnMicrosoft.com"],
    'link_to_manual': link_to_manuals
}
machines_df = pd.DataFrame(machines_data)
# Merge machines_df with sites_df on siteid
machines_df = machines_df.merge(sites_df, on='siteid', how='left')
print("\n=== PRODUCTION MACHINES ===")
print(machines_df)
print(f"Total machines: {len(machines_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 3: Products
# ============================================================================
# Generate 100 products with varied categories, weights, prices, and lead times
product_categories = ['Component', 'Subassembly', 'End Product', 'Raw Material']
product_prefixes = ['Standard', 'Premium', 'Industrial', 'Precision', 'Heavy-Duty', 'Compact', 'Modular', 'Custom']
product_types = ['Bearing', 'Shaft', 'Bracket', 'Fastener', 'Seal', 'Plate', 'Rod', 'Gear', 'Coupling', 'Housing', 'Frame', 'Assembly']

products_data = {
    'productid': list(range(1001, 1101)),  # 100 products from 1001-1100
    'productname': [f"{product_prefixes[i % len(product_prefixes)]} {product_types[i % len(product_types)]} {i // 10 + 1}" 
                    for i in range(100)],
    'productcategory': [product_categories[i % len(product_categories)] for i in range(100)],
    'unitweight': np.random.uniform(0.1, 50.0, 100).round(2),  # kg, 0.1 to 50 kg
    'unitprice': np.random.uniform(5.0, 500.0, 100).round(2),  # USD, 5 to 500 USD
    'leadtimedays': np.random.choice([3, 5, 7, 10, 14, 21, 30], 100),
    'minimumstock': np.random.randint(10, 500, 100)
}
products_df = pd.DataFrame(products_data)
print("\n=== PRODUCTS ===")
print(products_df.head(20))
print("...")
print(products_df.tail(5))
print(f"Total products: {len(products_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 3B: Components
# ============================================================================
# Generate 50 components that can be used in products
component_types = ['Fastener', 'Bearing', 'Seal', 'Spring', 'Bushing', 'Washer', 'Pin', 'Spacer', 'Gasket', 'Latch']
component_prefixes = ['Standard', 'Heavy-Duty', 'Precision', 'Stainless', 'Aluminum', 'Titanium']

components_data = {
    'componentid': list(range(2001, 2051)),  # 50 components from 2001-2050
    'componentname': [f"{component_prefixes[i % len(component_prefixes)]} {component_types[i % len(component_types)]} {i // 10 + 1}" 
                      for i in range(50)],
    'componenttype': [component_types[i % len(component_types)] for i in range(50)],
    'unitcost': np.random.uniform(0.50, 50.0, 50).round(2),  # USD, 0.50 to 50 USD
    'supplierleaddays': np.random.choice([1, 2, 3, 5, 7, 14], 50),
    'standardstock': np.random.randint(50, 2000, 50)
}
components_df = pd.DataFrame(components_data)
print("\n=== COMPONENTS ===")
print(components_df.head(20))
print("...")
print(components_df.tail(5))
print(f"Total components: {len(components_df)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 3C: Product-Component Combinations (Many-to-Many)
# ============================================================================
# Define which components are used in which products
product_component_records = []

# Each product uses 2-8 components
for product_id in range(1001, 1101):
    num_components = np.random.randint(2, 9)  # 2-8 components per product
    selected_components = np.random.choice(range(2001, 2051), size=num_components, replace=False)
    
    for component_id in selected_components:
        quantity_required = np.random.randint(1, 6)  # 1-5 of each component
        product_component_records.append({
            'productid': product_id,
            'componentid': component_id,
            'quantityrequired': quantity_required
        })

product_components_df = pd.DataFrame(product_component_records)
print("\n=== PRODUCT-COMPONENT COMBINATIONS ===")
print(product_components_df.head(20))
print("...")
print(product_components_df.tail(5))
print(f"Total product-component relationships: {len(product_components_df)}")
print(f"\nComponents per product (average): {len(product_components_df) / 100:.1f}")
print(f"\nProducts using each component (average): {len(product_components_df) / 50:.1f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 4: Machine-Product Capability (Normalization: Many-to-Many relationship)
# ============================================================================
# This table defines which machines can produce which products
# Generate realistic machine-product capabilities based on machine types
machine_product_records = []

# Define machine type capabilities
machine_capability_map = {
    101: {'type': 'CNC', 'products': list(range(1001, 1041))},      # CNC Router A: products 1001-1040
    102: {'type': 'CNC', 'products': list(range(1010, 1050))},      # CNC Router B: products 1010-1049
    103: {'type': 'Assembly', 'products': list(range(1035, 1075))}, # Assembly Line 1: products 1035-1074
    104: {'type': 'Assembly', 'products': list(range(1045, 1085))}, # Assembly Line 2: products 1045-1084
    105: {'type': 'Stamping', 'products': list(range(1020, 1060))}, # Stamping Press 1: products 1020-1059
    106: {'type': 'Welding', 'products': list(range(1030, 1070))},  # Welding Station: products 1030-1069
    107: {'type': 'CNC', 'products': list(range(1001, 1050))},      # CNC Router C: products 1001-1049
    108: {'type': 'Testing', 'products': list(range(1001, 1101))}   # Quality Tester: products 1001-1100
}

# Generate machine-product capabilities
for machine_id, capability in machine_capability_map.items():
    for product_id in capability['products']:
        if product_id <= 1100:  # Ensure product exists in our 100-product range
            machine_product_records.append({
                'machineid': machine_id,
                'productid': product_id,
                'setuptimeminutes': np.random.randint(15, 180),      # 15 to 180 minutes
                'cycletimeseconds': np.random.randint(60, 3600)      # 60 seconds to 60 minutes
            })

machine_product_df = pd.DataFrame(machine_product_records)
print("\n=== MACHINE-PRODUCT CAPABILITY MATRIX ===")
print(machine_product_df.head(20))
print("...")
print(machine_product_df.tail(5))
print(f"Total capabilities: {len(machine_product_df)}")
print(f"\nCapabilities by machine:")
print(machine_product_df.groupby('machineid').size())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TABLE 5: Production Records (Fact Table)
# ============================================================================
# Generate 50,000 production records with realistic combinations
base_date = datetime.today()
production_records = []

# Create weighted distribution for machines and sites
# Some machines produce more than others
machine_weights = {
    101: 0.15,  # CNC Router A - heavy production
    102: 0.15,  # CNC Router B - heavy production
    103: 0.12,  # Assembly Line 1
    104: 0.12,  # Assembly Line 2
    105: 0.10,  # Stamping Press 1
    106: 0.08,  # Welding Station
    107: 0.15,  # CNC Router C - heavy production
    108: 0.13   # Quality Tester - all products
}

# Generate 50,000 production records
production_id = 5000
for i in range(50000):
    # Select machine based on weights
    machine_id = np.random.choice(
        list(machine_weights.keys()), 
        p=list(machine_weights.values())
    )
    
    # Get site for this machine
    machine_row = machines_df[machines_df['machineid'] == machine_id].iloc[0]
    site_id = machine_row['siteid']
    
    # Select product that this machine can produce
    capable_products = machine_product_df[machine_product_df['machineid'] == machine_id]['productid'].tolist()
    if not capable_products:
        continue
    
    # Weight products so some are produced more frequently
    product_weights = np.random.dirichlet(np.ones(len(capable_products)))
    product_id = np.random.choice(capable_products, p=product_weights)
    
    # Generate realistic production metrics
    quantity_produced = np.random.randint(50, 500)  # Larger batches
    defect_rate = np.random.uniform(0.0, 0.1)  # 0-10% defect rate
    defective_units = max(0, int(quantity_produced * defect_rate))
    
    production_records.append({
        'productionid': production_id,
        'siteid': site_id,
        'machineid': machine_id,
        'productid': product_id,
        'productiondate': base_date - timedelta(days=np.random.randint(0, 365)),
        'quantityproduced': quantity_produced,
        'defectiveunits': defective_units,
        'downtimeminutes': np.random.choice([0, 0, 0, 0, 15, 30, 60, 120, 240], p=[0.5, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05, 0.02, 0.03]),
        'status': np.random.choice(['Completed', 'Completed', 'Completed', 'Completed', 'Partial', 'Failed'], p=[0.75, 0.05, 0.05, 0.05, 0.05, 0.05])
    })
    production_id += 1

production_df = pd.DataFrame(production_records)

# Calculate key metrics
total_produced = production_df['quantityproduced'].sum()
total_defective = production_df['defectiveunits'].sum()
defect_rate = (total_defective / total_produced * 100) if total_produced > 0 else 0

print("\n=== PRODUCTION RECORDS ===")
print(production_df.head(15))
print("...")
print(production_df.tail(5))
print(f"\nTotal production records: {len(production_df)}")
print(f"\n--- PRODUCTION SUMMARY ---")
print(f"Total units produced: {total_produced:,}")
print(f"Total defective units: {total_defective:,}")
print(f"Overall defect rate: {defect_rate:.2f}%")
print(f"Total downtime minutes: {production_df['downtimeminutes'].sum():,}")
print(f"\nProduction by site:")
print(production_df.groupby('siteid').agg({
    'quantityproduced': 'sum',
    'productionid': 'count'
}).rename(columns={'productionid': 'Records'}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# SUMMARY: Data Model Overview
# ============================================================================
print("\n" + "="*70)
print("MANUFACTURING DATA MODEL SUMMARY")
print("="*70)
print(f"\n✓ Sites:                  {len(sites_df):>3} records")
print(f"✓ Machines:               {len(machines_df):>3} records")
print(f"✓ Products:               {len(products_df):>3} records")
print(f"✓ Components:             {len(components_df):>3} records")
print(f"✓ Machine Capabilities:   {len(machine_product_df):>3} records")
print(f"✓ Product Components:     {len(product_components_df):>3} records")
print(f"✓ Production Records:     {len(production_df):>3} records")

print("\n" + "="*70)
print("DATA MODEL RELATIONSHIPS")
print("="*70)
print("""
Sites (1) ──────── (Many) Machines
  ↓
  Machines (Many) ──────── (Many) Products (via MachineProduct table)
  
Production Records (Fact Table) references:
  - Sites (SiteID)
  - Machines (MachineID)  
  - Products (ProductID)
""")

print("\n✓ All test data generated successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert pandas DataFrames to PySpark DataFrames and save as Delta tables
spark.createDataFrame(sites_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/sites")

spark.createDataFrame(machines_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/machines")

spark.createDataFrame(products_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/products")

spark.createDataFrame(components_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/components")

spark.createDataFrame(machine_product_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/machine_product_capability")

spark.createDataFrame(product_components_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/product_components")

spark.createDataFrame(production_df).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{manufacturing_data}/masterdata/production_records")

print("All tables saved to Manufacturing.masterdata schema")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token = notebookutils.credentials.getToken("kusto")
headers = {"Authorization": "Bearer " + token}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def kusto_command(csl):
    url = f"{eh_query_uri}/v1/rest/mgmt"
    body = {"csl": csl, "db": "machinedata"}
    resp = requests.post(url, json=body, headers=headers)
    return resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### KQL
command = """
 .create-or-alter external table machines_external (
     machineid: int,
     machinename: string,
     siteid: int,
     machinetypeid: int,
     manufacturerid: int,
     status: string,
     manufactureyear: int,
     hourlycapacity: real,
     operator: string,
     sitename: string,
     city: string,
     country: string,
     capacity: long,
     establishedyear: long,
     latitude: real,
     longitude: real
 )
 kind = delta
 (
     '{manufacturing_data}/masterdata/machines;impersonate'
 )"""

resp = kusto_command(command)
resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### KQL
command = """
.set-or-replace machines_internal <| external_table('machines_external')
"""

resp = kusto_command(command)
resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### KQL
command = f"""
.create-or-alter external table sites_external (
     siteid: int,
     sitename: string,
     city: string,
     country: string,
     capacity: int,
     establishedyear: int,
     latitude: real,
     longitude: real
 )
 kind = delta
 (
     '{manufacturing_data}/masterdata/sites;impersonate'
 )
"""

resp = kusto_command(command)
resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### KQL
command = """
.set-or-replace sites_internal <| external_table('sites_external')
"""

resp = kusto_command(command)
resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
