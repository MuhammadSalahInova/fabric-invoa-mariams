# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2d18090-7295-438d-905e-205a42447bdc",
# META       "default_lakehouse_name": "BronzeSalesLH",
# META       "default_lakehouse_workspace_id": "03a51e0a-fbda-4ed9-a1a5-f5259819c960",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2d18090-7295-438d-905e-205a42447bdc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from pyspark.sql import Row
from pyspark.sql.types import *
import xml.etree.ElementTree as ET

BASE_URL = "https://services.odata.org/V4/Northwind/Northwind.svc"

WANTED_TABLES = [
    "Categories",
    "Customers",
    "Employees",
    "Products",
    "Regions",
    "Shippers",
    "Suppliers",
    "Territories",
    "Orders",
    "Order_Details"
]

resp = requests.get(BASE_URL + "/", headers={"Accept": "application/json"})
all_entities = [e["name"] for e in resp.json()["value"]]

entities = [e for e in all_entities if e in WANTED_TABLES]

missing = [t for t in WANTED_TABLES if t not in all_entities]

meta_resp = requests.get(BASE_URL + "/$metadata")
ns = {"edm": "http://docs.oasis-open.org/odata/ns/edm"}
root = ET.fromstring(meta_resp.content)

entity_columns = {}  
entity_pk = {}       

for et in root.iter("{http://docs.oasis-open.org/odata/ns/edm}EntityType"):
    name = et.attrib["Name"]
    cols = []
    for prop in et.findall("edm:Property", ns):
        col_name = prop.attrib["Name"]
        col_type = prop.attrib["Type"]

        if col_type == "Edm.Binary" or col_name.lower() in ("picture", "photo"):
            continue
        cols.append(col_name)
    key_els = et.findall("edm:Key/edm:PropertyRef", ns)
    pk = ",".join([k.attrib["Name"] for k in key_els]) if key_els else None
    entity_columns[name] = cols
    entity_pk[name] = pk

entityset_to_type = {}

for es in root.iter("{http://docs.oasis-open.org/odata/ns/edm}EntitySet"):
    es_name = es.attrib["Name"]
    et_full = es.attrib["EntityType"]
    et_simple = et_full.split(".")[-1]
    entityset_to_type[es_name] = et_simple

rows = []
for e in entities:
    et_name = entityset_to_type.get(e, e)
    cols = entity_columns.get(et_name, [])
    pk = entity_pk.get(et_name, "")
    rows.append(Row(
        table_name=e,
        source_url=f"{BASE_URL}/{e}",
        primary_key=pk,
        select_columns=",".join(cols),
        status="Pending",
        last_ingestion_date=None,
        error_message=None,
        source="API"
    ))

schema = StructType([
    StructField("table_name", StringType()),
    StructField("source_url", StringType()),
    StructField("primary_key", StringType()),
    StructField("select_columns", StringType()),
    StructField("status", StringType()),
    StructField("last_ingestion_date", TimestampType()),
    StructField("error_message", StringType()),
    StructField("source", StringType())
])

df = spark.createDataFrame(rows, schema)
df.write.format("delta").mode("overwrite").saveAsTable("control_table")

display(spark.table("control_table"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
