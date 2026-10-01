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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F

# Configuration
TABLE_CONFIG = {
    "Orders":       {"pk": ["OrderID"]},
    "OrderDetails": {"pk": ["OrderID", "ProductID"]},
    "Customers":    {"pk": ["CustomerID"]},
    "Products":     {"pk": ["ProductID"]},
}

p_excel_filename = "FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"

excel_path = "abfss://03a51e0a-fbda-4ed9-a1a5-f5259819c960@onelake.dfs.fabric.microsoft.com/f2d18090-7295-438d-905e-205a42447bdc/Files/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"

print(f"Reading from: {excel_path}")

# Copy Excel from OneLake to local driver
local_excel_path = f"/tmp/{p_excel_filename}"

notebookutils.fs.cp(excel_path, f"file:{local_excel_path}")

print(f"Excel copied to: {local_excel_path}")

# Read Excel
excel_file = pd.ExcelFile(local_excel_path)

print("Available sheets:")
print(excel_file.sheet_names)

def normalize_name(name):
    return name.replace("_", "").replace(" ", "").lower()

ctrl_cols = [f.name for f in spark.table("control_table").schema.fields]
if "source" not in ctrl_cols:
    spark.sql("ALTER TABLE control_table ADD COLUMNS (source STRING)")


def esc(s):
    return "NULL" if s is None or s == "" else "'" + str(s).replace("'", "''") + "'"


def update_control_table(target_table, rows_loaded, status="Success", error_message=None):
    ctrl_rows = spark.sql("SELECT table_name, source FROM control_table").collect()
    norm_target = normalize_name(target_table)
    matched_row = next((r for r in ctrl_rows if normalize_name(r["table_name"]) == norm_target), None)

    if matched_row:
        current_source = matched_row["source"]
        new_source = "Excel" if current_source in (None, "Excel") else "Both"
        spark.sql(f"""
            UPDATE control_table
            SET status = {esc(status)},
                rows_loaded = {rows_loaded},
                last_ingestion_date = current_timestamp(),
                error_message = {esc(error_message)},
                source = {esc(new_source)}
            WHERE table_name = {esc(matched_row['table_name'])}
        """)
    else:
        spark.sql(f"""
            INSERT INTO control_table
            (table_name, source_url, primary_key, select_columns, status, last_ingestion_date, error_message, rows_loaded, source)
            VALUES ({esc(target_table)}, NULL, NULL, NULL, {esc(status)}, current_timestamp(), {esc(error_message)}, {rows_loaded}, 'Excel')
        """)

# Process Each Table
for table_name, config in TABLE_CONFIG.items():
    print(f"\nProcessing: {table_name}")

    existing_tables = [
        t.name for t in spark.catalog.listTables()
        if not t.name.startswith("stg_") and t.name != "control_table"
    ]
    norm_new = normalize_name(table_name)
    matched_table = next((t for t in existing_tables if normalize_name(t) == norm_new), None)
    target_table = matched_table if matched_table else table_name

    # Read specific sheet
    pdf = pd.read_excel(local_excel_path, sheet_name=table_name)

    print(f"Original columns: {list(pdf.columns)}")
    print(f"Rows: {len(pdf)}")

    # Remove Operation column
    operation_cols = [
        c for c in pdf.columns
        if c.strip().lower() == "operation"
    ]

    if operation_cols:
        pdf = pdf.drop(columns=operation_cols)
        print("Operation column removed")

    # Convert Pandas -> Spark
    df = spark.createDataFrame(pdf)
    print(f"Final columns: {df.columns}")

    if matched_table:
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"Appended into existing table: {target_table}")
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"{target_table} created successfully")

    update_control_table(target_table, rows_loaded=df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
