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

# PARAMETERS CELL ********************

p_bronze_retention_days = 30
p_vacuum_after_delete = True
p_silver_retention_days = 365
p_silver_prefix = "silver_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control_rows = spark.sql("SELECT table_name FROM control_table").collect()
tables_to_run = [r["table_name"] for r in control_rows]

print(f"num tables: {len(tables_to_run)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


cleanup_summary = []

for table_name in tables_to_run:
    bronze_table = table_name.replace("_", "").replace(" ", "").lower()

    if not spark.catalog.tableExists(bronze_table):
        print(f"{bronze_table}: does not exist, skipped")
        continue

    if "CreatedDate" not in spark.table(bronze_table).columns:
        print(f"{bronze_table}: no CreatedDate column, skipped")
        continue

    dt = DeltaTable.forName(spark, bronze_table)
    before_count = spark.table(bronze_table).count()

    dt.delete(f"CreatedDate < date_sub(current_date(), {p_bronze_retention_days})")

    after_count = spark.table(bronze_table).count()
    deleted = before_count - after_count

    cleanup_summary.append({
        "table": bronze_table,
        "retention_days": p_bronze_retention_days,
        "rows_before": before_count,
        "rows_deleted": deleted,
        "rows_after": after_count
    })

    print(f"{bronze_table}: deleted {deleted} row(s) older than {p_bronze_retention_days} day(s)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optional VACUUM to physically reclaim storage
if p_vacuum_after_delete:
    for row in cleanup_summary:
        if row["rows_deleted"] > 0:
            spark.sql(f"VACUUM {row['table']} RETAIN 168 HOURS")
            print(f"VACUUM done for {row['table']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pd.DataFrame(cleanup_summary))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


cleanup_summary = []

for table_name in tables_to_run:
    silver_table = f"{p_silver_prefix}{table_name.lower()}"

    if not spark.catalog.tableExists(silver_table):
        print(f"{silver_table}: does not exist, skipped")
        continue

    dt = DeltaTable.forName(spark, silver_table)
    before_count = spark.table(silver_table).count()

    dt.delete(
        "_scd_is_current = false AND "
        f"to_date(_scd_start_date, 'yyyy-MM-dd HH:mm:ss') < date_sub(current_date(), {p_silver_retention_days})"
    )

    after_count = spark.table(silver_table).count()
    deleted = before_count - after_count

    cleanup_summary.append({
        "table": silver_table,
        "retention_days": p_silver_retention_days,
        "rows_before": before_count,
        "rows_deleted": deleted,
        "rows_after": after_count
    })

    print(f"{silver_table}: deleted {deleted} historical row(s) older than {p_silver_retention_days} day(s)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optional VACUUM to physically reclaim storage
if p_vacuum_after_delete:
    for row in cleanup_summary:
        if row["rows_deleted"] > 0:
            spark.sql(f"VACUUM {row['table']} RETAIN 168 HOURS")
            print(f"VACUUM done for {row['table']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pd.DataFrame(cleanup_summary))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
