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

p_table_name = ""
p_primary_key = ""       
p_status = "Success"      
p_error_message = ""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit, col

def normalize_name(name):
    return name.replace("_", "").replace(" ", "").lower()

staging_table = f"stg_{p_table_name}"

runtime_status = p_status
runtime_error_message = p_error_message
rows_loaded = 0
rows_changed = 0
final_table = p_table_name

try:
    if runtime_status == "Success" and spark.catalog.tableExists(staging_table):

        existing_tables = [
            t.name for t in spark.catalog.listTables()
            if not t.name.startswith("stg_") and t.name != "control_table"
        ]

        norm_new = normalize_name(p_table_name)
        matched_table = next(
            (t for t in existing_tables if normalize_name(t) == norm_new),
            None
        )
        final_table = matched_table if matched_table else p_table_name

        df_new = spark.table(staging_table)

        for c in df_new.columns:
            if c.startswith("value."):
                df_new = df_new.withColumnRenamed(c, c.replace("value.", "", 1))

        odata_cols = [c for c in df_new.columns if c.lower().lstrip("@").startswith("odata")]
        if odata_cols:
            df_new = df_new.drop(*odata_cols)

        df_new = df_new.withColumn("CreatedDate", current_timestamp())
        df_new = df_new.withColumn("ModifiedDate", lit(None).cast("timestamp"))

        if spark.catalog.tableExists(final_table):
            target_types = {f.name: f.dataType for f in spark.table(final_table).schema.fields}
            for c in df_new.columns:
                if c in target_types:
                    df_new = df_new.withColumn(c, col(c).cast(target_types[c]))

        df_new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(final_table)

        rows_changed = df_new.count()
        rows_loaded = rows_changed

        spark.sql(f"TRUNCATE TABLE {staging_table}")

    else:
        rows_loaded = 0

except Exception as e:
    runtime_status = "Failed"
    runtime_error_message = str(e)
    rows_loaded = 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ctrl_cols = [f.name for f in spark.table("control_table").schema.fields]
if "source" not in ctrl_cols:
    spark.sql("ALTER TABLE control_table ADD COLUMNS (source STRING)")

def esc(s):
    return "NULL" if s is None or s == "" else "'" + str(s).replace("'", "''") + "'"

ctrl_rows = spark.sql("SELECT table_name, source FROM control_table").collect()
norm_final = normalize_name(final_table)
matched_row = next((r for r in ctrl_rows if normalize_name(r["table_name"]) == norm_final), None)

if matched_row:
    current_source = matched_row["source"]
    new_source = "API" if current_source in (None, "API") else "Both"
    spark.sql(f"""
        UPDATE control_table
        SET status = {esc(runtime_status)},
            rows_loaded = {rows_loaded},
            last_ingestion_date = current_timestamp(),
            error_message = {esc(runtime_error_message)},
            source = {esc(new_source)}
        WHERE table_name = {esc(matched_row['table_name'])}
    """)
else:
    spark.sql(f"""
        INSERT INTO control_table
        (table_name, source_url, primary_key, select_columns, status, last_ingestion_date, error_message, rows_loaded, source)
        VALUES ({esc(final_table)}, NULL, {esc(p_primary_key)}, NULL, {esc(runtime_status)}, current_timestamp(), {esc(runtime_error_message)}, {rows_loaded}, 'API')
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
