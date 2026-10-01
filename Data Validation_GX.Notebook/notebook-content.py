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
%pip install great_expectations --quiet

import great_expectations as gx
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, BooleanType
)

context = gx.get_context(mode="ephemeral")
MAX_CHANGE_PCT = 20.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ctrl_rows = spark.sql("SELECT table_name, primary_key, select_columns, status FROM control_table").collect()

ALL_TABLE_META = {}
for r in ctrl_rows:
    pk_cols = [c.strip() for c in r["primary_key"].split(",")] if r["primary_key"] else []
    columns = [c.strip() for c in r["select_columns"].split(",")] if r["select_columns"] else []
    ALL_TABLE_META[r["table_name"]] = {
        "pk_cols": pk_cols,
        "columns": columns,
        "status": r["status"],
    }


single_pk_to_table = {
    meta["pk_cols"][0]: t
    for t, meta in ALL_TABLE_META.items()
    if len(meta["pk_cols"]) == 1
}

tables_to_validate = {
    t: meta for t, meta in ALL_TABLE_META.items() if meta["status"] == "Success"
}


if not tables_to_validate:
    print("no tables")
else:
    print(f"will check {len(tables_to_validate)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_table(table_name, meta):
    target=table_name.replace("_", "").replace(" ", "").lower()
    pk_cols = meta["pk_cols"]

    sdf = spark.table(target)
    pdf = sdf.toPandas()

    data_source = context.data_sources.add_pandas(f"src_{table_name}")
    data_asset = data_source.add_dataframe_asset(name=f"asset_{table_name}")
    batch_def = data_asset.add_batch_definition_whole_dataframe(f"batch_{table_name}")
    batch = batch_def.get_batch(batch_parameters={"dataframe": pdf})

    results = []
    
    for col in pk_cols:
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        r = batch.validate(expectation)
        results.append({"table": table_name, "check": f"not_null:{col}", "success": bool(r.success)})


    if len(pk_cols) == 1:
        expectation = gx.expectations.ExpectColumnValuesToBeUnique(column=pk_cols[0])
        r = batch.validate(expectation)
        results.append({"table": table_name, "check": f"unique:{pk_cols[0]}", "success": bool(r.success)})
    elif len(pk_cols) > 1:
        expectation = gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=pk_cols)
        r = batch.validate(expectation)
        results.append({"table": table_name, "check": f"unique:({','.join(pk_cols)})", "success": bool(r.success)})


    fk_cols = [
        c for c in meta["columns"]
        if c not in pk_cols and c in single_pk_to_table and single_pk_to_table[c] != table_name
    ]
    numeric_cols = pdf.select_dtypes(include=["number"]).columns.tolist()
    check_cols = [c for c in numeric_cols if c not in pk_cols and c not in fk_cols]

    for col in check_cols:
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
        r = batch.validate(expectation)
        results.append({"table": table_name, "check": f"min_value:{col}", "success": bool(r.success)})

    return results, pdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_foreign_keys(table_name, meta):
    results = []
    pk_cols = meta["pk_cols"]

    fk_cols = [
        c for c in meta["columns"]
        if c not in pk_cols and c in single_pk_to_table and single_pk_to_table[c] != table_name
    ]

    if not fk_cols:
        return results

    target=table_name.replace("_", "").replace(" ", "").lower()
    sdf = spark.table(target)

    for col in fk_cols:
        ref_table = single_pk_to_table[col]

        if ref_table not in tables_to_validate:
            results.append({
                "table": table_name,
                "check": f"foreign_key:{col}->{ref_table}",
                "success": False,
                "orphan_rows": None,
                "note": f"  {ref_table} havnit loaded yet",
            })
            continue

        ref_target = ref_table.replace("_", "").replace(" ", "").lower()
        ref_sdf = spark.table(ref_target)
        ref_df = ref_sdf.select(F.col(col).alias("ref_key")).distinct()

        orphan_count = (
            sdf.join(ref_df, sdf[col] == ref_df["ref_key"], "left_anti")
               .count()
        )

        results.append({
            "table": table_name,
            "check": f"foreign_key:{col}->{ref_table}",
            "success": orphan_count == 0,
            "orphan_rows": orphan_count,
        })
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_results = []

for table_name, meta in tables_to_validate.items():
    print(f"Checking : {table_name}")
    basic_results, _ = validate_table(table_name, meta)
    fk_results = validate_foreign_keys(table_name, meta)
    all_results.extend(basic_results)
    all_results.extend(fk_results)

results_pdf = pd.DataFrame(all_results)
display(results_pdf)

if not results_pdf.empty:
    failed = results_pdf[results_pdf["success"] == False]
    print(f"\nTotal: {len(results_pdf)} | Succeeded: {(results_pdf['success']==True).sum()} | Failed: {len(failed)}")
else:
    failed = results_pdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_log_schema = StructType([
    StructField("run_timestamp", TimestampType(), True),
    StructField("table_name", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("success", BooleanType(), True),
])

if not results_pdf.empty:
    results_pdf_clean = results_pdf.rename(columns={"check": "check_name", "table": "table_name"})[
        ["table_name", "check_name", "success"]
    ]

    sdf_log = spark.createDataFrame(results_pdf_clean).withColumn("run_timestamp", F.current_timestamp())
    sdf_log = sdf_log.select("run_timestamp", "table_name", "check_name", "success")

    sdf_log.write.format("delta").mode("append").saveAsTable("bronze_validation_log")
else:
    print("no records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rowcount_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("run_timestamp", TimestampType(), True),
])

if not spark.catalog.tableExists("bronze_rowcount_history"):
    spark.createDataFrame([], schema=rowcount_schema)\
        .write.format("delta").mode("overwrite").saveAsTable("bronze_rowcount_history")
else:
    print("bronze_rowcount_history already exists")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_row_count(table_name):
    target=table_name.replace("_", "").replace(" ", "").lower()
    current_count = spark.table(target).count()

    history = spark.table("bronze_rowcount_history") \
        .filter(F.col("table_name") == table_name) \
        .orderBy(F.col("run_timestamp").desc()) \
        .limit(1) \
        .collect()

    result = {
        "table": table_name,
        "check": "row_count_stability",
        "current_count": current_count,
        "previous_count": None,
        "change_pct": None,
        "success": True,
    }

    if history:
        previous_count = history[0]["row_count"]
        result["previous_count"] = previous_count

        if previous_count == 0:
            change_pct = 0.0 if current_count == 0 else 100.0
        else:
            change_pct = ((current_count - previous_count) / previous_count) * 100

        result["change_pct"] = round(change_pct, 2)
        result["success"] = abs(change_pct) <= MAX_CHANGE_PCT

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rowcount_results = []

for table_name in tables_to_validate.keys():
    res = validate_row_count(table_name)
    rowcount_results.append(res)

rowcount_pdf = pd.DataFrame(rowcount_results)

if not rowcount_pdf.empty:
    failed_rowcount = rowcount_pdf[rowcount_pdf["success"] == False]
    if len(failed_rowcount) > 0:
        print(f"{len(failed_rowcount)} table(s) exceeded {MAX_CHANGE_PCT}% change:")
        display(failed_rowcount)
    else:
        print("Succeeded")
else:
    failed_rowcount = rowcount_pdf
    print("no tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
