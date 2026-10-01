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
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Global settings
import re

OUTLIER_IQR_MULTIPLIER = 1.5   # standard IQR multiplier for outlier detection
TOP_N_FREQUENT_VALUES = 3      # number of most frequent values to log per string column
MAX_OUTLIER_ROWS_PER_COLUMN = 50  # cap how many actual outlier rows we pull per column


SKIP_COLUMN_PREFIXES = ("@odata",)

NUMERIC_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "float", "double"}
DATE_TYPES = {"date", "timestamp"}
DATE_NAME_PATTERN = re.compile(r"date", re.IGNORECASE)


def classify_dtype(dtype_str, col_name=None):
    dtype_str = dtype_str.lower()
    if dtype_str in NUMERIC_TYPES or dtype_str.startswith("decimal"):
        return "numeric"
    elif dtype_str in DATE_TYPES:
        return "date"
    elif dtype_str == "string" and col_name and DATE_NAME_PATTERN.search(col_name):
        return "date_string"
    elif dtype_str == "boolean":
        return "boolean"
    else:
        return "string"


def should_skip_column(col_name):
    return any(col_name.startswith(p) for p in SKIP_COLUMN_PREFIXES)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tables to profile = every table in control_table with status = Success (no external params)
ctrl_rows = spark.sql("SELECT table_name, primary_key, status FROM control_table").collect()

ALL_TABLE_META = {}
for r in ctrl_rows:
    pk_cols = [c.strip() for c in r["primary_key"].split(",")] if r["primary_key"] else []
    ALL_TABLE_META[r["table_name"]] = {"pk_cols": pk_cols, "status": r["status"]}

tables_to_profile = {
    t: meta for t, meta in ALL_TABLE_META.items() if meta["status"] == "Success"
}

if not tables_to_profile:
    print("No tables with status Success found in control_table.")
else:
    print(f"Will profile {len(tables_to_profile)} table(s): {list(tables_to_profile.keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_outlier_rows(sdf, table_name, col, pk_cols, lower_bound, upper_bound, limit=MAX_OUTLIER_ROWS_PER_COLUMN):
    c = F.col(f"`{col}`")
    select_cols = list(pk_cols) + [col] if pk_cols else [col]
    select_col_refs = [F.col(f"`{sc}`") for sc in select_cols]

    outlier_sdf = (
        sdf.filter((c < lower_bound) | (c > upper_bound))
           .select(*select_col_refs)
           .orderBy(c.desc())
           .limit(limit)
    )

    rows = outlier_sdf.collect()
    results = []
    for row in rows:
        row_dict = row.asDict()
        value = row_dict.pop(col)
        pk_str = ", ".join(f"{k}={row_dict[k]}" for k in pk_cols) if pk_cols else "N/A"
        bound_type = "below_lower_bound" if value < lower_bound else "above_upper_bound"
        results.append({
            "table": table_name,
            "column": col,
            "primary_key": pk_str,
            "value": value,
            "bound_type": bound_type,
        })
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_numeric_column(sdf, table_name, col, pk_cols, total_count):
    c = F.col(f"`{col}`")
    stats = sdf.select(
        F.count(c).alias("non_null_count"),
        F.mean(c).alias("mean"),
        F.stddev(c).alias("stddev"),
        F.min(c).alias("min_value"),
        F.max(c).alias("max_value"),
        F.countDistinct(c).alias("distinct_count"),
    ).collect()[0]

    non_null_count = stats["non_null_count"]
    null_count = total_count - non_null_count
    null_pct = round((null_count / total_count) * 100, 2) if total_count > 0 else 0.0

    q1, median, q3 = sdf.approxQuantile(col, [0.25, 0.5, 0.75], 0.01) if non_null_count > 0 else (None, None, None)

    outlier_count = 0
    lower_bound = upper_bound = None
    iqr = None
    outlier_rows = []

    if q1 is not None and q3 is not None:
        iqr = q3 - q1
        lower_bound = q1 - OUTLIER_IQR_MULTIPLIER * iqr
        upper_bound = q3 + OUTLIER_IQR_MULTIPLIER * iqr

        min_val = stats["min_value"]
        if min_val is not None and min_val >= 0 and lower_bound < 0:
            lower_bound = 0

        outlier_count = sdf.filter(
            (c < lower_bound) | (c > upper_bound)
        ).count()

        if outlier_count > 0:
            outlier_rows = get_outlier_rows(sdf, table_name, col, pk_cols, lower_bound, upper_bound)

    outlier_pct = round((outlier_count / total_count) * 100, 2) if total_count > 0 else 0.0

    profile = {
        "column": col,
        "data_type": "numeric",
        "row_count": total_count,
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_count": stats["distinct_count"],
        "min_value": stats["min_value"],
        "max_value": stats["max_value"],
        "mean": round(stats["mean"], 4) if stats["mean"] is not None else None,
        "median": round(median, 4) if median is not None else None,
        "stddev": round(stats["stddev"], 4) if stats["stddev"] is not None else None,
        "q1": round(q1, 4) if q1 is not None else None,
        "q3": round(q3, 4) if q3 is not None else None,
        "iqr": round(iqr, 4) if iqr is not None else None,
        "lower_bound": round(lower_bound, 4) if lower_bound is not None else None,
        "upper_bound": round(upper_bound, 4) if upper_bound is not None else None,
        "outlier_count": outlier_count,
        "outlier_pct": outlier_pct,
        "most_frequent_values": None,
        "date_parse_failure_count": None,
    }
    return profile, outlier_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_string_column(sdf, col, total_count):
    c = F.col(f"`{col}`")
    stats = sdf.select(
        F.count(c).alias("non_null_count"),
        F.countDistinct(c).alias("distinct_count"),
        F.min(F.length(c)).alias("min_length"),
        F.max(F.length(c)).alias("max_length"),
    ).collect()[0]

    non_null_count = stats["non_null_count"]
    null_count = total_count - non_null_count
    null_pct = round((null_count / total_count) * 100, 2) if total_count > 0 else 0.0

    top_values_rows = (
        sdf.filter(c.isNotNull())
           .groupBy(c)
           .count()
           .orderBy(F.desc("count"))
           .limit(TOP_N_FREQUENT_VALUES)
           .collect()
    )
    top_values = [f"{row[col]} ({row['count']})" for row in top_values_rows]

    return {
        "column": col,
        "data_type": "string",
        "row_count": total_count,
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_count": stats["distinct_count"],
        "min_value": stats["min_length"],
        "max_value": stats["max_length"],
        "mean": None,
        "median": None,
        "stddev": None,
        "q1": None,
        "q3": None,
        "iqr": None,
        "lower_bound": None,
        "upper_bound": None,
        "outlier_count": None,
        "outlier_pct": None,
        "most_frequent_values": ", ".join(top_values) if top_values else None,
        "date_parse_failure_count": None,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_date_column(sdf, col, total_count, is_string=False):
    raw_c = F.col(f"`{col}`")
    # If the column is physically a string, cast it to a real date before computing
    # stats. Otherwise min/max would be computed on the raw string representation.
    c = F.to_date(raw_c) if is_string else raw_c

    stats = sdf.select(
        F.count(c).alias("non_null_count"),
        F.countDistinct(c).alias("distinct_count"),
        F.min(c).alias("min_value"),
        F.max(c).alias("max_value"),
    ).collect()[0]

    non_null_count = stats["non_null_count"]
    null_count = total_count - non_null_count
    null_pct = round((null_count / total_count) * 100, 2) if total_count > 0 else 0.0

    parse_failure_count = None
    if is_string:
        # Values that had real text in them (not null/blank) but could not be parsed
        # into a valid date -> real data-quality signal, worth flagging separately.
        parse_failure_count = sdf.filter(raw_c.isNotNull() & c.isNull()).count()

    return {
        "column": col,
        "data_type": "date_string" if is_string else "date",
        "row_count": total_count,
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_count": stats["distinct_count"],
        "min_value": str(stats["min_value"]) if stats["min_value"] is not None else None,
        "max_value": str(stats["max_value"]) if stats["max_value"] is not None else None,
        "mean": None,
        "median": None,
        "stddev": None,
        "q1": None,
        "q3": None,
        "iqr": None,
        "lower_bound": None,
        "upper_bound": None,
        "outlier_count": None,
        "outlier_pct": None,
        "most_frequent_values": None,
        "date_parse_failure_count": parse_failure_count,
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_table(table_name, meta):
    target=table_name.replace("_", "").replace(" ", "").lower()
    sdf = spark.table(target)
    sdf.cache()

    total_count = sdf.count()
    pk_cols = meta["pk_cols"]

    column_profiles = []
    outlier_details = []
    skipped_columns = []

    for col_name, dtype in sdf.dtypes:
        if should_skip_column(col_name):
            skipped_columns.append(col_name)
            continue

        kind = classify_dtype(dtype, col_name)
        if kind == "numeric":
            profile, col_outlier_rows = profile_numeric_column(sdf, table_name, col_name, pk_cols, total_count)
            outlier_details.extend(col_outlier_rows)
        elif kind in ("date", "date_string"):
            profile = profile_date_column(sdf, col_name, total_count, is_string=(kind == "date_string"))
        else:
            profile = profile_string_column(sdf, col_name, total_count)

        profile["table"] = table_name
        profile["is_primary_key"] = col_name in pk_cols
        column_profiles.append(profile)

    if skipped_columns:
        print(f"  Skipped columns in {table_name}: {skipped_columns}")

    # Table-level summary
    distinct_row_count = sdf.dropDuplicates().count()
    duplicate_row_count = total_count - distinct_row_count

    duplicate_pk_count = 0
    if pk_cols:
        pk_col_refs = [F.col(f"`{p}`") for p in pk_cols]
        duplicate_pk_count = (
            sdf.groupBy(*pk_col_refs).count()
               .filter(F.col("count") > 1)
               .agg(F.sum(F.col("count") - 1))
               .collect()[0][0] or 0
        )

    table_summary = {
        "table": table_name,
        "row_count": total_count,
        "column_count": len(sdf.columns) - len(skipped_columns),
        "duplicate_row_count": duplicate_row_count,
        "duplicate_pk_count": int(duplicate_pk_count),
    }

    sdf.unpersist()
    return column_profiles, table_summary, outlier_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_column_profiles = []
all_table_summaries = []
all_outlier_details = []

for table_name, meta in tables_to_profile.items():
    print(f"Profiling : {table_name}")
    col_profiles, table_summary, outlier_details = profile_table(table_name, meta)
    all_column_profiles.extend(col_profiles)
    all_table_summaries.append(table_summary)
    all_outlier_details.extend(outlier_details)

columns_pdf = pd.DataFrame(all_column_profiles)
tables_pdf = pd.DataFrame(all_table_summaries)
outliers_pdf = pd.DataFrame(all_outlier_details)

col_order = [
    "table", "column", "data_type", "is_primary_key", "row_count", "null_count", "null_pct",
    "distinct_count", "min_value", "max_value", "mean", "median", "stddev",
    "q1", "q3", "iqr", "lower_bound", "upper_bound", "outlier_count", "outlier_pct",
    "most_frequent_values", "date_parse_failure_count",
]
columns_pdf = columns_pdf[col_order]

print("\n=== Table summary ===")
display(tables_pdf)

print("\n=== Column profile (full detail) ===")
display(columns_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
