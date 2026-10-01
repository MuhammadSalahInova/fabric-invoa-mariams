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

p_silver_prefix = "silver_"
p_null_placeholder = "Unknown"     
p_scd_high_date = "9999-12-31 23:59:59"
p_datetime_format = "yyyy-MM-dd HH:mm:ss"   # unified format for ALL date/datetime columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import pandas as pd
import re
from datetime import datetime

RUN_TS_STR = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PK_OVERRIDES = {
    "Order_Details": ["OrderID", "ProductID"],
}

control_rows = spark.sql("SELECT table_name, primary_key FROM control_table").collect()

TABLE_PK_MAP = {}
for r in control_rows:
    table_name = r["table_name"]
    if table_name in PK_OVERRIDES:
        TABLE_PK_MAP[table_name] = PK_OVERRIDES[table_name]
    elif r["primary_key"]:
        TABLE_PK_MAP[table_name] = [r["primary_key"]]
    else:
        TABLE_PK_MAP[table_name] = []
    print(table_name)

tables_to_run = list(TABLE_PK_MAP.keys())

print(f"num: {len(tables_to_run)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

COUNTRY_NAME_MAP = {
    "USA": "United States",
    "US": "United States",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "U.S.A.": "United States",
}


def standardize_phone_fax(df, col_name):
    if col_name not in df.columns:
        return df

    tmp_digits = f"_tmp_digits_{col_name}"
    tmp_len = f"_tmp_len_{col_name}"

    df = df.withColumn(tmp_digits, F.regexp_replace(F.col(col_name), r"[^0-9]", ""))
    df = df.withColumn(tmp_len, F.length(F.col(tmp_digits)))

    last_8 = F.substring(F.col(tmp_digits), -8, 8)
    prefix_part = F.expr(f"substring(`{tmp_digits}`, 1, `{tmp_len}` - 8)")

    standardized = F.when((F.col(col_name).isNull()) | (F.col(tmp_digits) == ""), None) \
        .when(F.col(tmp_len) <= 8, F.col(tmp_digits)) \
        .otherwise(F.concat(F.lit("("), prefix_part, F.lit(") "), last_8))

    df = df.withColumn(col_name, standardized).drop(tmp_digits, tmp_len)
    return df


def standardize_country(df, col_name):
    if col_name not in df.columns:
        return df

    mapping_expr = F.create_map([F.lit(x) for pair in COUNTRY_NAME_MAP.items() for x in pair])
    lookup_key = F.upper(F.trim(F.col(col_name)))
    df = df.withColumn(col_name, F.coalesce(mapping_expr[lookup_key], F.col(col_name)))
    return df


def parse_package_size(df, col_name):

    if col_name not in df.columns:
        return df

    cleaned = F.trim(F.regexp_replace(F.col(col_name), r"\s+", " ")) #multi whitspace
    cleaned = F.regexp_replace(cleaned, r"\.$", "") # . at the end
    df = df.withColumn(col_name, cleaned)

    count_match = F.regexp_extract(F.col(col_name), r"^(\d+)", 1) #digits at start
    package_count = F.when(count_match != "", count_match.cast("int")).otherwise(None) #trans count to int

    unit_raw = F.regexp_replace(F.col(col_name), r"^\d+\s*", "")#cut digits at first
    unit_raw = F.regexp_replace(unit_raw, r"^[-x]\s*", "")#- * fist
    package_unit = F.when(F.trim(unit_raw) != "", F.trim(unit_raw)).otherwise(None)

    df = df.withColumn(f"{col_name}_Count", package_count)
    df = df.withColumn(f"{col_name}_Unit", package_unit)
    return df


def normalize_column_name(name): #camel case
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    s2 = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', s1)
    words = [w for w in s2.replace(" ", "_").split("_") if w]
    if not words:
        return name
    return "".join(w.capitalize() for w in words)


def apply_title_case(df):
    cols_to_fix = set(c for c in df.columns if re.search(r'(name|title)', c, re.IGNORECASE))
    for col in cols_to_fix:
        df = df.withColumn(col, F.initcap(F.trim(F.col(col))))
    return df


def apply_city_cleanup(df):
    cols_to_fix = [c for c in df.columns if re.search(r'city', c, re.IGNORECASE)]
    for col in cols_to_fix:
        cleaned = F.regexp_replace(F.col(col), r"[^A-Za-z\s]", "") #spaecial chars
        df = df.withColumn(col, F.initcap(F.trim(cleaned)))
    return df

def fill_null_placeholders(df, date_cols, pk_cols):
    for c in date_cols:
        if c in df.columns and c not in pk_cols:
            df = df.withColumn(c, F.coalesce(F.date_format(c, p_datetime_format), F.lit(p_scd_high_date)))
    for c in [f.name for f in df.schema.fields if isinstance(f.dataType, StringType) and f.name not in pk_cols + date_cols]:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(p_null_placeholder)))
    return df

def fix_employee_nulls(df):
    if "ReportsTo" not in df.columns:
        return df

    mode_row = (
        df.filter(F.col("ReportsTo").isNotNull())
          .groupBy("ReportsTo")
          .count()
          .orderBy(F.desc("count"))
          .first()
    )

    if mode_row:
        df = df.withColumn(
            "ReportsTo",
            F.coalesce(F.col("ReportsTo"), F.lit(mode_row["ReportsTo"]))
        )

    return df


def fix_employee_dates(df):
    if "BirthDate" not in df.columns or "HireDate" not in df.columns:
        return df

    # HireDate cannot be before BirthDate
    df = df.withColumn(
        "HireDate",
        F.when(
            F.col("HireDate") < F.col("BirthDate"),
            F.add_months(F.col("BirthDate"), 216) # +18 year
        ).otherwise(F.col("HireDate"))
    )

    # HireDate cannot be in the future
    df = df.withColumn(
        "HireDate",
        F.when(
            F.col("HireDate") > F.current_timestamp(),
            F.current_timestamp()
        ).otherwise(F.col("HireDate"))
    )

    # BirthDate cannot be in the future
    df = df.withColumn(
        "BirthDate",
        F.when(
            F.col("BirthDate") > F.current_timestamp(),
            F.add_months(F.col("HireDate"), -216) # -18 year
        ).otherwise(F.col("BirthDate"))
    )

    return df


def fix_order_dates(df):
    if "ShippedDate" in df.columns and "RequiredDate" in df.columns:
        df = df.withColumn(
            "ShippedDate",
            F.when(
                F.col("ShippedDate").isNull() |
                (F.col("ShippedDate") < F.col("RequiredDate")),
                F.date_add(F.col("RequiredDate"), 10)
            ).otherwise(F.col("ShippedDate"))
        )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Columns excluded from SCD hash calculation
HASH_EXCLUDE = { "_scd_start_date", "_scd_end_date", "_scd_is_current", "_scd_hash", "_scd_key"}

# Standardize audit column names
def _canonicalize_scd_columns(df):
    names = {"createddate": "CreatedDate", "modifieddate": "ModifiedDate"}
    for target in set(names.values()):
        matches = [c for c in df.columns if c.lower() == target.lower()]
        if len(matches) > 1:
            keep = matches[0]
            for extra in matches[1:]:
                df = df.drop(extra)
            if keep != target:
                df = df.withColumnRenamed(keep, target)
        elif len(matches) == 1 and matches[0] != target:
            df = df.withColumnRenamed(matches[0], target)
    return df

# Generate hash for detecting business changes
def _add_hash_key(df, pk_cols):
    cols = sorted(c for c in df.columns if c not in pk_cols and c not in HASH_EXCLUDE)
    if not cols: raise ValueError("No business columns available for SCD2 hash")
    return (df
        .withColumn("_scd_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~NULL~")) for c in cols]), 256))
        .withColumn("_scd_key", F.concat_ws("||", *[F.col(c).cast("string") for c in pk_cols])))

# Build SCD2 versions and validity dates
def _versions(df, pk_cols):
    df = df.withColumn("_created_ts", F.to_timestamp("CreatedDate", p_datetime_format))
    w = Window.partitionBy("_scd_key", "_scd_hash").orderBy("_created_ts")
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    w = Window.partitionBy("_scd_key").orderBy(F.col("_created_ts"), F.col("_scd_hash"))
    return (df
        .withColumn("_scd_start_date", F.date_format("_created_ts", p_datetime_format))
        .withColumn("_scd_end_date", F.coalesce(F.date_format(F.lead("_created_ts").over(w), p_datetime_format), F.lit(p_scd_high_date)))
        .withColumn("_scd_is_current", F.lead("_created_ts").over(w).isNull())
        .drop("_created_ts"))

# Apply SCD Type 2 logic to the target table
def apply_scd2(table_name, source_df, pk_cols):
    target_table = f"{p_silver_prefix}{table_name.lower()}"
    
    # Standardize source column names
    source_df = _canonicalize_scd_columns(source_df)
    
    # Validate required SCD columns
    if not pk_cols or "CreatedDate" not in source_df.columns:
        raise ValueError(f"{table_name}: PK and CreatedDate are required")

    # Remove source audit and SCD columns
    source_df = source_df.drop(*[c for c in ["ModifiedDate", "_scd_start_date", "_scd_end_date", "_scd_is_current", "_scd_hash", "_scd_key"] if c in source_df.columns])
    
    # Prepare source data and generate SCD keys
    source_df = _add_hash_key(source_df.filter(F.col("CreatedDate").isNotNull()), pk_cols).withColumn("ModifiedDate", F.lit(RUN_TS_STR)).cache()
    source_cols = [c for c in source_df.columns if c not in {"ModifiedDate", "_scd_hash", "_scd_key", "_created_ts"}]
    final_cols = source_cols + ["ModifiedDate", "_scd_hash", "_scd_key", "_scd_start_date", "_scd_end_date", "_scd_is_current"]

    # Create target table on the first load
    if not spark.catalog.tableExists(target_table):
        out = _versions(source_df, pk_cols).select(*final_cols)
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        n = out.count(); source_df.unpersist()
        return {"table":table_name,"status":"Success","inserted":n,"expired":0,"unchanged":0,"error":None}

    # Find hashes that do not already exist
    target = spark.table(target_table)
    existing = target.select("_scd_key", "_scd_hash").dropDuplicates()
    new_hashes = source_df.select("_scd_key", "_scd_hash").dropDuplicates().join(existing, ["_scd_key","_scd_hash"], "left_anti")
    
    # Skip processing when there are no new versions
    if new_hashes.limit(1).count() == 0:
        n = source_df.select("_scd_key","_scd_hash").dropDuplicates().count(); source_df.unpersist()
        return {"table":table_name,"status":"Skipped","inserted":0,"expired":0,"unchanged":n,"error":None}

    # Identify keys that need rebuilding
    keys = source_df.select("_scd_key").distinct()
    old = target.join(keys, "_scd_key").select(*source_cols,"ModifiedDate","_scd_hash","_scd_key").withColumn("_old",F.lit(1))
    new = source_df.select(*source_cols,"ModifiedDate","_scd_hash","_scd_key").withColumn("_old",F.lit(0))
    
    # Combine old and new versions and remove duplicates
    all_versions = old.unionByName(new)
    w = Window.partitionBy("_scd_key","_scd_hash").orderBy(F.col("_old").desc())
    all_versions = all_versions.withColumn("_rn",F.row_number().over(w)).filter("_rn = 1").drop("_rn","_old")
    
    # Recalculate SCD2 validity periods
    rebuilt = _versions(all_versions, pk_cols)

    # Keep target rows that were not affected
    unaffected = target.join(keys,"_scd_key","left_anti").select(*final_cols)
    result = unaffected.unionByName(rebuilt.select(*final_cols))
    
    # Replace target with the rebuilt SCD2 data
    result.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(target_table)

    # Calculate load statistics
    inserted = new_hashes.count()
    old_current = target.filter("_scd_is_current").select("_scd_key",F.col("_scd_hash").alias("old_hash"))
    new_current = rebuilt.filter("_scd_is_current").select("_scd_key",F.col("_scd_hash").alias("new_hash"))
    expired = old_current.join(new_current,"_scd_key").filter("old_hash <> new_hash").count()
    unchanged = max(source_df.select("_scd_key","_scd_hash").dropDuplicates().count() - inserted, 0)
    
    # Release cached source data
    source_df.unpersist()
    return {"table":table_name,"status":"Success","inserted":inserted,"expired":expired,"unchanged":unchanged,"error":None}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


def fix_dates_after_2020(df, date_cols):
    for c in date_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    F.year(F.col(c)) > 2020,
                    F.to_timestamp(F.concat(F.lit("1995-"),F.date_format(F.col(c), "MM-dd HH:mm:ss")))
                ).otherwise(F.col(c))
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_table(table_name):
    source_table = table_name.replace("_", "").replace(" ", "").lower()

    if not spark.catalog.tableExists(source_table):
        raise Exception(f"{source_table} does not exist")

    df = _canonicalize_scd_columns(spark.table(source_table))

    # Remove source / old SCD metadata
    drop_cols = [
        c for c in [
            "ModifiedDate",
            "_scd_start_date",
            "_scd_end_date",
            "_scd_is_current",
            "_scd_hash",
            "_scd_key",
        ]
        if c in df.columns
    ]
    if drop_cols:
        df = df.drop(*drop_cols)


    # Primary key
    pk_cols = [c for c in TABLE_PK_MAP.get(table_name, []) if c in df.columns]
    if not pk_cols:
        raise Exception(f"{table_name}: no valid primary key columns found")

    if "CreatedDate" not in df.columns:
        raise Exception(f"{table_name}: CreatedDate is required")

    # Remove invalid PK / CreatedDate
    pk_cond = F.lit(True)
    for c in pk_cols:
        pk_cond &= F.col(c).isNotNull()

    df = df.filter(pk_cond)
    df = df.filter(F.col("CreatedDate").isNotNull())

    # Clean strings
    string_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, StringType)
    ]

    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(
                (F.trim(F.col(c)) == "") |
                (F.lower(F.trim(F.col(c))) == "null"),
                None
            ).otherwise(F.trim(F.col(c)))
        )

    # Dates
    date_cols = [
        c for c in df.columns
        if re.search(r"date", c, re.IGNORECASE)
    ]
    for c in date_cols:
        df = df.withColumn(c, F.to_timestamp(c))

    # Standard transformations
    for c in [
        c for c in df.columns
        if re.search(r"country", c, re.IGNORECASE)
    ]:
        df = standardize_country(df, c)

    df = apply_title_case(df)
    df = apply_city_cleanup(df)

    for c in [
        c for c in df.columns
        if re.search(r"(phone|fax)", c, re.IGNORECASE)
    ]:
        df = standardize_phone_fax(df, c)

    if "QuantityPerUnit" in df.columns:
        df = parse_package_size(df, "QuantityPerUnit")

    if "FirstName" in df.columns and "LastName" in df.columns:
        df = (
            df.withColumn(
                "FullName",
                F.concat_ws(" ", "FirstName", "LastName")
            )
            .drop("FirstName", "LastName")
        )

    # Business rules
    if table_name.lower() == "employees":
        df = fix_employee_nulls(df)
        df = fix_employee_dates(df)

    if table_name.lower() == "orders":
        df = fix_order_dates(df)   
        df = fix_dates_after_2020(df, date_cols)

    # Final NULL handling
    df = fill_null_placeholders(df, date_cols, pk_cols)

    # Normalize column names
    df = df.toDF(*[normalize_column_name(c) for c in df.columns])
    pk_cols = [normalize_column_name(c) for c in pk_cols]

    return apply_scd2(table_name, df, pk_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_summary = []
for table_name in tables_to_run:
    try:
        r = transform_table(table_name); run_summary.append(r)
        print(f"{table_name}: {r['status']} -> inserted={r['inserted']}, expired={r['expired']}, unchanged={r['unchanged']}")
    except Exception as e:
        r = {"table":table_name,"status":"Failed","inserted":None,"expired":None,"unchanged":None,"error":str(e)}
        run_summary.append(r); print(f"{table_name}: Failed - {e}")
display(pd.DataFrame(run_summary))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BronzeSalesLH.silver_orders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
