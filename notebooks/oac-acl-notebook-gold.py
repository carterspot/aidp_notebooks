# ============================================================
# OAC CATALOG ACL REPORT — GOLD LAYER
# Version: 1.0
# Purpose: Aggregate Silver OAC_CATALOG_ACL_SILVER into four
#          analytics-ready summary views written to a single
#          Gold table for OAC admin report consumption.
#
# Source:  arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_SILVER
# Target:  arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_GOLD
#
# Gold aggregations:
#   1. PERMISSION_SUMMARY  — item counts by catalog type,
#      account, permission level, and risk level
#   2. RISK_SUMMARY        — highest-risk items ranked by
#      risk score with owner and account details
#   3. OWNER_SUMMARY       — item counts and max risk score
#      per owner across catalog types
#   4. ACCOUNT_COVERAGE    — how many items each principal
#      has access to, broken down by catalog type
#
# All four views are written to a single table with a
# VIEW_TYPE column as the partition key. This keeps the
# OAC dataset simple — one connection, one dataset, one
# workbook filter to switch between views.
#
# PRE-REQUISITES:
#   1. Silver table must exist and be populated
#   2. arganoadw_oacuser_sh External Catalog registered in AIDP
#   3. oacuser schema pre-exists in ADW
#   4. Spark cluster attached to this notebook
#   5. No tokens.json required — pure Spark transform
# ============================================================


# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────
# Configuration is centralised here. Gold reads exclusively
# from Silver — never from Bronze directly. This ensures
# Gold always reflects the enriched, quality-checked data
# and not the raw API extract.
#
# VIEW_TYPE values written to the Gold table:
#   PERMISSION_SUMMARY  — primary OAC report dataset
#   RISK_SUMMARY        — high privilege alert canvas
#   OWNER_SUMMARY       — ownership analysis canvas
#   ACCOUNT_COVERAGE    — access breadth canvas
#
# TOP_N_RISK: Controls how many highest-risk items are
#   included in the RISK_SUMMARY view. Default 50 covers
#   the most actionable items without overwhelming the report.
#   Increase if the catalog grows significantly.
#
# QUALITY_FILTER: Only 'OK' rows from Silver are included
#   in Gold aggregates. Rows flagged MISSING_PRINCIPAL or
#   MISSING_PERMISSIONS are excluded to ensure clean totals.
# ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType
)
from datetime import datetime, timezone

# ── Source & Target ──────────────────────────────────────────
AIDP_CATALOG    = "arganoadw_oacuser_sh"
AIDP_SCHEMA     = "oacuser"
SILVER_TABLE    = "OAC_CATALOG_ACL_SILVER"
GOLD_TABLE      = "OAC_CATALOG_ACL_GOLD"

SILVER_FULL     = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{SILVER_TABLE}"
GOLD_FULL       = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{GOLD_TABLE}"

# ── Gold Configuration ───────────────────────────────────────
TOP_N_RISK      = 50     # Max rows in RISK_SUMMARY view
QUALITY_FILTER  = "OK"   # Only include Silver rows passing QC

print("=" * 50)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  Source : {SILVER_FULL}")
print(f"  Target : {GOLD_FULL}")
print(f"  Top N Risk items : {TOP_N_RISK}")
print(f"  Quality filter   : {QUALITY_FILTER}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 2: SPARK SESSION & READ SILVER
# ─────────────────────────────────────────────────────────────
# Reads from the Silver table via the AIDP External Catalog
# connection. Quality filter is applied immediately so all
# downstream Gold aggregations work on clean rows only.
#
# The Silver row count vs filtered count is printed so the
# developer can see how many rows were excluded by the quality
# filter. A large exclusion count warrants investigation of
# the Silver DATA_QUALITY_FLAG distribution before proceeding.
# ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.appName("oac_acl_gold").getOrCreate()

df_silver_raw = spark.table(SILVER_FULL)
df_silver     = df_silver_raw.filter(F.col("DATA_QUALITY_FLAG") == QUALITY_FILTER)

total_raw      = df_silver_raw.count()
total_filtered = df_silver.count()
excluded       = total_raw - total_filtered

print("=" * 50)
print("  SECTION 2 COMPLETE: Silver Table Loaded")
print(f"  Total Silver rows : {total_raw:,}")
print(f"  Quality OK rows   : {total_filtered:,}")
print(f"  Excluded rows     : {excluded:,}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 3: BUILD GOLD VIEWS
# ─────────────────────────────────────────────────────────────
# Four aggregated DataFrames are built from the Silver data.
# Each adds a VIEW_TYPE column so they can be unioned into
# a single Gold table while remaining filterable in OAC.
#
# A GOLD_CREATED_AT timestamp is added to every row so the
# OAC report can show when the Gold layer was last refreshed,
# independent of when Bronze was extracted.
#
# VIEW 1 — PERMISSION_SUMMARY:
#   Groups by catalog type label, account name, account
#   category, permission level, and risk level. Counts the
#   number of items at each combination. This is the primary
#   dataset for the Overview and Access Matrix canvases.
#   ITEM_OWNER is not grouped here — use OWNER_SUMMARY for
#   ownership-based analysis.
#
# VIEW 2 — RISK_SUMMARY:
#   Selects the top N highest-risk individual item+account
#   combinations from Silver, ordered by RISK_SCORE desc.
#   Includes ITEM_PATH for full catalog location context.
#   Feeds the High Privilege Alert canvas (Canvas 3).
#   Only includes rows where RISK_SCORE > 0 — no-risk rows
#   are not actionable in a risk alert context.
#
# VIEW 3 — OWNER_SUMMARY:
#   Groups by item owner to show how many items each person
#   owns, broken down by catalog type. MAX_RISK_SCORE shows
#   the highest risk score among all items they own — a
#   quick indicator of whether an owner has high-risk items
#   in their catalog area. Feeds the Canvas 4 owner drill.
#
# VIEW 4 — ACCOUNT_COVERAGE:
#   Groups by account name, account category, and catalog
#   type to show how many items each principal has access to.
#   SUM of each permission flag shows the total grants per
#   type. This feeds the stacked bar on Canvas 6 and the
#   access breadth analysis on Canvas 5.
# ─────────────────────────────────────────────────────────────

gold_ts = datetime.now(tz=timezone.utc).isoformat()

# ── View 1: PERMISSION_SUMMARY ───────────────────────────────
df_permission_summary = (df_silver
    .groupBy(
        "CATALOG_TYPE_LABEL",
        "ACCOUNT_NAME",
        "ACCOUNT_CATEGORY",
        "ACCOUNT_TYPE",
        "PERMISSION_LEVEL",
        "RISK_LEVEL"
    )
    .agg(
        F.count("ITEM_ID").alias("ITEM_COUNT"),
        F.countDistinct("ITEM_ID").alias("DISTINCT_ITEM_COUNT"),
        F.sum("PERM_READ").alias("TOTAL_READ"),
        F.sum("PERM_WRITE").alias("TOTAL_WRITE"),
        F.sum("PERM_DELETE").alias("TOTAL_DELETE"),
        F.sum("PERM_CHANGE_PERM").alias("TOTAL_CHANGE_PERM"),
        F.sum("PERM_TAKE_OWN").alias("TOTAL_TAKE_OWN"),
        F.avg("RISK_SCORE").alias("AVG_RISK_SCORE"),
        F.max("RISK_SCORE").alias("MAX_RISK_SCORE")
    )
    .withColumn("VIEW_TYPE",       F.lit("PERMISSION_SUMMARY"))
    .withColumn("GOLD_CREATED_AT", F.lit(gold_ts))
    # Null-fill columns not used in this view
    .withColumn("ITEM_NAME",   F.lit(None).cast(StringType()))
    .withColumn("ITEM_PATH",   F.lit(None).cast(StringType()))
    .withColumn("ITEM_OWNER",  F.lit(None).cast(StringType()))
    .withColumn("RISK_SCORE",  F.lit(None).cast(IntegerType()))
    .withColumn("ITEM_MODIFIED_TS", F.lit(None).cast(StringType()))
)

# ── View 2: RISK_SUMMARY ─────────────────────────────────────
df_risk_summary = (df_silver
    .filter(F.col("RISK_SCORE") > 0)
    .select(
        "CATALOG_TYPE_LABEL",
        "ITEM_NAME",
        "ITEM_PATH",
        "ITEM_OWNER",
        "ACCOUNT_NAME",
        "ACCOUNT_CATEGORY",
        "ACCOUNT_TYPE",
        "PERMISSION_LEVEL",
        "RISK_LEVEL",
        "RISK_SCORE",
        F.col("ITEM_MODIFIED_TS").cast(StringType()).alias("ITEM_MODIFIED_TS")
    )
    .orderBy(F.col("RISK_SCORE").desc())
    .limit(TOP_N_RISK)
    .withColumn("VIEW_TYPE",            F.lit("RISK_SUMMARY"))
    .withColumn("GOLD_CREATED_AT",      F.lit(gold_ts))
    # Null-fill aggregate columns not used in this view
    .withColumn("ITEM_COUNT",           F.lit(None).cast(LongType()))
    .withColumn("DISTINCT_ITEM_COUNT",  F.lit(None).cast(LongType()))
    .withColumn("TOTAL_READ",           F.lit(None).cast(LongType()))
    .withColumn("TOTAL_WRITE",          F.lit(None).cast(LongType()))
    .withColumn("TOTAL_DELETE",         F.lit(None).cast(LongType()))
    .withColumn("TOTAL_CHANGE_PERM",    F.lit(None).cast(LongType()))
    .withColumn("TOTAL_TAKE_OWN",       F.lit(None).cast(LongType()))
    .withColumn("AVG_RISK_SCORE",       F.lit(None).cast(DoubleType()))
    .withColumn("MAX_RISK_SCORE",       F.lit(None).cast(IntegerType()))
)

# ── View 3: OWNER_SUMMARY ────────────────────────────────────
df_owner_summary = (df_silver
    .groupBy(
        "ITEM_OWNER",
        "CATALOG_TYPE_LABEL"
    )
    .agg(
        F.countDistinct("ITEM_ID").alias("DISTINCT_ITEM_COUNT"),
        F.count("ITEM_ID").alias("ITEM_COUNT"),
        F.max("RISK_SCORE").alias("MAX_RISK_SCORE"),
        F.avg("RISK_SCORE").alias("AVG_RISK_SCORE"),
        F.sum("PERM_CHANGE_PERM").alias("TOTAL_CHANGE_PERM"),
        F.sum("PERM_TAKE_OWN").alias("TOTAL_TAKE_OWN")
    )
    .withColumn("VIEW_TYPE",       F.lit("OWNER_SUMMARY"))
    .withColumn("GOLD_CREATED_AT", F.lit(gold_ts))
    # Null-fill columns not used in this view
    .withColumn("ACCOUNT_NAME",     F.lit(None).cast(StringType()))
    .withColumn("ACCOUNT_CATEGORY", F.lit(None).cast(StringType()))
    .withColumn("ACCOUNT_TYPE",     F.lit(None).cast(StringType()))
    .withColumn("PERMISSION_LEVEL", F.lit(None).cast(StringType()))
    .withColumn("RISK_LEVEL",       F.lit(None).cast(StringType()))
    .withColumn("RISK_SCORE",       F.lit(None).cast(IntegerType()))
    .withColumn("ITEM_NAME",        F.lit(None).cast(StringType()))
    .withColumn("ITEM_PATH",        F.lit(None).cast(StringType()))
    .withColumn("TOTAL_READ",       F.lit(None).cast(LongType()))
    .withColumn("TOTAL_WRITE",      F.lit(None).cast(LongType()))
    .withColumn("TOTAL_DELETE",     F.lit(None).cast(LongType()))
    .withColumn("ITEM_MODIFIED_TS", F.lit(None).cast(StringType()))
)

# ── View 4: ACCOUNT_COVERAGE ─────────────────────────────────
df_account_coverage = (df_silver
    .groupBy(
        "ACCOUNT_NAME",
        "ACCOUNT_CATEGORY",
        "ACCOUNT_TYPE",
        "CATALOG_TYPE_LABEL"
    )
    .agg(
        F.countDistinct("ITEM_ID").alias("DISTINCT_ITEM_COUNT"),
        F.count("ITEM_ID").alias("ITEM_COUNT"),
        F.sum("PERM_READ").alias("TOTAL_READ"),
        F.sum("PERM_WRITE").alias("TOTAL_WRITE"),
        F.sum("PERM_DELETE").alias("TOTAL_DELETE"),
        F.sum("PERM_CHANGE_PERM").alias("TOTAL_CHANGE_PERM"),
        F.sum("PERM_TAKE_OWN").alias("TOTAL_TAKE_OWN"),
        F.max("RISK_SCORE").alias("MAX_RISK_SCORE"),
        F.avg("RISK_SCORE").alias("AVG_RISK_SCORE")
    )
    .withColumn("VIEW_TYPE",       F.lit("ACCOUNT_COVERAGE"))
    .withColumn("GOLD_CREATED_AT", F.lit(gold_ts))
    # Null-fill columns not used in this view
    .withColumn("PERMISSION_LEVEL", F.lit(None).cast(StringType()))
    .withColumn("RISK_LEVEL",       F.lit(None).cast(StringType()))
    .withColumn("RISK_SCORE",       F.lit(None).cast(IntegerType()))
    .withColumn("ITEM_NAME",        F.lit(None).cast(StringType()))
    .withColumn("ITEM_PATH",        F.lit(None).cast(StringType()))
    .withColumn("ITEM_OWNER",       F.lit(None).cast(StringType()))
    .withColumn("ITEM_MODIFIED_TS", F.lit(None).cast(StringType()))
)

print("=" * 50)
print("  SECTION 3 COMPLETE: Gold Views Built")
print(f"  PERMISSION_SUMMARY rows : {df_permission_summary.count():,}")
print(f"  RISK_SUMMARY rows       : {df_risk_summary.count():,}")
print(f"  OWNER_SUMMARY rows      : {df_owner_summary.count():,}")
print(f"  ACCOUNT_COVERAGE rows   : {df_account_coverage.count():,}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 4: UNION & WRITE GOLD TABLE
# ─────────────────────────────────────────────────────────────
# The four Gold views are unioned into a single DataFrame
# before writing. A single Gold table with a VIEW_TYPE column
# is preferred over four separate tables because:
#   - One OAC dataset connection covers all four views
#   - VIEW_TYPE can be used as a canvas-level filter in OAC
#   - Simpler catalog management — one table to govern
#   - All views share the same snapshot timestamp (GOLD_CREATED_AT)
#
# Column alignment: Each view fills unused columns with NULL
# (cast to the correct type) so the union schema is consistent.
# The column order in the final select matches the logical
# grouping: metadata → account → permissions → aggregates →
# audit fields.
#
# Write strategy mirrors Bronze and Silver: full overwrite
# on every run. Gold is always derived from Silver which is
# derived from Bronze — all three layers are fully replayable.
# ─────────────────────────────────────────────────────────────

# Align all four views to a consistent column order
FINAL_COLS = [
    "VIEW_TYPE",
    "CATALOG_TYPE_LABEL",
    "ITEM_NAME",
    "ITEM_PATH",
    "ITEM_OWNER",
    "ACCOUNT_NAME",
    "ACCOUNT_CATEGORY",
    "ACCOUNT_TYPE",
    "PERMISSION_LEVEL",
    "RISK_LEVEL",
    "RISK_SCORE",
    "ITEM_COUNT",
    "DISTINCT_ITEM_COUNT",
    "TOTAL_READ",
    "TOTAL_WRITE",
    "TOTAL_DELETE",
    "TOTAL_CHANGE_PERM",
    "TOTAL_TAKE_OWN",
    "AVG_RISK_SCORE",
    "MAX_RISK_SCORE",
    "ITEM_MODIFIED_TS",
    "GOLD_CREATED_AT"
]

df_gold = (df_permission_summary.select(FINAL_COLS)
    .union(df_risk_summary.select(FINAL_COLS))
    .union(df_owner_summary.select(FINAL_COLS))
    .union(df_account_coverage.select(FINAL_COLS))
)

# Write Gold — full overwrite, no DDL required
(df_gold.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FULL))

count = spark.table(GOLD_FULL).count()

print("=" * 50)
print("  SECTION 4 COMPLETE: Gold Table Written")
print(f"  Table   : {GOLD_FULL}")
print(f"  Rows    : {count:,}")
print(f"  Columns : {len(FINAL_COLS)}")
print("  Status  : Queryable in AIDP + OAC")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 5: GOLD SUMMARY & PREVIEW
# ─────────────────────────────────────────────────────────────
# Prints row counts per VIEW_TYPE and a sample preview of
# each view so the developer can validate the Gold table
# before pointing the OAC report at it.
#
# Expected output for this OAC instance:
#   PERMISSION_SUMMARY  — small number of rows (~10–20)
#     since we only have 2 principals × 5 catalog types
#   RISK_SUMMARY        — up to TOP_N_RISK rows, ordered
#     by RISK_SCORE desc — should show BI Service Admin rows
#   OWNER_SUMMARY       — one row per owner × catalog type
#     (~4 owners × 5 types = ~20 rows max)
#   ACCOUNT_COVERAGE    — one row per account × catalog type
#     (~2 accounts × 5 types = ~10 rows)
# ─────────────────────────────────────────────────────────────

print("\n📊 Gold row counts by VIEW_TYPE:")
spark.table(GOLD_FULL) \
    .groupBy("VIEW_TYPE") \
    .count() \
    .orderBy("VIEW_TYPE") \
    .show()

print("📊 PERMISSION_SUMMARY preview:")
spark.table(GOLD_FULL) \
    .filter(F.col("VIEW_TYPE") == "PERMISSION_SUMMARY") \
    .select("CATALOG_TYPE_LABEL", "ACCOUNT_NAME", "ACCOUNT_CATEGORY",
            "PERMISSION_LEVEL", "RISK_LEVEL", "ITEM_COUNT", "MAX_RISK_SCORE") \
    .orderBy(F.col("MAX_RISK_SCORE").desc()) \
    .show(10, truncate=30)

print("📊 RISK_SUMMARY preview (top 5):")
spark.table(GOLD_FULL) \
    .filter(F.col("VIEW_TYPE") == "RISK_SUMMARY") \
    .select("CATALOG_TYPE_LABEL", "ITEM_NAME", "ACCOUNT_NAME",
            "PERMISSION_LEVEL", "RISK_LEVEL", "RISK_SCORE") \
    .orderBy(F.col("RISK_SCORE").desc()) \
    .show(5, truncate=30)

print("📊 OWNER_SUMMARY preview:")
spark.table(GOLD_FULL) \
    .filter(F.col("VIEW_TYPE") == "OWNER_SUMMARY") \
    .select("ITEM_OWNER", "CATALOG_TYPE_LABEL",
            "DISTINCT_ITEM_COUNT", "MAX_RISK_SCORE", "TOTAL_CHANGE_PERM") \
    .orderBy(F.col("MAX_RISK_SCORE").desc()) \
    .show(10, truncate=30)

print("📊 ACCOUNT_COVERAGE preview:")
spark.table(GOLD_FULL) \
    .filter(F.col("VIEW_TYPE") == "ACCOUNT_COVERAGE") \
    .select("ACCOUNT_NAME", "CATALOG_TYPE_LABEL",
            "DISTINCT_ITEM_COUNT", "TOTAL_CHANGE_PERM", "MAX_RISK_SCORE") \
    .orderBy(F.col("TOTAL_CHANGE_PERM").desc()) \
    .show(10, truncate=30)

print("=" * 50)
print("  SECTION 5 COMPLETE: Gold Summary Printed")
print("  Review previews above before using in OAC.")
print("=" * 50)

print("\n" + "=" * 60)
print("  🏁 GOLD PIPELINE COMPLETE")
print(f"  Table: {GOLD_FULL}")
print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
print("=" * 60)
