# ============================================================
# OAC CATALOG ACL TRANSFORMER — SILVER LAYER
# Purpose: Read Bronze OAC_CATALOG_ACL, cleanse and enrich,
#          write to OAC_CATALOG_ACL_SILVER in same schema.
#
# Enrichments applied:
#   - Base64 decode ITEM_ID to readable catalog path
#   - Proper case CATALOG_TYPE (workbooks → Workbooks)
#   - ITEM_MODIFIED cast to proper timestamp
#   - PERMISSION_LEVEL label (Full Control, Read-Write, etc.)
#   - RISK_SCORE weighted integer (0–10)
#   - RISK_LEVEL label (Critical / High / Medium / Low / None)
#   - ACCOUNT_CATEGORY clean label (User / Application Role)
#   - EXTRACTED_AT cast to proper timestamp
#   - HAS_CREATED_DATE null quality flag
#   - DATA_QUALITY_FLAG row-level completeness indicator
#
# Source: arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
# Target: arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_SILVER
#
# Bronze table is never modified — Silver is a separate
# enriched copy. Both tables coexist in the same schema.
# ============================================================


# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────
# All configuration is defined here in one place so that
# developers can adjust targets and scoring weights without
# touching transformation logic downstream.
#
# RISK_SCORE weights are intentionally separated by concern:
#   - WEIGHT_CHANGE_PERM (4): Highest risk — this principal
#     can modify other users' access to this item
#   - WEIGHT_TAKE_OWN (3): Principal can reassign ownership,
#     bypassing access controls entirely
#   - WEIGHT_DELETE (2): Destructive action — item can be
#     permanently removed
#   - WEIGHT_WRITE (1): Data modification risk — item content
#     can be changed
# Max possible RISK_SCORE = 10 (all elevated perms granted)
# ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType
import base64

# ── Source & Target ──────────────────────────────────────────
AIDP_CATALOG    = "arganoadw_oacuser_sh"
AIDP_SCHEMA     = "oacuser"
BRONZE_TABLE    = "OAC_CATALOG_ACL"
SILVER_TABLE    = "OAC_CATALOG_ACL_SILVER"

BRONZE_FULL     = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{BRONZE_TABLE}"
SILVER_FULL     = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{SILVER_TABLE}"

# ── Risk Score Weights (adjust to reflect org risk posture) ──
WEIGHT_CHANGE_PERM  = 4   # Can change permissions on item
WEIGHT_TAKE_OWN     = 3   # Can take ownership of item
WEIGHT_DELETE       = 2   # Can delete item
WEIGHT_WRITE        = 1   # Can write/edit item

print("=" * 50)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  Source : {BRONZE_FULL}")
print(f"  Target : {SILVER_FULL}")
print(f"  Risk weights: ChangePerm={WEIGHT_CHANGE_PERM}, "
      f"TakeOwn={WEIGHT_TAKE_OWN}, "
      f"Delete={WEIGHT_DELETE}, "
      f"Write={WEIGHT_WRITE}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 2: SPARK SESSION & READ BRONZE
# ─────────────────────────────────────────────────────────────
# Reads directly from the Bronze table via the AIDP External
# Catalog connection (arganoadw_oacuser_sh). No wallet, no
# JDBC string — Spark resolves the 3-part catalog path.
#
# The Bronze table is the raw, unmodified API extract and is
# treated as the source of truth. It is never written to from
# this notebook. All enrichment goes to the Silver table only.
# ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.appName("oac_acl_silver").getOrCreate()

df_bronze = spark.table(BRONZE_FULL)

total_bronze = df_bronze.count()
print("=" * 50)
print("  SECTION 2 COMPLETE: Bronze Table Loaded")
print(f"  Rows read   : {total_bronze:,}")
print(f"  Columns     : {len(df_bronze.columns)}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 3: UDFs — CUSTOM TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────
# Four Python functions are defined and registered as Spark
# UDFs (User Defined Functions). UDFs allow row-level Python
# logic to run inside a distributed Spark DataFrame operation.
#
# decode_base64_id:
#   The OAC REST API returns catalog object paths as Base64
#   URL-safe encoded strings (required by the getACL endpoint).
#   This UDF reverses that encoding to restore the human-
#   readable catalog path, e.g.:
#     L3NoYXJlZC9TYWxlcw → /shared/Sales
#   Padding is added back before decoding since the API strips
#   the trailing '=' characters during extraction.
#
# get_permission_level:
#   Maps the 6 raw boolean permission flags from Bronze into a
#   single human-readable label for reporting. Priority order
#   ensures the highest privilege wins when multiple flags are
#   set. Labels: Full Control, Read-Write-Delete, Read-Write,
#   Read-Only, No Access.
#
# get_risk_score:
#   Computes a weighted integer risk score (0–10) based on
#   elevated privilege flags. Used in OAC report to surface
#   the highest-risk items and principals. Weights are defined
#   in Section 1 and can be tuned without touching this UDF.
#
# get_account_category:
#   The OAC API returns ACCOUNT_TYPE as either 'ApplicationRole'
#   or 'User', but the ACCOUNT_GUID for roles contains names
#   like 'AuthenticatedUser' or 'BIServiceAdministrator' which
#   are not individual people. This UDF maps both fields into
#   a clean label — 'Application Role' or 'Individual User' —
#   so the OAC report can filter meaningfully by principal type.
# ─────────────────────────────────────────────────────────────

def decode_base64_id(encoded_id):
    if not encoded_id:
        return None
    try:
        padded = encoded_id + "=" * (4 - len(encoded_id) % 4)
        return base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        return encoded_id   # return original if decode fails


def get_permission_level(read, write, delete, change_perm, take_own):
    if change_perm == 1 or take_own == 1:
        return "Full Control"
    elif delete == 1:
        return "Read-Write-Delete"
    elif write == 1:
        return "Read-Write"
    elif read == 1:
        return "Read-Only"
    else:
        return "No Access"


def get_risk_score(write, delete, change_perm, take_own):
    if write is None and delete is None and change_perm is None and take_own is None:
        return 0
    score = 0
    if change_perm == 1: score += WEIGHT_CHANGE_PERM
    if take_own    == 1: score += WEIGHT_TAKE_OWN
    if delete      == 1: score += WEIGHT_DELETE
    if write       == 1: score += WEIGHT_WRITE
    return score


def get_account_category(account_type, account_guid):
    if account_type == "ApplicationRole":
        return "Application Role"
    elif account_type == "User":
        return "Individual User"
    elif account_guid and "@" in account_guid:
        return "Individual User"
    else:
        return "Unknown"


# Register UDFs with Spark
udf_decode_id        = F.udf(decode_base64_id,    StringType())
udf_permission_level = F.udf(get_permission_level, StringType())
udf_risk_score       = F.udf(get_risk_score,       IntegerType())
udf_account_category = F.udf(get_account_category, StringType())

print("=" * 50)
print("  SECTION 3 COMPLETE: UDFs Registered")
print("  UDFs: decode_base64_id, get_permission_level,")
print("        get_risk_score, get_account_category")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 4: TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────
# All 10 enrichments are applied in sequence using a chained
# withColumn() pattern. The final .select() controls the
# column order in the Silver table — original Bronze columns
# first, derived enrichments grouped at the end.
#
# Enrichment summary:
#  1. ITEM_PATH_DECODED  — Base64 → readable catalog path
#  2. CATALOG_TYPE_LABEL — proper case (workbooks → Workbooks)
#  3. ITEM_MODIFIED_TS   — ISO string → Spark TimestampType
#  4. PERMISSION_LEVEL   — 6 flags → single readable label
#  5. RISK_SCORE         — weighted integer 0–10
#  6. RISK_LEVEL         — score bucket label
#  7. ACCOUNT_CATEGORY   — clean principal type label
#  8. EXTRACTED_AT_TS    — ISO string → Spark TimestampType
#  9. HAS_CREATED_DATE   — 0/1 flag for missing created dates
#                          (ITEM_CREATED is unreliable from the
#                          OAC API — most items return null.
#                          Flagged here so downstream reports
#                          do not depend on this column.)
# 10. DATA_QUALITY_FLAG  — row-level completeness indicator.
#                          Rows with missing principal or
#                          permission data are flagged so they
#                          can be excluded from Gold aggregates
#                          or investigated separately.
# ─────────────────────────────────────────────────────────────

df_silver = (df_bronze

    # 1. Decode Base64 ITEM_ID to readable catalog path
    .withColumn("ITEM_PATH_DECODED",
        udf_decode_id(F.col("ITEM_ID"))
    )

    # 2. Proper case CATALOG_TYPE for display labels
    .withColumn("CATALOG_TYPE_LABEL",
        F.initcap(F.col("CATALOG_TYPE"))
    )

    # 3. Cast ITEM_MODIFIED to proper timestamp
    #    Bronze stores this as a VARCHAR ISO 8601 string
    #    e.g. "2026-02-12T17:47:58Z" → TimestampType
    .withColumn("ITEM_MODIFIED_TS",
        F.to_timestamp(F.col("ITEM_MODIFIED"))
    )

    # 4. PERMISSION_LEVEL — human-readable privilege label
    .withColumn("PERMISSION_LEVEL",
        udf_permission_level(
            F.col("PERM_READ"),
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # 5. RISK_SCORE — weighted integer based on elevated perms
    .withColumn("RISK_SCORE",
        udf_risk_score(
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # 6. RISK_LEVEL — bucket label derived from RISK_SCORE
    #    Thresholds: Critical ≥7, High ≥4, Medium ≥2, Low ≥1
    .withColumn("RISK_LEVEL",
        F.when(F.col("RISK_SCORE") >= 7, "Critical")
         .when(F.col("RISK_SCORE") >= 4, "High")
         .when(F.col("RISK_SCORE") >= 2, "Medium")
         .when(F.col("RISK_SCORE") >= 1, "Low")
         .otherwise("None")
    )

    # 7. ACCOUNT_CATEGORY — clean principal type label
    .withColumn("ACCOUNT_CATEGORY",
        udf_account_category(
            F.col("ACCOUNT_TYPE"),
            F.col("ACCOUNT_GUID")
        )
    )

    # 8. Cast EXTRACTED_AT to proper timestamp
    .withColumn("EXTRACTED_AT_TS",
        F.to_timestamp(F.col("EXTRACTED_AT"))
    )

    # 9. HAS_CREATED_DATE — flag rows missing creation date
    .withColumn("HAS_CREATED_DATE",
        F.when(
            F.col("ITEM_CREATED").isNull() | (F.col("ITEM_CREATED") == ""),
            F.lit(0)
        ).otherwise(F.lit(1))
    )

    # 10. DATA_QUALITY_FLAG — row-level completeness check
    .withColumn("DATA_QUALITY_FLAG",
        F.when(
            F.col("ACCOUNT_GUID").isNull() | F.col("ACCOUNT_NAME").isNull(),
            "MISSING_PRINCIPAL"
        ).when(
            F.col("PERM_READ").isNull() & F.col("PERM_WRITE").isNull(),
            "MISSING_PERMISSIONS"
        ).otherwise("OK")
    )

    # Final column selection — Bronze originals first,
    # enrichments grouped at the end for clear lineage
    .select(
        # Original Bronze columns — preserved unchanged
        F.col("CATALOG_TYPE"),
        F.col("ITEM_ID"),
        F.col("ITEM_NAME"),
        F.col("ITEM_PATH").alias("ITEM_PATH_RAW"),
        F.col("ITEM_OWNER"),
        F.col("ITEM_MODIFIED"),
        F.col("ACCOUNT_GUID"),
        F.col("ACCOUNT_TYPE"),
        F.col("ACCOUNT_NAME"),
        F.col("PERM_READ"),
        F.col("PERM_WRITE"),
        F.col("PERM_LIST"),
        F.col("PERM_DELETE"),
        F.col("PERM_CHANGE_PERM"),
        F.col("PERM_TAKE_OWN"),
        F.col("EXTRACTED_AT"),
        # Silver enrichments
        F.col("CATALOG_TYPE_LABEL"),
        F.col("ITEM_PATH_DECODED").alias("ITEM_PATH"),
        F.col("ITEM_MODIFIED_TS"),
        F.col("ACCOUNT_CATEGORY"),
        F.col("PERMISSION_LEVEL"),
        F.col("RISK_SCORE"),
        F.col("RISK_LEVEL"),
        F.col("HAS_CREATED_DATE"),
        F.col("DATA_QUALITY_FLAG"),
        F.col("EXTRACTED_AT_TS")
    )
)

total_silver = df_silver.count()
ok_rows      = df_silver.filter(F.col("DATA_QUALITY_FLAG") == "OK").count()
flagged_rows = total_silver - ok_rows

print("=" * 50)
print("  SECTION 4 COMPLETE: Transformations Applied")
print(f"  Total rows   : {total_silver:,}")
print(f"  Quality OK   : {ok_rows:,}")
print(f"  Flagged rows : {flagged_rows:,}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 5: QUALITY SUMMARY
# ─────────────────────────────────────────────────────────────
# Three distribution tables are printed BEFORE writing to the
# Silver table. This gives the developer a chance to validate
# the enrichment results look correct before committing data.
#
# If the distributions look wrong (e.g. all rows showing
# "No Access" or unexpected RISK_LEVEL values), stop here,
# investigate the Bronze data, and re-run Section 4.
#
# Expected distributions based on current Bronze data:
#   PERMISSION_LEVEL: majority Read-Only or Full Control
#   RISK_LEVEL: mix of High and None (two ApplicationRoles)
#   ACCOUNT_CATEGORY: Application Role dominant (~100%)
#     — this is expected since the API returns role-based ACLs.
#     Individual users will appear after Silver-level user
#     directory enrichment in a future iteration.
# ─────────────────────────────────────────────────────────────

print("\n📊 Permission Level Distribution:")
df_silver.groupBy("PERMISSION_LEVEL") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show()

print("📊 Risk Level Distribution:")
df_silver.groupBy("RISK_LEVEL") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show()

print("📊 Account Category Distribution:")
df_silver.groupBy("ACCOUNT_CATEGORY") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show()

print("=" * 50)
print("  SECTION 5 COMPLETE: Quality Summary Printed")
print("  Review distributions above before proceeding.")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 6: WRITE SILVER TABLE
# ─────────────────────────────────────────────────────────────
# Writes to OAC_CATALOG_ACL_SILVER using the same saveAsTable()
# pattern as the Bronze notebook — no wallet, no JDBC, no DDL.
# The External Catalog connection handles the write to ADW.
#
# Write strategy: full overwrite on every run.
# Silver is a derived layer — it can always be rebuilt from
# Bronze. Incremental merge is not needed at this data volume.
#
# overwriteSchema=true allows column additions between runs
# without requiring a manual table drop/recreate in ADW.
# This is safe here because Silver is always fully regenerated
# from Bronze — there is no partial state to preserve.
# ─────────────────────────────────────────────────────────────

(df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FULL))

count = spark.table(SILVER_FULL).count()

print("=" * 50)
print("  SECTION 6 COMPLETE: Silver Table Written")
print(f"  Table   : {SILVER_FULL}")
print(f"  Rows    : {count:,}")
print(f"  Columns : {len(df_silver.columns)}")
print("  Status  : Queryable in AIDP + OAC")
print("=" * 50)

print("\n📊 Silver Preview (5 rows):")
spark.table(SILVER_FULL).select(
    "CATALOG_TYPE_LABEL",
    "ITEM_NAME",
    "ACCOUNT_NAME",
    "ACCOUNT_CATEGORY",
    "PERMISSION_LEVEL",
    "RISK_LEVEL",
    "RISK_SCORE",
    "ITEM_MODIFIED_TS"
).show(5, truncate=40)

print("\n" + "=" * 60)
print("  🏁 SILVER PIPELINE COMPLETE")
print("=" * 60)
