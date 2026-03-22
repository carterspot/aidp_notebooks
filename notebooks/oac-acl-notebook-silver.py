# ============================================================
# OAC CATALOG ACL TRANSFORMER — SILVER LAYER
# Purpose: Read Bronze OAC_CATALOG_ACL, cleanse and enrich,
#          write to OAC_CATALOG_ACL_SILVER in same schema.
#
# Enrichments applied:
#   - Base64 decode ITEM_ID to readable catalog path
#   - Proper case CATALOG_TYPE (workbooks → Workbooks)
#   - PERMISSION_LEVEL label (Full Control, Read-Write, etc.)
#   - RISK_SCORE weighted integer (0–4)
#   - ACCOUNT_CATEGORY clean label (User / Application Role)
#   - EXTRACTED_AT cast to proper timestamp
#   - NULL quality flags for incomplete rows
#
# Source: arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
# Target: arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_SILVER
# ============================================================


# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
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

# ── Risk Score Weights ────────────────────────────────────────
# Used to weight RISK_SCORE — adjust as needed
WEIGHT_CHANGE_PERM  = 4   # Can change permissions on item
WEIGHT_TAKE_OWN     = 3   # Can take ownership
WEIGHT_DELETE       = 2   # Can delete
WEIGHT_WRITE        = 1   # Can write/edit

print("=" * 50)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  Source : {BRONZE_FULL}")
print(f"  Target : {SILVER_FULL}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 2: SPARK SESSION & READ BRONZE
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
# SECTION 3: UDFs — Custom Transformations
# ─────────────────────────────────────────────────────────────

def decode_base64_id(encoded_id):
    """
    Reverse the Base64URL encoding applied during extraction.
    Restores the human-readable catalog path.
    e.g. L3NoYXJlZC9TYWxlcw → /shared/Sales
    """
    if not encoded_id:
        return None
    try:
        # Add padding back before decoding
        padded = encoded_id + "=" * (4 - len(encoded_id) % 4)
        return base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        return encoded_id   # return original if decode fails


def get_permission_level(read, write, delete, change_perm, take_own):
    """
    Derive a human-readable permission level label
    from the individual permission flags.

    Priority order (highest wins):
      Full Control  → ChangePerm=1 OR TakeOwn=1
      Read-Write-Delete → Delete=1
      Read-Write    → Write=1
      Read-Only     → Read=1
      No Access     → all 0 or NULL
    """
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
    """
    Weighted risk score based on elevated privilege flags.
    Max score = 10 (all elevated permissions granted).
    Used to surface highest-risk items in OAC report.
    """
    if write is None and delete is None and change_perm is None and take_own is None:
        return 0
    score = 0
    if change_perm == 1: score += WEIGHT_CHANGE_PERM
    if take_own    == 1: score += WEIGHT_TAKE_OWN
    if delete      == 1: score += WEIGHT_DELETE
    if write       == 1: score += WEIGHT_WRITE
    return score


def get_account_category(account_type, account_guid):
    """
    Map raw AccountType/AccountGuid values to clean labels.
    ApplicationRole guids like 'AuthenticatedUser' or
    'BIServiceAdministrator' are role-based, not individual users.
    """
    if account_type == "ApplicationRole":
        return "Application Role"
    elif account_type == "User":
        return "Individual User"
    elif account_guid and "@" in account_guid:
        return "Individual User"
    else:
        return "Unknown"


# Register UDFs
udf_decode_id          = F.udf(decode_base64_id,     StringType())
udf_permission_level   = F.udf(get_permission_level,  StringType())
udf_risk_score         = F.udf(get_risk_score,        IntegerType())
udf_account_category   = F.udf(get_account_category,  StringType())

print("=" * 50)
print("  SECTION 3 COMPLETE: UDFs Registered")
print("  UDFs: decode_base64_id, permission_level,")
print("        risk_score, account_category")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 4: TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────

df_silver = (df_bronze

    # ── 1. Decode Base64 ITEM_ID to readable path ────────────
    .withColumn("ITEM_PATH_DECODED",
        udf_decode_id(F.col("ITEM_ID"))
    )

    # ── 2. Proper case CATALOG_TYPE ──────────────────────────
    #    workbooks → Workbooks, datasets → Datasets, etc.
    .withColumn("CATALOG_TYPE_LABEL",
        F.initcap(F.col("CATALOG_TYPE"))
    )

    # ── 3. PERMISSION_LEVEL label ────────────────────────────
    .withColumn("PERMISSION_LEVEL",
        udf_permission_level(
            F.col("PERM_READ"),
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # ── 4. RISK_SCORE weighted integer ───────────────────────
    .withColumn("RISK_SCORE",
        udf_risk_score(
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # ── 5. RISK_LEVEL label from score ───────────────────────
    .withColumn("RISK_LEVEL",
        F.when(F.col("RISK_SCORE") >= 7, "Critical")
         .when(F.col("RISK_SCORE") >= 4, "High")
         .when(F.col("RISK_SCORE") >= 2, "Medium")
         .when(F.col("RISK_SCORE") >= 1, "Low")
         .otherwise("None")
    )

    # ── 6. ACCOUNT_CATEGORY clean label ─────────────────────
    .withColumn("ACCOUNT_CATEGORY",
        udf_account_category(
            F.col("ACCOUNT_TYPE"),
            F.col("ACCOUNT_GUID")
        )
    )

    # ── 7. Cast EXTRACTED_AT to proper timestamp ─────────────
    .withColumn("EXTRACTED_AT_TS",
        F.to_timestamp(F.col("EXTRACTED_AT"))
    )

    # ── 8. ITEM_CREATED null flag ────────────────────────────
    #    ITEM_CREATED is unreliable from the API — flag it
    .withColumn("HAS_CREATED_DATE",
        F.when(
            F.col("ITEM_CREATED").isNull() | (F.col("ITEM_CREATED") == ""),
            F.lit(0)
        ).otherwise(F.lit(1))
    )

    # ── 9. DATA_QUALITY_FLAG ─────────────────────────────────
    #    Flag rows where ACL data is incomplete
    .withColumn("DATA_QUALITY_FLAG",
        F.when(
            F.col("ACCOUNT_GUID").isNull() | F.col("ACCOUNT_NAME").isNull(),
            "MISSING_PRINCIPAL"
        ).when(
            F.col("PERM_READ").isNull() & F.col("PERM_WRITE").isNull(),
            "MISSING_PERMISSIONS"
        ).otherwise("OK")
    )

    # ── 10. Select and order final Silver columns ────────────
    .select(
        # Original identifiers
        F.col("CATALOG_TYPE"),
        F.col("CATALOG_TYPE_LABEL"),
        F.col("ITEM_ID"),
        F.col("ITEM_PATH_DECODED").alias("ITEM_PATH"),
        F.col("ITEM_NAME"),
        F.col("ITEM_OWNER"),
        F.col("ITEM_MODIFIED"),
        F.col("HAS_CREATED_DATE"),
        # Principal
        F.col("ACCOUNT_GUID"),
        F.col("ACCOUNT_TYPE"),
        F.col("ACCOUNT_CATEGORY"),
        F.col("ACCOUNT_NAME"),
        # Raw permission flags (preserved from Bronze)
        F.col("PERM_READ"),
        F.col("PERM_WRITE"),
        F.col("PERM_LIST"),
        F.col("PERM_DELETE"),
        F.col("PERM_CHANGE_PERM"),
        F.col("PERM_TAKE_OWN"),
        # Derived enrichments
        F.col("PERMISSION_LEVEL"),
        F.col("RISK_SCORE"),
        F.col("RISK_LEVEL"),
        # Audit
        F.col("DATA_QUALITY_FLAG"),
        F.col("EXTRACTED_AT"),
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
print("  SECTION 5 COMPLETE: Quality Summary")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 6: WRITE SILVER TABLE
# ─────────────────────────────────────────────────────────────

(df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FULL))

count = spark.table(SILVER_FULL).count()

print("=" * 50)
print("  SECTION 6 COMPLETE: Silver Table Written")
print(f"  Table  : {SILVER_FULL}")
print(f"  Rows   : {count:,}")
print(f"  Columns: {len(df_silver.columns)}")
print("  Status : Queryable in AIDP + OAC")
print("=" * 50)

print("\n📊 Silver Preview (5 rows):")
spark.table(SILVER_FULL).select(
    "CATALOG_TYPE_LABEL",
    "ITEM_NAME",
    "ACCOUNT_NAME",
    "ACCOUNT_CATEGORY",
    "PERMISSION_LEVEL",
    "RISK_LEVEL",
    "RISK_SCORE"
).show(5, truncate=40)

print("\n" + "=" * 60)
print("  🏁 SILVER PIPELINE COMPLETE")
print("=" * 60)
