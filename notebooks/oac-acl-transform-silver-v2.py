# ============================================================
# OAC CATALOG ACL TRANSFORMER — SILVER LAYER
# Version: 2.0 — Standard Catalog source, two-stage write,
#          admin account detection, OCI GenAI enrichments
#
# Purpose: Read Bronze ACL data, apply 10 PySpark enrichments,
#          detect and flag admin accounts, call OCI GenAI to
#          generate risk narratives (Agent B) and infer owners
#          for orphaned catalog objects (Agent C), then write
#          to Standard Catalog Silver (Delta) and ADW.
#
# Source:  cbtest_standard_catalog.default.OAC_CATALOG_ACL_BRONZE
#
# Write Targets:
#   Stage 1: cbtest_standard_catalog.default.OAC_CATALOG_ACL_SILVER
#            Format: Delta
#   Stage 2: arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_SILVER
#            Format: Default (ADW External Catalog)
#
# Enrichments applied:
#   PySpark (existing from v1):
#     1.  ITEM_PATH_DECODED  — Base64 decode ITEM_ID to readable path
#     2.  CATALOG_TYPE_LABEL — proper case (workbooks → Workbooks)
#     3.  ITEM_MODIFIED_TS   — ISO string → Spark TimestampType
#     4.  PERMISSION_LEVEL   — 6 flags → single readable label
#     5.  RISK_SCORE         — weighted integer 0–10
#     6.  RISK_LEVEL         — score bucket label
#     7.  ACCOUNT_CATEGORY   — clean principal type label
#     8.  EXTRACTED_AT_TS    — ISO string → Spark TimestampType
#     9.  HAS_CREATED_DATE   — null quality flag (0/1)
#     10. DATA_QUALITY_FLAG  — row-level completeness indicator
#   GenAI (new in v2):
#     11. IS_ADMIN_ACCOUNT   — admin flag (name pattern + breadth)
#     12. RISK_NARRATIVE     — plain-English risk summary per
#                              user/role via OCI GenAI (Agent B).
#                              Admin accounts receive a standard
#                              override note — not a risk flag.
#     13. INFERRED_OWNER     — suggested owner for orphaned or
#                              service-account-owned objects via
#                              OCI GenAI (Agent C).
#     14. INFERRED_OWNER_NOTE — one-sentence rationale from model
#
# Bronze table is never modified — Silver is a separate
# enriched copy. Both tables coexist in their respective
# catalogs and schemas.
# ============================================================


# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────
# All configuration is defined here so developers can adjust
# source/target tables, risk weights, admin patterns, and
# GenAI settings without touching transformation logic.
#
# RISK_SCORE weight design (v1, preserved):
#   Weights reflect the relative severity of elevated privileges:
#   WEIGHT_CHANGE_PERM (4): Highest risk — this principal can
#     modify other users' access to the item, making it a
#     potential escalation path.
#   WEIGHT_TAKE_OWN (3): Principal can reassign ownership,
#     bypassing access controls entirely.
#   WEIGHT_DELETE (2): Destructive action — item can be
#     permanently removed from the catalog.
#   WEIGHT_WRITE (1): Data modification risk — item content
#     or definition can be changed.
#   Max possible RISK_SCORE = 10 (all elevated perms granted).
#   Weights can be tuned to reflect organisational risk posture
#   without changing UDF logic in Section 3.
#
# ADMIN_NAME_PATTERNS:
#   Accounts whose ACCOUNT_NAME contains any of these strings
#   (case-insensitive) are flagged as admin accounts. Flagged
#   accounts are not passed to the GenAI risk narrative agent —
#   they receive ADMIN_NARRATIVE instead. This prevents false-
#   positive risk flags on accounts that legitimately hold Full
#   Control across all catalog types as part of their role.
#   Add or remove patterns to match your environment's naming.
#
# ADMIN_FC_THRESHOLD:
#   Accounts with Full Control on this many or more distinct
#   catalog types are also flagged as admin accounts regardless
#   of name. An account with Full Control on workbooks, datasets,
#   connections, dataflows, and folders is behaving as an admin
#   whether or not its name contains "admin".
#
# SERVICE_ACCOUNT_PATTERNS:
#   ITEM_OWNER values matching these patterns are treated as
#   service or shared accounts, triggering owner inference (C).
#   A null ITEM_OWNER also triggers inference.
#
# OCI_GENAI_MODEL_ID:
#   google.gemini-2.5-pro is available in this AIDP instance's
#   Default Catalog AI Models. Update if a different model
#   is preferred. See the model list in the project reference.
# ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType
import base64
import json
import time
from datetime import datetime, timezone
from collections import defaultdict

# ── Source & Target ──────────────────────────────────────────
BRONZE_FULL   = "cbtest_standard_catalog.default.OAC_CATALOG_ACL_BRONZE"
SILVER_STAGE1 = "cbtest_standard_catalog.default.OAC_CATALOG_ACL_SILVER"
SILVER_STAGE2 = "arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL_SILVER"

# ── Risk Score Weights (tune to reflect org risk posture) ────
WEIGHT_CHANGE_PERM  = 4   # Can change permissions on item
WEIGHT_TAKE_OWN     = 3   # Can take ownership of item
WEIGHT_DELETE       = 2   # Can delete item
WEIGHT_WRITE        = 1   # Can write/edit item

# ── Admin Account Detection ───────────────────────────────────
# Case-insensitive substring match against ACCOUNT_NAME.
# Matching accounts receive ADMIN_NARRATIVE and skip GenAI.
ADMIN_NAME_PATTERNS = [
    "admin",
    "administrator",
    "bi_administrator",
    "biserviceadministrator",
    "sysadmin",
    "oac-admin",
    "catalog-admin",
]

# Accounts with Full Control on this many distinct catalog
# types are also flagged as admins regardless of name.
ADMIN_FC_THRESHOLD = 3

# Standard narrative for confirmed admin accounts.
# Applied directly — no GenAI call is made for these accounts.
ADMIN_NARRATIVE = (
    "This is an administrative or privileged account. "
    "Elevated access across catalog types is expected and by design. "
    "No remediation required. Recommend periodic verification that "
    "this account is actively managed and assigned to the correct owner."
)

# ── Owner Inference Triggers ──────────────────────────────────
# ITEM_OWNER values matching these substrings (case-insensitive)
# are treated as service/shared accounts and trigger Agent C.
SERVICE_ACCOUNT_PATTERNS = [
    "svc-",
    "service-",
    "shared-",
    "system-",
    "oac-svc",
    "oacservice",
]

# ── OCI GenAI Configuration ───────────────────────────────────
# google.gemini-2.5-pro is available in this AIDP instance.
# Update OCI_GENAI_MODEL_ID to switch models — see the AIDP
# Workbench → Default Catalog → OCI AI Models for available list.
# COMPARTMENT_OCID must match the compartment where GenAI is
# provisioned in your OCI tenancy.
OCI_GENAI_MODEL_ID       = "google.gemini-2.5-pro"
OCI_GENAI_ENDPOINT       = "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com"
OCI_GENAI_COMPARTMENT_ID = "REPLACE_WITH_COMPARTMENT_OCID"
GENAI_MAX_TOKENS         = 350
GENAI_TEMPERATURE        = 0.1   # Low temp for factual, consistent summaries
GENAI_RATE_LIMIT_SLEEP   = 0.5   # Seconds between GenAI calls to avoid throttling

print("=" * 55)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  Source  : {BRONZE_FULL}")
print(f"  Stage 1 : {SILVER_STAGE1}")
print(f"  Stage 2 : {SILVER_STAGE2}")
print(f"  Model   : {OCI_GENAI_MODEL_ID}")
print(f"  Risk weights: ChangePerm={WEIGHT_CHANGE_PERM}, "
      f"TakeOwn={WEIGHT_TAKE_OWN}, "
      f"Delete={WEIGHT_DELETE}, "
      f"Write={WEIGHT_WRITE}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 2: SPARK SESSION & READ BRONZE
# ─────────────────────────────────────────────────────────────
# Reads from the Standard Catalog Bronze Delta table written by
# the Bronze notebook. No wallet, no JDBC string — Spark
# resolves the 3-part catalog path via the Standard Catalog
# metadata layer.
#
# The Bronze table is the raw, unmodified API extract and is
# the source of truth for this pipeline. It is never written
# to from this notebook. All enrichment and derived columns are
# written to the Silver table only.
#
# In v2 the source is the Standard Catalog Delta table rather
# than the ADW External Catalog. This decouples the Silver
# read from the ADW connection and means Silver can run in
# any AIDP instance that has the Bronze Delta table — no ADW
# connection is required for the read path.
# ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.appName("oac_acl_silver").getOrCreate()

df_bronze = spark.table(BRONZE_FULL)

total_bronze = df_bronze.count()
print("=" * 55)
print("  SECTION 2 COMPLETE: Bronze Table Loaded")
print(f"  Source      : {BRONZE_FULL}")
print(f"  Rows read   : {total_bronze:,}")
print(f"  Columns     : {len(df_bronze.columns)}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 3: UDFs — CUSTOM TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────
# Four Python functions are defined and registered as Spark
# UDFs. UDFs allow row-level Python logic to run inside a
# distributed Spark DataFrame operation without materialising
# data to the driver.
#
# decode_base64_id:
#   The OAC REST API returns catalog object IDs as Base64
#   URL-safe encoded strings — this encoding is required by
#   the getACL endpoint. The Silver layer reverses it to
#   restore the human-readable catalog path, e.g.:
#     L3NoYXJlZC9TYWxlcw → /shared/Sales
#   Padding = characters are added back before decoding since
#   the API strips them during extraction. If decoding fails
#   for any reason, the original value is returned unchanged
#   so no data is silently lost.
#
# get_permission_level:
#   Maps the 6 raw boolean permission flags from Bronze into a
#   single human-readable label for reporting. Priority order
#   ensures the highest privilege wins when multiple flags are
#   set simultaneously. Labels in descending priority:
#     Full Control     — change_perm or take_own is 1
#     Read-Write-Delete — delete is 1
#     Read-Write       — write is 1
#     Read-Only        — read is 1 only
#     No Access        — all flags are 0 or null
#
# get_risk_score:
#   Computes a weighted integer risk score (0–10) based on
#   elevated privilege flags. The WEIGHT_* constants from
#   Section 1 are captured at UDF definition time — changing
#   the weights in Section 1 requires re-running this section
#   to pick up the new values.
#
# get_account_category:
#   The OAC API returns ACCOUNT_TYPE as 'ApplicationRole' or
#   'User'. Application roles with names like 'AuthenticatedUser'
#   or 'BIServiceAdministrator' are not individual people.
#   This UDF maps both fields into 'Application Role' or
#   'Individual User' for clean OAC report filtering.
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

print("=" * 55)
print("  SECTION 3 COMPLETE: UDFs Registered")
print("  UDFs: decode_base64_id, get_permission_level,")
print("        get_risk_score, get_account_category")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 4: PYSPARK TRANSFORMATIONS (10 enrichments)
# ─────────────────────────────────────────────────────────────
# All 10 enrichments are applied in sequence using a chained
# withColumn() pattern. The final .select() controls column
# order in the Silver table — original Bronze columns first,
# derived enrichments grouped at the end for clear lineage.
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
#                          (ITEM_CREATED is blank on this OAC
#                          instance but is a valid API field on
#                          others — keep the flag active)
# 10. DATA_QUALITY_FLAG  — row-level completeness indicator
#
# The ITEM_CREATED hooks below (A–D) are commented out because
# ITEM_CREATED is blank on this OAC instance. Activate them
# when the field is populated on the target instance.
# ─────────────────────────────────────────────────────────────

df_silver = (
    df_bronze

    # 1. Decode Base64-encoded ITEM_ID to readable catalog path
    .withColumn("ITEM_PATH_DECODED",
        udf_decode_id(F.col("ITEM_ID"))
    )

    # 2. CATALOG_TYPE_LABEL — proper case for reporting
    .withColumn("CATALOG_TYPE_LABEL",
        F.initcap(F.col("CATALOG_TYPE"))
    )

    # 3. Cast ITEM_MODIFIED to proper timestamp
    .withColumn("ITEM_MODIFIED_TS",
        F.to_timestamp(F.col("ITEM_MODIFIED"))
    )

    # 4. PERMISSION_LEVEL — map 6 boolean flags to readable label
    .withColumn("PERMISSION_LEVEL",
        udf_permission_level(
            F.col("PERM_READ"),
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # 5. RISK_SCORE — weighted integer 0–10
    #    Weights are defined in Section 1 and captured at UDF
    #    registration time. Re-run Section 3 if weights change.
    .withColumn("RISK_SCORE",
        udf_risk_score(
            F.col("PERM_WRITE"),
            F.col("PERM_DELETE"),
            F.col("PERM_CHANGE_PERM"),
            F.col("PERM_TAKE_OWN")
        )
    )

    # 6. RISK_LEVEL — bucket RISK_SCORE into named tiers
    #    Thresholds: Critical 8–10, High 5–7, Medium 2–4, Low 0–1
    #    Adjust thresholds here if risk posture changes.
    .withColumn("RISK_LEVEL",
        F.when(F.col("RISK_SCORE") >= 8, "Critical")
         .when(F.col("RISK_SCORE") >= 5, "High")
         .when(F.col("RISK_SCORE") >= 2, "Medium")
         .when(F.col("RISK_SCORE") == 0, "None")
         .otherwise("Low")
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
    #    ITEM_CREATED is blank on this OAC instance but is a
    #    valid API field on others. The flag is kept active so
    #    the column exists in Silver regardless of instance.
    #    See commented hooks below for timestamp casting and
    #    age-based enrichments when the field is populated.
    .withColumn("HAS_CREATED_DATE",
        F.when(
            F.col("ITEM_CREATED").isNull() | (F.col("ITEM_CREATED") == ""),
            F.lit(0)
        ).otherwise(F.lit(1))
    )

    # ── ITEM_CREATED HOOKS (activate when field is populated) ──
    #
    # Hook A: Cast ITEM_CREATED string to proper timestamp
    # Uncomment when ITEM_CREATED is reliably populated:
    #
    # .withColumn("ITEM_CREATED_TS",
    #     F.to_timestamp(F.col("ITEM_CREATED"))
    # )
    #
    # Hook B: Item age in days from creation to last modified
    # Useful for identifying stale or orphaned catalog items.
    # Requires Hook A to be active first:
    #
    # .withColumn("ITEM_AGE_DAYS",
    #     F.datediff(
    #         F.col("ITEM_MODIFIED_TS"),
    #         F.col("ITEM_CREATED_TS")
    #     )
    # )
    #
    # Hook C: Age bucket label for report filtering
    # Requires Hook B to be active first:
    #
    # .withColumn("ITEM_AGE_BUCKET",
    #     F.when(F.col("ITEM_AGE_DAYS") <= 30,  "< 30 days")
    #      .when(F.col("ITEM_AGE_DAYS") <= 90,  "30–90 days")
    #      .when(F.col("ITEM_AGE_DAYS") <= 365, "90–365 days")
    #      .otherwise("> 1 year")
    # )
    #
    # Hook D: Add to final .select() when hooks A–C are active:
    # F.col("ITEM_CREATED"),        # original string preserved
    # F.col("ITEM_CREATED_TS"),     # proper timestamp
    # F.col("ITEM_AGE_DAYS"),       # integer age
    # F.col("ITEM_AGE_BUCKET"),     # label for OAC filtering
    # ──────────────────────────────────────────────────────────

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
        F.col("ITEM_CREATED"),
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
        # Silver enrichments (v1)
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

print("=" * 55)
print("  SECTION 4 COMPLETE: PySpark Transformations Applied")
print(f"  Total rows   : {total_silver:,}")
print(f"  Quality OK   : {ok_rows:,}")
print(f"  Flagged rows : {flagged_rows:,}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 5: QUALITY SUMMARY
# ─────────────────────────────────────────────────────────────
# Three distribution tables are printed BEFORE writing to
# Silver. This gives the developer a chance to validate that
# the enrichment results look correct before committing data.
#
# If distributions look wrong (e.g. all rows showing "No Access"
# or unexpected RISK_LEVEL values), stop here, investigate the
# Bronze data, and re-run Section 4 after fixing the issue.
#
# Expected distributions based on current Bronze data:
#   PERMISSION_LEVEL: majority Read-Only or Full Control
#   RISK_LEVEL: mix of High and None (two ApplicationRoles)
#   ACCOUNT_CATEGORY: Application Role dominant (~100%)
#     — expected: the API returns role-based ACLs primarily.
#     Individual users appear after Silver-level enrichment.
#
# Note: df.show() is retained here intentionally because this
# is a pre-write validation gate — not a production data print.
# It runs on the aggregated distribution (small row count),
# not on the full Silver DataFrame. This is safe at any scale.
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

print("=" * 55)
print("  SECTION 5 COMPLETE: Quality Summary Printed")
print("  Review distributions above before proceeding.")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 6: ADMIN ACCOUNT DETECTION
# ─────────────────────────────────────────────────────────────
# Identifies admin accounts before the GenAI enrichment step
# so they can be excluded from risk narrative generation and
# instead receive a standard administrative account note.
#
# Why is this step necessary?
#   Admin accounts legitimately hold Full Control across most
#   or all catalog types as part of their operational role.
#   If these accounts are passed to the GenAI risk narrative
#   agent without flagging, the model will generate high-risk
#   alerts for expected behaviour — producing false positives
#   that reduce trust in the Risk Dashboard for actual admins
#   using the report.
#
# Two detection mechanisms (account is flagged if EITHER fires):
#
# Name-based (ADMIN_NAME_PATTERNS):
#   Case-insensitive substring match against ACCOUNT_NAME.
#   Catches naming conventions like 'bi_administrator',
#   'oac-admin', 'sysadmin'. Configurable in Section 1.
#
# Breadth-based (ADMIN_FC_THRESHOLD):
#   Any account with Full Control on ADMIN_FC_THRESHOLD or more
#   distinct catalog types is flagged regardless of name. This
#   catches admin service accounts, break-glass accounts, or
#   accounts whose names do not contain conventional patterns.
#   Default threshold is 3 catalog types — adjust in Section 1.
#
# Output column: IS_ADMIN_ACCOUNT (boolean, added to df_silver)
#   True  — account receives ADMIN_NARRATIVE, skips GenAI
#   False — account is processed by Agent B GenAI (Section 7)
# ─────────────────────────────────────────────────────────────

# Build name-based condition — OR across all configured patterns
admin_name_condition = F.lit(False)
for pattern in ADMIN_NAME_PATTERNS:
    admin_name_condition = admin_name_condition | F.lower(F.col("ACCOUNT_NAME")).contains(pattern.lower())

# Build breadth-based condition — Full Control on N+ catalog types
admin_breadth_df = (
    df_silver
    .filter(F.col("PERMISSION_LEVEL") == "Full Control")
    .groupBy("ACCOUNT_NAME")
    .agg(F.countDistinct("CATALOG_TYPE").alias("FC_TYPE_COUNT"))
    .filter(F.col("FC_TYPE_COUNT") >= ADMIN_FC_THRESHOLD)
    .select("ACCOUNT_NAME", F.lit(True).alias("IS_BREADTH_ADMIN"))
)

# Join breadth flag, combine with name flag, drop interim column
df_silver = df_silver.join(admin_breadth_df, on="ACCOUNT_NAME", how="left")
df_silver = df_silver.withColumn(
    "IS_ADMIN_ACCOUNT",
    admin_name_condition | (F.col("IS_BREADTH_ADMIN") == True)
).drop("IS_BREADTH_ADMIN")

admin_acct_count = df_silver.filter(F.col("IS_ADMIN_ACCOUNT") == True) \
                            .select("ACCOUNT_NAME").distinct().count()
total_acct_count = df_silver.select("ACCOUNT_NAME").distinct().count()

print("=" * 55)
print("  SECTION 6 COMPLETE: Admin Account Detection")
print(f"  Total distinct accounts  : {total_acct_count:,}")
print(f"  Admin accounts flagged   : {admin_acct_count:,}")
print(f"  Non-admin (GenAI input)  : {total_acct_count - admin_acct_count:,}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 7: OCI GENAI CLIENT
# ─────────────────────────────────────────────────────────────
# Initialises the OCI Generative AI Inference client using
# Resource Principal authentication. Resource Principal is the
# correct auth method for notebooks running inside the AIDP
# Workbench — no API key or config file is needed.
#
# Why Resource Principal instead of API Key auth?
#   AIDP notebooks run in OCI compute instances that are
#   provisioned with a Resource Principal identity. This
#   identity has been granted the necessary OCI IAM policies
#   to call OCI GenAI. Using Resource Principal means no
#   credentials are stored in the notebook or workspace.
#
# GenAI model: google.gemini-2.5-pro
#   Available in this AIDP instance's Default Catalog AI Models.
#   The model is called via the OCI GenAI Inference API using
#   a GenericChatRequest (required for non-Cohere models such
#   as Gemini, Grok, and Llama). Cohere models use
#   CohereChatRequest instead — swap if switching to Cohere.
#
# call_genai():
#   Wraps the OCI GenAI SDK chat call with error handling.
#   Returns the generated text string on success, or a
#   [GENAI_ERROR] prefixed message on failure. Callers check
#   for this prefix to detect failed generations without
#   raising exceptions in the enrichment loop.
#
# OCI GenAI API alternative — AIDP SQL approach:
#   If you prefer to call GenAI via Spark SQL using AIDP's
#   built-in model function syntax, the pattern is:
#     spark.sql("""
#         SELECT account_name,
#                ai_generate(prompt_col) AS risk_narrative
#         FROM   prompt_temp_view
#     """)
#   Replace 'ai_generate' with the registered function name
#   visible under Default Catalog → OCI AI Models in AIDP.
#   The Python SDK approach is used here for explicit control
#   over prompt construction, error handling, and retry logic.
# ─────────────────────────────────────────────────────────────

import oci
from oci.generative_ai_inference.models import (
    ChatDetails,
    OnDemandServingMode,
    GenericChatRequest,
    UserMessage,
    TextContent
)


def get_genai_client():
    # Resource Principal — correct auth for AIDP notebooks.
    # No config file or API key required.
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.generative_ai_inference.GenerativeAiInferenceClient(
        config={},
        signer=signer,
        service_endpoint=OCI_GENAI_ENDPOINT,
    )
    return client


def call_genai(client, prompt, max_tokens=GENAI_MAX_TOKENS, temperature=GENAI_TEMPERATURE):
    """
    Call OCI GenAI chat endpoint with a text prompt.
    Uses GenericChatRequest — required for Gemini, Grok, and
    Llama models. Switch to CohereChatRequest for Cohere models.
    Returns the generated text string, or a [GENAI_ERROR] message.
    """
    try:
        # Build the user message
        text_content = TextContent()
        text_content.text = prompt

        message = UserMessage()
        message.content = [text_content]

        # Build the chat request
        chat_request = GenericChatRequest()
        chat_request.messages    = [message]
        chat_request.max_tokens  = max_tokens
        chat_request.temperature = temperature

        # Build the chat details envelope
        chat_detail = ChatDetails()
        chat_detail.serving_mode = OnDemandServingMode(model_id=OCI_GENAI_MODEL_ID)
        chat_detail.chat_request = chat_request
        chat_detail.compartment_id = OCI_GENAI_COMPARTMENT_ID

        response = client.chat(chat_detail)
        # Extract text from the first choice
        return response.data.chat_response.choices[0].message.content[0].text.strip()

    except Exception as e:
        return f"[GENAI_ERROR] {str(e)[:200]}"


print("=" * 55)
print("  SECTION 7 COMPLETE: OCI GenAI Client Ready")
print(f"  Model  : {OCI_GENAI_MODEL_ID}")
print(f"  Auth   : Resource Principal")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 8: AGENT B — RISK NARRATIVE PER ACCOUNT
# ─────────────────────────────────────────────────────────────
# Generates a plain-English risk summary for each distinct
# ACCOUNT_NAME by calling OCI GenAI with a structured prompt
# built from that account's permission profile across the catalog.
#
# Why driver-side collection rather than a Spark UDF?
#   ACL data is small (typically <500 distinct accounts).
#   Collecting to the driver and calling GenAI sequentially is
#   simpler, easier to debug, and avoids the complexity of
#   Spark executor → external API call patterns which require
#   careful handling of connection pools and rate limits across
#   distributed nodes.
#
# Admin accounts:
#   Accounts flagged IS_ADMIN_ACCOUNT = True in Section 6 are
#   excluded from GenAI calls and assigned ADMIN_NARRATIVE
#   directly. This prevents false-positive risk alerts for
#   accounts that legitimately hold elevated access by design.
#
# Prompt design:
#   The prompt is factual and structured, providing the model
#   with the account name, category (User/Application Role),
#   and a breakdown of permission levels, catalog types, and
#   risk tiers for each object the account has access to.
#   Temperature is set to 0.1 for consistent, factual output
#   rather than creative or varied language.
#
# Output column: RISK_NARRATIVE (StringType)
#   - Admin accounts: ADMIN_NARRATIVE constant
#   - Non-admin accounts: GenAI-generated 2–3 sentence summary
#   - Accounts with no profile data: "No profile available"
#   - Failed GenAI calls: [GENAI_ERROR] prefixed message
#
# The narrative is joined back to df_silver on ACCOUNT_NAME
# so every row for a given account receives the same narrative.
# ─────────────────────────────────────────────────────────────

def build_risk_narrative_prompt(account_name, account_category, profile):
    """
    Builds a structured prompt for GenAI risk narrative generation.
    profile is a list of dicts with keys: catalog_type_label,
    permission_level, risk_level, item_count.
    """
    profile_lines = "\n".join([
        f"  - {p['catalog_type_label']}: {p['permission_level']} "
        f"({p['item_count']} object(s), risk: {p['risk_level']})"
        for p in profile
    ])
    return f"""You are an Oracle Analytics Cloud security analyst reviewing access control lists.

Write a 2–3 sentence plain-English risk assessment for the {account_category.lower()} account '{account_name}'. Be specific and factual based only on the permission profile below. Do not use bullet points. Do not recommend generic best practices unless there is a specific reason based on this profile.

Permission profile:
{profile_lines}

Risk assessment:"""


print("\n[AGENT B] Building risk narratives via OCI GenAI...")

genai_client = get_genai_client()

# Separate admin and non-admin accounts
admin_accounts = set(
    row.ACCOUNT_NAME
    for row in df_silver.filter(F.col("IS_ADMIN_ACCOUNT") == True)
                        .select("ACCOUNT_NAME").distinct().collect()
    if row.ACCOUNT_NAME
)

# Build permission profiles for non-admin accounts
profile_rows = (
    df_silver
    .filter(F.col("IS_ADMIN_ACCOUNT") == False)
    .filter(F.col("ACCOUNT_NAME").isNotNull())
    .groupBy("ACCOUNT_NAME", "ACCOUNT_CATEGORY", "CATALOG_TYPE_LABEL",
             "PERMISSION_LEVEL", "RISK_LEVEL")
    .agg(F.count("*").alias("item_count"))
    .collect()
)

# Group profiles by account name
account_profiles = defaultdict(lambda: {"category": "Unknown", "permissions": []})
for row in profile_rows:
    account_profiles[row.ACCOUNT_NAME]["category"] = row.ACCOUNT_CATEGORY
    account_profiles[row.ACCOUNT_NAME]["permissions"].append({
        "catalog_type_label": row.CATALOG_TYPE_LABEL,
        "permission_level":   row.PERMISSION_LEVEL,
        "risk_level":         row.RISK_LEVEL,
        "item_count":         row.item_count,
    })

# Generate narratives
narrative_lookup = {}

# Admin accounts — standard override, no GenAI call
for acct in admin_accounts:
    narrative_lookup[acct] = ADMIN_NARRATIVE

print(f"  Admin accounts  : {len(admin_accounts)} — standard narrative applied")
print(f"  Non-admin       : {len(account_profiles)} — calling OCI GenAI...")

for i, (account_name, profile) in enumerate(account_profiles.items()):
    prompt    = build_risk_narrative_prompt(
        account_name     = account_name,
        account_category = profile["category"],
        profile          = profile["permissions"],
    )
    narrative = call_genai(genai_client, prompt)
    narrative_lookup[account_name] = narrative

    if (i + 1) % 10 == 0 or (i + 1) == len(account_profiles):
        print(f"  Progress: {i + 1}/{len(account_profiles)} narratives generated")

    time.sleep(GENAI_RATE_LIMIT_SLEEP)

# Build lookup DataFrame and join to df_silver
narrative_df = spark.createDataFrame(
    [(name, text) for name, text in narrative_lookup.items()],
    schema=["ACCOUNT_NAME", "RISK_NARRATIVE"]
)

df_silver = df_silver.join(narrative_df, on="ACCOUNT_NAME", how="left")
df_silver = df_silver.fillna({"RISK_NARRATIVE": "No profile available"})

print("=" * 55)
print("  SECTION 8 COMPLETE: Agent B — Risk Narratives Done")
print(f"  Admin narratives (override) : {len(admin_accounts)}")
print(f"  GenAI narratives generated  : {len(account_profiles)}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 9: AGENT C — OWNER INFERENCE FOR ORPHANED OBJECTS
# ─────────────────────────────────────────────────────────────
# For catalog objects where ITEM_OWNER is null, empty, or
# matches a known service/shared account pattern, calls OCI
# GenAI to suggest the most likely responsible owner.
#
# Why is owner inference useful?
#   Catalog objects without a clear human owner are governance
#   blind spots. If no one is accountable for a workbook or
#   dataset, access permissions may never be reviewed, objects
#   accumulate stale permissions, and sensitive data may be
#   more widely accessible than intended. Owner inference gives
#   admins a starting point for accountability conversations.
#
# Trigger conditions (either fires inference):
#   1. ITEM_OWNER is null or empty string
#   2. ITEM_OWNER matches any SERVICE_ACCOUNT_PATTERNS (Section 1)
#      These are service or shared accounts that may have been
#      set as owner during automated provisioning but do not
#      represent a real accountable human owner.
#
# Prompt context:
#   The model receives: object name, catalog type, decoded path,
#   and up to 10 account names with access to the object.
#   The account list provides context about who is actually
#   using the object, which often hints at likely ownership
#   (e.g., a dataset accessed only by one team → likely owned
#   by someone from that team based on their account names).
#
# Output columns:
#   INFERRED_OWNER (StringType):
#     "Unable to infer" — model could not determine an owner
#     "N/A — Owner on record" — object has a valid human owner
#     Otherwise — the model's suggested owner or account name
#   INFERRED_OWNER_NOTE (StringType):
#     One-sentence rationale from the model. Blank for objects
#     with owners on record.
# ─────────────────────────────────────────────────────────────

def build_owner_inference_prompt(item_name, item_path, catalog_type_label, subjects):
    subjects_str = ", ".join(subjects[:10]) if subjects else "none on record"
    return f"""You are an Oracle Analytics Cloud governance analyst.

The following catalog object has no identifiable human owner. Based only on the information provided, suggest the most likely responsible owner in one sentence and explain your reasoning. If you cannot infer an owner, say "Unable to infer" and briefly explain why.

Object name:           {item_name}
Object type:           {catalog_type_label}
Object path:           {item_path}
Accounts with access:  {subjects_str}

Suggested owner and rationale (one sentence):"""


print("\n[AGENT C] Identifying orphaned objects for owner inference...")

# Detect orphaned objects — null, empty, or service account owned
def is_service_owner(name):
    if not name:
        return True
    n = name.lower()
    return any(p in n for p in SERVICE_ACCOUNT_PATTERNS)

is_service_owner_udf = F.udf(is_service_owner, StringType())

orphan_df = (
    df_silver
    .filter(
        F.col("ITEM_OWNER").isNull()
        | (F.col("ITEM_OWNER") == "")
        | F.col("ITEM_OWNER").rlike("(?i)(" + "|".join(SERVICE_ACCOUNT_PATTERNS) + ")")
    )
    .select("ITEM_ID", "ITEM_NAME", "ITEM_PATH", "CATALOG_TYPE_LABEL",
            "ACCOUNT_NAME", "ITEM_OWNER")
    .distinct()
)

orphan_item_count = orphan_df.select("ITEM_ID").distinct().count()
print(f"  Orphaned/service-owned objects: {orphan_item_count}")

# Collect subjects per object for prompt context
orphan_rows = orphan_df.collect()
object_subjects = defaultdict(list)
object_meta     = {}

for row in orphan_rows:
    iid = row.ITEM_ID
    if row.ACCOUNT_NAME:
        object_subjects[iid].append(row.ACCOUNT_NAME)
    object_meta[iid] = {
        "item_name":          row.ITEM_NAME  or "",
        "item_path":          row.ITEM_PATH  or "",
        "catalog_type_label": row.CATALOG_TYPE_LABEL or "",
    }

# Generate inferences
inference_lookup = {}

print(f"  Calling OCI GenAI for {len(object_meta)} object(s)...")

for i, (item_id, meta) in enumerate(object_meta.items()):
    prompt = build_owner_inference_prompt(
        item_name          = meta["item_name"],
        item_path          = meta["item_path"],
        catalog_type_label = meta["catalog_type_label"],
        subjects           = object_subjects.get(item_id, []),
    )
    response = call_genai(genai_client, prompt, max_tokens=150)

    # If model indicates inability to infer, capture cleanly
    if response.lower().startswith("unable to infer"):
        inference_lookup[item_id] = ("Unable to infer", response)
    elif response.startswith("[GENAI_ERROR]"):
        inference_lookup[item_id] = ("Error", response)
    else:
        inference_lookup[item_id] = ("See note", response)

    if (i + 1) % 10 == 0 or (i + 1) == len(object_meta):
        print(f"  Progress: {i + 1}/{len(object_meta)} inferences generated")

    time.sleep(GENAI_RATE_LIMIT_SLEEP)

# Build lookup DataFrame and join
inference_df = spark.createDataFrame(
    [(iid, owner, note) for iid, (owner, note) in inference_lookup.items()],
    schema=["ITEM_ID", "INFERRED_OWNER", "INFERRED_OWNER_NOTE"]
)

df_silver = df_silver.join(inference_df, on="ITEM_ID", how="left")
df_silver = df_silver.fillna({
    "INFERRED_OWNER":      "N/A — Owner on record",
    "INFERRED_OWNER_NOTE": "",
})

print("=" * 55)
print("  SECTION 9 COMPLETE: Agent C — Owner Inference Done")
print(f"  Objects with inferred owners: {len(inference_lookup)}")
print(f"  Total Silver columns        : {len(df_silver.columns)}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 10: WRITE — Stage 1 (Standard Catalog Silver / Delta)
# ─────────────────────────────────────────────────────────────
# Writes the enriched Silver DataFrame to the AIDP Standard
# Catalog as a managed Delta table backed by OCI Object Storage.
#
# This mirrors the Bronze Stage 1 write pattern and carries
# the same benefits: Delta format, schema evolution support,
# ACID transactions, and portability across AIDP instances.
#
# The Silver Delta table is the preferred downstream source for
# the Gold notebook and for any OAC dataset connection that
# requires the full enriched column set (PERMISSION_LEVEL,
# RISK_LEVEL, RISK_NARRATIVE, IS_ADMIN_ACCOUNT, etc.).
# ─────────────────────────────────────────────────────────────

print(f"\n[WRITE] Stage 1 — Standard Catalog Silver (Delta)")
print(f"        Target: {SILVER_STAGE1}")

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_STAGE1)
)

df_check_s1 = spark.table(SILVER_STAGE1)
print("=" * 55)
print("  SECTION 10 COMPLETE: Stage 1 Write Done")
print(f"  Table   : {SILVER_STAGE1}")
print(f"  Rows    : {df_check_s1.count():,}")
print(f"  Columns : {len(df_check_s1.columns)}")
print(f"  Format  : Delta")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 11: WRITE — Stage 2 (ADW via External Catalog)
# ─────────────────────────────────────────────────────────────
# Reads from the Stage 1 Silver Delta table and writes to ADW
# via the AIDP External Catalog connection. Same pattern and
# constraints as the Bronze Stage 2 write:
#   - Read from Delta first (validates Stage 1 before ADW write)
#   - No Delta format (ADW External Catalog does not support it)
#   - No CREATE SCHEMA (502 from AIDP Metastore)
#   - 3-part catalog path required
#   - Full overwrite — Silver is always fully regenerated from
#     Bronze so there is no partial state to preserve
#
# The ADW Silver table powers the existing OAC report workbook.
# Until OAC canvases are repointed to the Standard Catalog
# Silver connection, Stage 2 is required for report continuity.
# ─────────────────────────────────────────────────────────────

print(f"\n[WRITE] Stage 2 — ADW via External Catalog Silver")
print(f"        Source: {SILVER_STAGE1}  (Delta)")
print(f"        Target: {SILVER_STAGE2}")

df_from_delta = spark.table(SILVER_STAGE1)

(df_from_delta.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_STAGE2)
)

df_check_s2 = spark.table(SILVER_STAGE2)
print("=" * 55)
print("  SECTION 11 COMPLETE: Stage 2 Write Done")
print(f"  Table   : {SILVER_STAGE2}")
print(f"  Rows    : {df_check_s2.count():,}")
print(f"  Columns : {len(df_check_s2.columns)}")
print(f"  Status  : Queryable in AIDP Master Catalog + OAC")
print("=" * 55)

print("\n" + "=" * 60)
print("  🏁 SILVER PIPELINE COMPLETE")
print("=" * 60)
