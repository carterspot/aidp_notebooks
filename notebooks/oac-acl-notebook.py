# ============================================================
# OAC CATALOG ACL EXTRACTOR
# Purpose: Pull ACLs for all catalog item types from Oracle
#          Analytics Cloud via REST API and load into ADW.
# Target:  ADW table OAC_CATALOG_ACL (created if not exists)
# Auth:    OAuth 2.0 Resource Owner Grant via IDCS/IAM Domain
#          Confidential Application
#
# PRE-REQUISITES (one-time setup):
#   1. In OCI Console → Identity & Security → Domains
#      → Select your domain → Integrated Applications
#      → Add Application → Confidential Application
#      → Allowed Grant Types: check "Resource Owner"
#      → Add OAC instance to scope (Resources tab)
#      → Note the Client ID and Client Secret
#   2. Calling user must have OAC "BI Service Administrator"
#      application role to see all catalog objects
#   3. cx_Oracle wallet must be configured on the cluster
#      (AIDP Compute → Cluster → Libraries)
# ============================================================

import requests
import pandas as pd
import base64
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# SECTION 1: CONFIGURATION
# ─────────────────────────────────────────────────────────────

# -- OAC Connection ------------------------------------------
OAC_BASE_URL    = "https://argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com"
OAC_API_VERSION = "20210901"

# -- IDCS OAuth (Resource Owner Password Grant) --------------
# CLIENT_ID is the OAC instance's own built-in IDCS app ID.
# CLIENT_SECRET: OCI Console → Identity & Security → Domains
#   → Default → Integrated Applications
#   → Search: "ANALYTICSINST_argano4oracleanalytics-idsmdul6idrs-ia"
#   → Configuration tab → Client Secret → Show / Generate
IDCS_DOMAIN_URL = "https://idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com"
CLIENT_ID       = "gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID"
CLIENT_SECRET   = "<get-from-idcs-app-configuration>"    # ← only missing piece
OAC_USERNAME    = "carter.beaton@argano.com"             # native IDCS user confirmed
OAC_PASSWORD    = "<your-oac-password>"
OAC_SCOPE       = "urn:opc:resource:consumer::all"       # confirmed from JWT

# -- Target Table (AIDP External Catalog → ADW) --------------
# Uses the catalog_manager External Catalog already registered
# in the AIDP Master Catalog. No wallet or cx_Oracle needed.
AIDP_CATALOG    = "catalog_manager"
AIDP_SCHEMA     = "catalog_manager"
AIDP_TABLE      = "OAC_CATALOG_ACL"
FULL_TABLE_NAME = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{AIDP_TABLE}"

# -- Catalog Types to Extract --------------------------------
CATALOG_TYPES = [
    "workbooks",
    "folders",
    "datasets",
    "dataflows",
    "connections"
]

# -- Pagination & Rate Limiting ------------------------------
PAGE_SIZE       = 100    # Max items per API page
RATE_LIMIT_WAIT = 0.2    # Seconds between API calls (be kind to OAC)


# ─────────────────────────────────────────────────────────────
# SECTION 2: AUTHENTICATION — Get OAuth Bearer Token
# ─────────────────────────────────────────────────────────────

def get_oauth_token():
    """
    Obtain OAuth 2.0 Bearer token from IDCS/IAM using
    Resource Owner Password Credentials grant.
    Token is typically valid for 3600 seconds.
    """
    token_url = f"{IDCS_DOMAIN_URL}/oauth2/v1/token"
    
    # Scope must match the OAC instance scope registered
    # in the Confidential Application. Format:
    # https://<oac-hostname>/api/20210901/catalog:read
    # OR use the full instance scope from your app config.
    # Using the BI platform scope for full catalog access:
    payload = {
        "grant_type":    "password",
        "username":      OAC_USERNAME,
        "password":      OAC_PASSWORD,
        "scope":         OAC_SCOPE
    }

    resp = requests.post(
        token_url,
        data=payload,
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )
    resp.raise_for_status()
    token_data = resp.json()
    print(f"✅ OAuth token obtained. Expires in: {token_data.get('expires_in', '?')}s")
    return token_data["access_token"]


# ─────────────────────────────────────────────────────────────
# SECTION 3: OAC API HELPERS
# ─────────────────────────────────────────────────────────────

def get_headers(token=None):
    """Use token_mgr by default; accepts override for testing."""
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return token_mgr.get_headers()


def b64_encode_id(object_path):
    """
    Base64URL-safe encode the catalog object path/ID.
    OAC requires this encoding for getACL and detail endpoints.
    Example: /shared/Sales/MyWorkbook → base64url string
    """
    return base64.urlsafe_b64encode(
        object_path.encode("utf-8")
    ).decode("utf-8").rstrip("=")


def get_catalog_items(catalog_type):
    """
    Paginate through all catalog items of a given type.
    Returns list of dicts with id, name, path, owner, created, modified.
    Token is refreshed automatically via token_mgr on each page.
    """
    items = []
    page  = 1
    base  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}"

    while True:
        params = {"search": "*", "limit": PAGE_SIZE, "page": page}
        resp = requests.get(
            base,
            headers=get_headers(),   # token_mgr auto-refreshes here
            params=params,
            timeout=30
        )

        if resp.status_code == 404:
            print(f"  ⚠️  {catalog_type}: no items found (404)")
            break
        resp.raise_for_status()

        page_items = resp.json()
        if not page_items:
            break

        items.extend(page_items)
        total_pages = int(resp.headers.get("oa-page-count", 1))
        print(f"  📄 {catalog_type}: page {page}/{total_pages} "
              f"— {len(page_items)} items "
              f"[token: {token_mgr.seconds_remaining}s remaining]")

        if page >= total_pages:
            break
        page += 1
        time.sleep(RATE_LIMIT_WAIT)

    print(f"  ✅ {catalog_type}: {len(items)} total items")
    return items


def get_acl_for_item(catalog_type, item_id):
    """
    Call the getACL endpoint for a single catalog item.
    item_id should already be base64url-encoded.
    Token is refreshed automatically via token_mgr.
    """
    url  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}/{item_id}/actions/getACL"
    hdrs = get_headers()    # token_mgr auto-refreshes here
    hdrs["Content-Length"] = "0"

    resp = requests.post(url, headers=hdrs, timeout=30)

    if resp.status_code in (403, 404):
        return []
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────────────────────
# SECTION 4: EXTRACT — Fetch All Items + ACLs
# ─────────────────────────────────────────────────────────────

def extract_all_acls():
    """
    For each catalog type, fetch all items then get their ACLs.
    Token is managed automatically — no token passing required.
    Returns a flat list of records, one row per item+principal.
    """
    all_records  = []
    extracted_at = datetime.now(tz=timezone.utc).isoformat()

    for catalog_type in CATALOG_TYPES:
        print(f"\n{'─'*50}")
        print(f"🔍 Processing: {catalog_type.upper()}")
        print(f"{'─'*50}")

        items = get_catalog_items(catalog_type)   # no token arg needed

        for item in items:
            raw_id   = item.get("id") or item.get("objectId") or ""
            name     = item.get("name", "")
            path     = item.get("path", "") or item.get("objectPath", "")
            owner    = item.get("owner", {})
            owner_nm = owner.get("displayName", "") if isinstance(owner, dict) else str(owner)
            created  = item.get("created", "")
            modified = item.get("lastModified", "") or item.get("modified", "")

            encoded_id = b64_encode_id(raw_id) if raw_id else ""
            if not encoded_id:
                continue

            acl_entries = get_acl_for_item(catalog_type, encoded_id)   # no token arg needed
            time.sleep(RATE_LIMIT_WAIT)

            if not acl_entries:
                # Record item with no ACL entries (e.g. access denied)
                all_records.append({
                    "CATALOG_TYPE":      catalog_type,
                    "ITEM_ID":           raw_id,
                    "ITEM_NAME":         name,
                    "ITEM_PATH":         path,
                    "ITEM_OWNER":        owner_nm,
                    "ITEM_CREATED":      created,
                    "ITEM_MODIFIED":     modified,
                    "ACCOUNT_GUID":      None,
                    "ACCOUNT_TYPE":      None,
                    "ACCOUNT_NAME":      None,
                    "PERM_READ":         None,
                    "PERM_WRITE":        None,
                    "PERM_LIST":         None,
                    "PERM_DELETE":       None,
                    "PERM_CHANGE_PERM":  None,
                    "PERM_TAKE_OWN":     None,
                    "EXTRACTED_AT":      extracted_at
                })
            else:
                # One row per principal in the ACL
                for entry in acl_entries:
                    perms = entry.get("permissions", {})
                    all_records.append({
                        "CATALOG_TYPE":      catalog_type,
                        "ITEM_ID":           raw_id,
                        "ITEM_NAME":         name,
                        "ITEM_PATH":         path,
                        "ITEM_OWNER":        owner_nm,
                        "ITEM_CREATED":      created,
                        "ITEM_MODIFIED":     modified,
                        "ACCOUNT_GUID":      entry.get("accountGuid"),
                        "ACCOUNT_TYPE":      entry.get("accountType"),
                        "ACCOUNT_NAME":      entry.get("accountDisplayName"),
                        "PERM_READ":         int(perms.get("read",            False)),
                        "PERM_WRITE":        int(perms.get("write",           False)),
                        "PERM_LIST":         int(perms.get("list",            False)),
                        "PERM_DELETE":       int(perms.get("delete",          False)),
                        "PERM_CHANGE_PERM":  int(perms.get("changePermission",False)),
                        "PERM_TAKE_OWN":     int(perms.get("takeOwnership",   False)),
                        "EXTRACTED_AT":      extracted_at
                    })

    print(f"\n✅ Extraction complete. Total records: {len(all_records)}")
    return all_records


# ─────────────────────────────────────────────────────────────
# SECTION 5: LOAD — Write to ADW via cx_Oracle
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# SECTION 5: LOAD — Write to ADW via AIDP External Catalog
# ─────────────────────────────────────────────────────────────

def load_to_adw(records):
    """
    Write ACL records to catalog_manager.catalog_manager.OAC_CATALOG_ACL
    via the AIDP External Catalog connection (no wallet/cx_Oracle needed).
    Full overwrite on each run — clean snapshot every execution.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType
    )

    spark = SparkSession.builder.appName("oac_acl_load").getOrCreate()

    schema = StructType([
        StructField("CATALOG_TYPE",     StringType(),  True),
        StructField("ITEM_ID",          StringType(),  True),
        StructField("ITEM_NAME",        StringType(),  True),
        StructField("ITEM_PATH",        StringType(),  True),
        StructField("ITEM_OWNER",       StringType(),  True),
        StructField("ITEM_CREATED",     StringType(),  True),
        StructField("ITEM_MODIFIED",    StringType(),  True),
        StructField("ACCOUNT_GUID",     StringType(),  True),
        StructField("ACCOUNT_TYPE",     StringType(),  True),
        StructField("ACCOUNT_NAME",     StringType(),  True),
        StructField("PERM_READ",        IntegerType(), True),
        StructField("PERM_WRITE",       IntegerType(), True),
        StructField("PERM_LIST",        IntegerType(), True),
        StructField("PERM_DELETE",      IntegerType(), True),
        StructField("PERM_CHANGE_PERM", IntegerType(), True),
        StructField("PERM_TAKE_OWN",    IntegerType(), True),
        StructField("EXTRACTED_AT",     StringType(),  True),
    ])

    df = spark.createDataFrame(records, schema=schema)

    # Ensure schema exists in the external catalog
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AIDP_CATALOG}.{AIDP_SCHEMA}")

    # Overwrite table — full refresh each run
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FULL_TABLE_NAME))

    count = spark.table(FULL_TABLE_NAME).count()
    print(f"\n✅ Load complete → {FULL_TABLE_NAME}")
    print(f"   Rows written: {count:,}")
    print(f"   Table is now queryable in AIDP Master Catalog and OAC.")


# ─────────────────────────────────────────────────────────────
# SECTION 6: MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("  OAC CATALOG ACL EXTRACTOR")
print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
print("=" * 60)

# Step 1: Initialise token manager (fetches first token)
print("\n[1/3] Authenticating with OAC...")
_ = token_mgr.token   # Triggers initial fetch and prints confirmation

# Step 2: Extract all catalog ACLs
print("\n[2/3] Extracting catalog items and ACLs...")
records = extract_all_acls()   # token_mgr handles all refresh internally

# Step 3: Load to ADW
print("\n[3/3] Loading to ADW...")
if records:
    df_preview = pd.DataFrame(records)
    print(f"\n📊 Preview (first 5 rows):")
    print(df_preview.head())
    print(f"\nShape: {df_preview.shape}")
    load_to_adw(records)
else:
    print("⚠️  No records extracted. Check credentials and OAC permissions.")

print("\n🏁 Pipeline complete.")
