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
import cx_Oracle
import pandas as pd
import base64
import json
import time
from datetime import datetime

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

# -- ADW Connection ------------------------------------------
# Wallet must be unzipped to WALLET_DIR on the cluster
WALLET_DIR      = "/path/to/wallet"                      # e.g. /home/opc/wallet/adw_wallet
ADW_DSN         = "<your-adw-service-name>"              # e.g. adwprod_high (from tnsnames.ora in wallet)
ADW_USER        = "<adw-schema-username>"
ADW_PASSWORD    = "<adw-schema-password>"
ADW_TABLE       = "OAC_CATALOG_ACL"

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

def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }


def b64_encode_id(object_path):
    """
    Base64URL-safe encode the catalog object path/ID.
    OAC requires this encoding for getACL and detail endpoints.
    Example: /shared/Sales/MyWorkbook → base64url string
    """
    return base64.urlsafe_b64encode(
        object_path.encode("utf-8")
    ).decode("utf-8").rstrip("=")


def get_catalog_items(token, catalog_type):
    """
    Paginate through all catalog items of a given type.
    Returns list of dicts with id, name, path, owner, created, modified.
    """
    items = []
    page  = 1
    base  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}"
    hdrs  = get_headers(token)

    while True:
        params = {
            "search": "*",
            "limit":  PAGE_SIZE,
            "page":   page
        }
        resp = requests.get(base, headers=hdrs, params=params, timeout=30)
        
        # 404 means no items of this type — skip gracefully
        if resp.status_code == 404:
            print(f"  ⚠️  {catalog_type}: no items found (404)")
            break
        resp.raise_for_status()

        page_items = resp.json()
        if not page_items:
            break

        items.extend(page_items)

        total_pages = int(resp.headers.get("oa-page-count", 1))
        print(f"  📄 {catalog_type}: page {page}/{total_pages} — {len(page_items)} items")

        if page >= total_pages:
            break
        page += 1
        time.sleep(RATE_LIMIT_WAIT)

    print(f"  ✅ {catalog_type}: {len(items)} total items")
    return items


def get_acl_for_item(token, catalog_type, item_id):
    """
    Call the getACL endpoint for a single catalog item.
    item_id should already be base64url-encoded.
    Returns list of ACL entries or empty list on error.
    """
    url  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}/{item_id}/actions/getACL"
    hdrs = get_headers(token)
    hdrs["Content-Length"] = "0"

    resp = requests.post(url, headers=hdrs, timeout=30)

    if resp.status_code in (403, 404):
        return []   # No access or item gone — skip silently
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────────────────────
# SECTION 4: EXTRACT — Fetch All Items + ACLs
# ─────────────────────────────────────────────────────────────

def extract_all_acls(token):
    """
    For each catalog type, fetch all items then get their ACLs.
    Returns a flat list of records, one row per item+principal.
    """
    all_records = []
    extracted_at = datetime.utcnow().isoformat()

    for catalog_type in CATALOG_TYPES:
        print(f"\n{'─'*50}")
        print(f"🔍 Processing: {catalog_type.upper()}")
        print(f"{'─'*50}")

        items = get_catalog_items(token, catalog_type)

        for item in items:
            # Extract item metadata — field names vary slightly by type
            raw_id   = item.get("id") or item.get("objectId") or ""
            name     = item.get("name", "")
            path     = item.get("path", "") or item.get("objectPath", "")
            owner    = item.get("owner", {})
            owner_nm = owner.get("displayName", "") if isinstance(owner, dict) else str(owner)
            created  = item.get("created", "")
            modified = item.get("lastModified", "") or item.get("modified", "")

            # Encode ID for ACL endpoint
            encoded_id = b64_encode_id(raw_id) if raw_id else ""
            if not encoded_id:
                continue

            # Get ACL entries for this item
            acl_entries = get_acl_for_item(token, catalog_type, encoded_id)
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

DDL_CREATE_TABLE = f"""
CREATE TABLE {ADW_TABLE} (
    CATALOG_TYPE      VARCHAR2(50),
    ITEM_ID           VARCHAR2(1000),
    ITEM_NAME         VARCHAR2(500),
    ITEM_PATH         VARCHAR2(2000),
    ITEM_OWNER        VARCHAR2(255),
    ITEM_CREATED      VARCHAR2(50),
    ITEM_MODIFIED     VARCHAR2(50),
    ACCOUNT_GUID      VARCHAR2(255),
    ACCOUNT_TYPE      VARCHAR2(50),
    ACCOUNT_NAME      VARCHAR2(255),
    PERM_READ         NUMBER(1),
    PERM_WRITE        NUMBER(1),
    PERM_LIST         NUMBER(1),
    PERM_DELETE       NUMBER(1),
    PERM_CHANGE_PERM  NUMBER(1),
    PERM_TAKE_OWN     NUMBER(1),
    EXTRACTED_AT      VARCHAR2(50)
)
"""

DML_INSERT = f"""
INSERT INTO {ADW_TABLE} (
    CATALOG_TYPE, ITEM_ID, ITEM_NAME, ITEM_PATH,
    ITEM_OWNER, ITEM_CREATED, ITEM_MODIFIED,
    ACCOUNT_GUID, ACCOUNT_TYPE, ACCOUNT_NAME,
    PERM_READ, PERM_WRITE, PERM_LIST, PERM_DELETE,
    PERM_CHANGE_PERM, PERM_TAKE_OWN, EXTRACTED_AT
) VALUES (
    :CATALOG_TYPE, :ITEM_ID, :ITEM_NAME, :ITEM_PATH,
    :ITEM_OWNER, :ITEM_CREATED, :ITEM_MODIFIED,
    :ACCOUNT_GUID, :ACCOUNT_TYPE, :ACCOUNT_NAME,
    :PERM_READ, :PERM_WRITE, :PERM_LIST, :PERM_DELETE,
    :PERM_CHANGE_PERM, :PERM_TAKE_OWN, :EXTRACTED_AT
)
"""


def load_to_adw(records):
    """
    Truncate and reload ADW table with fresh ACL snapshot.
    Creates table on first run. Uses executemany for performance.
    """
    cx_Oracle.init_oracle_client()  # Use instant client or wallet path
    
    # Point cx_Oracle to wallet directory
    conn = cx_Oracle.connect(
        user=ADW_USER,
        password=ADW_PASSWORD,
        dsn=ADW_DSN,
        config_dir=WALLET_DIR,
        wallet_location=WALLET_DIR,
        wallet_password=None  # Set if wallet is password-protected
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    try:
        cursor.execute(DDL_CREATE_TABLE)
        conn.commit()
        print(f"✅ Table {ADW_TABLE} created.")
    except cx_Oracle.DatabaseError as e:
        err, = e.args
        if "ORA-00955" in str(err):  # Table already exists
            print(f"ℹ️  Table {ADW_TABLE} already exists — truncating.")
        else:
            raise

    # Truncate for full refresh (change to merge logic if incremental needed)
    cursor.execute(f"TRUNCATE TABLE {ADW_TABLE}")
    conn.commit()

    # Batch insert
    batch_size = 500
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(DML_INSERT, batch)
        conn.commit()
        print(f"  ↳ Inserted rows {i+1}–{min(i+batch_size, total)} of {total}")

    cursor.close()
    conn.close()
    print(f"\n✅ Load complete. {total} rows in {ADW_TABLE}.")


# ─────────────────────────────────────────────────────────────
# SECTION 6: MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("  OAC CATALOG ACL EXTRACTOR")
print(f"  Run time: {datetime.utcnow().isoformat()} UTC")
print("=" * 60)

# Step 1: Authenticate
print("\n[1/3] Authenticating with OAC...")
token = get_oauth_token()

# Step 2: Extract all catalog ACLs
print("\n[2/3] Extracting catalog items and ACLs...")
records = extract_all_acls(token)

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
