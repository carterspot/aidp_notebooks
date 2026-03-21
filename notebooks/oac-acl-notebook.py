# ============================================================
# OAC CATALOG ACL EXTRACTOR
# Purpose: Pull ACLs for all catalog item types from Oracle
#          Analytics Cloud via REST API and load into ADW via
#          the AIDP catalog_manager External Catalog connection.
# Target:  catalog_manager.catalog_manager.OAC_CATALOG_ACL
# Auth:    tokens.json downloaded from OAC Profile
#          (no client_secret or password required)
#
# PRE-REQUISITES:
#   1. In OAC: click name badge → Profile → Access Tokens
#      → Download tokens → upload tokens.json to AIDP workspace
#      (If Access Tokens tab not visible: Profile → Advanced
#       → Enable Developer Options → Save)
#   2. Calling user must have OAC BI Service Administrator role
#   3. catalog_manager External Catalog registered in AIDP
#   4. Spark cluster attached to this notebook
# ============================================================

# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────

import requests
import pandas as pd
import base64
import time
from datetime import datetime, timezone

# ── OAC Connection ───────────────────────────────────────────
OAC_BASE_URL    = "https://argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com"
OAC_API_VERSION = "20210901"

# ── Token Configuration ──────────────────────────────────────
# Get tokens.json from OAC Profile:
#   OAC Home → click your name badge (top right)
#   → Profile → Access Tokens → Download tokens
#   Upload tokens.json to your AIDP workspace and set path below.
#
# To enable the Access Tokens tab if not visible:
#   Profile → Advanced tab → Enable Developer Options → Save
#
# Tokens expire — refresh from OAC Profile when needed.
# The refresh token is used automatically to get a new
# access token without returning to the browser.
TOKENS_FILE     = "/Workspace/Shared/tokens.json"   # ← update path if needed
IDCS_DOMAIN_URL = "https://idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com"
CLIENT_ID       = "gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID"  # OAC built-in IDCS app

# ── Target Table (AIDP External Catalog → ADW) ───────────────
AIDP_CATALOG    = "catalog_manager"
AIDP_SCHEMA     = "catalog_manager"
AIDP_TABLE      = "OAC_CATALOG_ACL"
FULL_TABLE_NAME = f"{AIDP_CATALOG}.{AIDP_SCHEMA}.{AIDP_TABLE}"

# ── Catalog Types to Extract ─────────────────────────────────
CATALOG_TYPES = [
    "workbooks",
    "folders",
    "datasets",
    "dataflows",
    "connections"
]

# ── Pagination & Rate Limiting ────────────────────────────────
PAGE_SIZE            = 100   # Max items per API page
RATE_LIMIT_WAIT      = 0.2   # Seconds between API calls
TOKEN_REFRESH_BUFFER = 300   # Refresh token 5 min before expiry

print("✅ Configuration loaded.")


# ─────────────────────────────────────────────────────────────
# SECTION 2: TOKEN MANAGER (Auto-Refresh via Refresh Token)
# ─────────────────────────────────────────────────────────────

class OACTokenManager:
    """
    Manages OAuth 2.0 Bearer token lifecycle using tokens.json
    downloaded from OAC Profile → Access Tokens → Download tokens.

    - Loads access_token and refresh_token from tokens.json on init
    - Uses access_token directly for all API calls
    - Automatically uses refresh_token to get a new access_token
      when within TOKEN_REFRESH_BUFFER seconds of expiry
    - No client_secret, username, or password required
    """

    def __init__(self, tokens_file):
        self._access_token  = None
        self._refresh_token = None
        self._expires_at    = 0
        self._load_tokens(tokens_file)

    def _load_tokens(self, tokens_file):
        """Load initial tokens from tokens.json."""
        import json
        with open(tokens_file, "r") as f:
            data = json.load(f)

        self._access_token  = data.get("access_token") or data.get("accessToken")
        self._refresh_token = data.get("refresh_token") or data.get("refreshToken")

        if not self._access_token:
            raise ValueError("tokens.json must contain 'access_token'")
        if not self._refresh_token:
            raise ValueError(
                "tokens.json must contain 'refresh_token' — "
                "ensure offline_access scope was included when downloading"
            )

        # Decode expiry from JWT payload (no verification — just reading exp claim)
        import json as _json
        try:
            payload = self._access_token.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)   # pad base64
            claims = _json.loads(base64.urlsafe_b64decode(payload))
            self._expires_at = claims.get("exp", 0)
            expiry_str = datetime.fromtimestamp(
                self._expires_at, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"✅ tokens.json loaded.")
            print(f"   Access token expires at : {expiry_str}")
            print(f"   Time remaining          : {self.seconds_remaining}s")
        except Exception:
            self._expires_at = time.time() + 3600
            print("✅ tokens.json loaded (expiry unknown — will refresh proactively).")

    def _refresh(self):
        """Use refresh_token grant to obtain a new access_token from IDCS."""
        token_url = f"{IDCS_DOMAIN_URL}/oauth2/v1/token"
        payload = {
            "grant_type":    "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id":     CLIENT_ID,
            "scope":         "urn:opc:resource:consumer::all"
        }
        resp = requests.post(
            token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        # Refresh token may rotate — update if a new one is returned
        if "refresh_token" in data:
            self._refresh_token = data["refresh_token"]

        expires_in       = int(data.get("expires_in", 3600))
        self._expires_at = time.time() + expires_in
        expiry_str = datetime.fromtimestamp(
            self._expires_at, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"🔑 Token refreshed via refresh_token grant. "
              f"Valid for {expires_in}s → expires at {expiry_str}")

    @property
    def token(self):
        """Return a valid access token, refreshing proactively if needed."""
        time_remaining = self._expires_at - time.time()
        if time_remaining <= TOKEN_REFRESH_BUFFER:
            print(f"⚠️  Token expiring in {int(time_remaining)}s — refreshing...")
            self._refresh()
        return self._access_token

    def get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type":  "application/json"
        }

    @property
    def seconds_remaining(self):
        return max(0, int(self._expires_at - time.time()))


# Instantiate — loads tokens.json and validates on startup
token_mgr = OACTokenManager(TOKENS_FILE)


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
    Required by the OAC getACL endpoint.
    """
    return base64.urlsafe_b64encode(
        object_path.encode("utf-8")
    ).decode("utf-8").rstrip("=")


def get_catalog_items(catalog_type):
    """
    Paginate through all catalog items of a given type.
    Token is refreshed automatically via token_mgr on each page.
    """
    items = []
    page  = 1
    base  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}"

    while True:
        params = {"search": "*", "limit": PAGE_SIZE, "page": page}
        resp = requests.get(
            base,
            headers=get_headers(),
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
    Call getACL for a single catalog item.
    item_id must be base64url-encoded.
    Token refreshed automatically via token_mgr.
    """
    url  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}/{item_id}/actions/getACL"
    hdrs = get_headers()
    hdrs["Content-Length"] = "0"
    resp = requests.post(url, headers=hdrs, timeout=30)
    if resp.status_code in (403, 404):
        return []
    resp.raise_for_status()
    return resp.json()

print("✅ API helpers loaded.")


# ─────────────────────────────────────────────────────────────
# SECTION 4: EXTRACT — Fetch All Items + ACLs
# ─────────────────────────────────────────────────────────────

def extract_all_acls():
    """
    For each catalog type, fetch all items then get their ACLs.
    Returns a flat list of records — one row per item+principal.
    """
    all_records  = []
    extracted_at = datetime.now(tz=timezone.utc).isoformat()

    for catalog_type in CATALOG_TYPES:
        print(f"\n{'─'*50}")
        print(f"🔍 Processing: {catalog_type.upper()}")
        print(f"{'─'*50}")

        items = get_catalog_items(catalog_type)

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

            acl_entries = get_acl_for_item(catalog_type, encoded_id)
            time.sleep(RATE_LIMIT_WAIT)

            base_row = {
                "CATALOG_TYPE": catalog_type,
                "ITEM_ID":      raw_id,
                "ITEM_NAME":    name,
                "ITEM_PATH":    path,
                "ITEM_OWNER":   owner_nm,
                "ITEM_CREATED": created,
                "ITEM_MODIFIED":modified,
                "EXTRACTED_AT": extracted_at
            }

            if not acl_entries:
                all_records.append({
                    **base_row,
                    "ACCOUNT_GUID": None, "ACCOUNT_TYPE": None, "ACCOUNT_NAME": None,
                    "PERM_READ": None, "PERM_WRITE": None, "PERM_LIST": None,
                    "PERM_DELETE": None, "PERM_CHANGE_PERM": None, "PERM_TAKE_OWN": None
                })
            else:
                for entry in acl_entries:
                    perms = entry.get("permissions", {})
                    all_records.append({
                        **base_row,
                        "ACCOUNT_GUID":     entry.get("accountGuid"),
                        "ACCOUNT_TYPE":     entry.get("accountType"),
                        "ACCOUNT_NAME":     entry.get("accountDisplayName"),
                        "PERM_READ":        int(perms.get("read",             False)),
                        "PERM_WRITE":       int(perms.get("write",            False)),
                        "PERM_LIST":        int(perms.get("list",             False)),
                        "PERM_DELETE":      int(perms.get("delete",           False)),
                        "PERM_CHANGE_PERM": int(perms.get("changePermission", False)),
                        "PERM_TAKE_OWN":    int(perms.get("takeOwnership",    False))
                    })

    print(f"\n✅ Extraction complete. Total records: {len(all_records):,}")
    return all_records

print("✅ Extract function loaded.")


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
        StructType, StructField, StringType, IntegerType
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

    # Full overwrite — clean snapshot each run
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FULL_TABLE_NAME))

    count = spark.table(FULL_TABLE_NAME).count()
    print(f"\n✅ Load complete → {FULL_TABLE_NAME}")
    print(f"   Rows written : {count:,}")
    print(f"   Queryable in AIDP Master Catalog and OAC.")

print("✅ Load function loaded.")


# ─────────────────────────────────────────────────────────────
# SECTION 6: RUN PIPELINE
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("  OAC CATALOG ACL EXTRACTOR")
print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
print("=" * 60)

# Step 1: Validate token on startup
print("\n[1/3] Authenticating with OAC...")
_ = token_mgr.token

# Step 2: Extract all catalog ACLs
print("\n[2/3] Extracting catalog items and ACLs...")
records = extract_all_acls()

# Step 3: Load to ADW
print("\n[3/3] Loading to ADW...")
if records:
    df_preview = pd.DataFrame(records)
    print(f"\n📊 Preview (first 5 rows):")
    print(df_preview.head())
    print(f"\nShape: {df_preview.shape}")
    load_to_adw(records)
else:
    print("⚠️  No records extracted. Check token validity and OAC permissions.")

print("\n🏁 Pipeline complete.")
