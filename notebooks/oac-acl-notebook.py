# ============================================================
# OAC CATALOG ACL EXTRACTOR — BRONZE LAYER
# Purpose: Pull ACLs for all catalog item types from Oracle
#          Analytics Cloud via REST API and load into ADW via
#          the AIDP arganoadw_oacuser_sh External Catalog.
# Target:  arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
# Auth:    tokens.json downloaded from OAC Profile
#          (no client_secret or password required)
#
# PRE-REQUISITES:
#   1. In OAC: click name badge → Profile → Access Tokens
#      → Download tokens → upload tokens.json to AIDP workspace
#      (If Access Tokens tab not visible: Profile → Advanced
#       → Enable Developer Options → Save)
#   2. Calling user must have OAC BI Service Administrator role
#   3. arganoadw_oacuser_sh External Catalog registered in AIDP
#   4. oacuser schema already exists in ADW (do not create it)
#   5. Spark cluster attached to this notebook
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
# If the Access Tokens tab is not visible:
#   Profile → Advanced tab → Enable Developer Options → Save
#
# Tokens expire — re-download from OAC Profile when needed.
# The refresh token is used automatically to get a new
# access token without returning to the browser.
TOKENS_FILE     = "/Workspace/Shared/tokens.json"    # ← update if stored elsewhere
IDCS_DOMAIN_URL = "https://idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com"
CLIENT_ID       = "gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID"   # OAC built-in IDCS app

# ── Target Table (AIDP External Catalog → ADW) ───────────────
# NOTE: arganoadw_oacuser_sh is an External ADW Catalog.
#       The oacuser schema must already exist in ADW.
#       Do NOT attempt CREATE SCHEMA against an External Catalog.
#       Do NOT use Delta format — ADW does not support Delta tables.
AIDP_CATALOG    = "arganoadw_oacuser_sh"
AIDP_SCHEMA     = "oacuser"
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
TOKEN_REFRESH_BUFFER = 300   # Proactively refresh 5 min before expiry

print("=" * 50)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  OAC  : {OAC_BASE_URL}")
print(f"  Table: {FULL_TABLE_NAME}")
print(f"  Types: {CATALOG_TYPES}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 2: TOKEN MANAGER (Auto-Refresh via Refresh Token)
# ─────────────────────────────────────────────────────────────

class OACTokenManager:
    """
    Manages OAuth 2.0 Bearer token lifecycle using tokens.json
    downloaded from OAC Profile → Access Tokens → Download tokens.

    - Loads access_token and refresh_token from tokens.json on init
    - Decodes JWT exp claim to track expiry precisely
    - Auto-refreshes via refresh_token grant before expiry
    - No client_secret, username, or password required
    - Strips _APPID suffix from CLIENT_ID for token endpoint calls
    """

    def __init__(self, tokens_file):
        self._access_token  = None
        self._refresh_token = None
        self._expires_at    = 0
        self._load_tokens(tokens_file)

    def _load_tokens(self, tokens_file):
        """Load initial tokens from tokens.json and decode expiry."""
        import json, json as _json
        with open(tokens_file, "r") as f:
            data = json.load(f)

        # Support both camelCase and snake_case key formats
        self._access_token  = data.get("access_token")  or data.get("accessToken")
        self._refresh_token = data.get("refresh_token") or data.get("refreshToken")

        if not self._access_token:
            raise ValueError(
                "❌ tokens.json must contain 'access_token'.\n"
                "   Re-download from OAC → Profile → Access Tokens → Download tokens."
            )
        if not self._refresh_token:
            raise ValueError(
                "❌ tokens.json must contain 'refresh_token'.\n"
                "   Ensure the token was downloaded (not just copied) from OAC Profile."
            )

        # Decode JWT payload to read exp claim (no signature verification needed)
        try:
            payload = self._access_token.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)    # pad to valid base64 length
            claims = _json.loads(base64.urlsafe_b64decode(payload))
            self._expires_at = claims.get("exp", 0)
            expiry_str = datetime.fromtimestamp(
                self._expires_at, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            remaining = self.seconds_remaining
            status = "✅ Valid" if remaining > TOKEN_REFRESH_BUFFER else "⚠️  Expired or expiring soon"
            print(f"  Token status   : {status}")
            print(f"  Expires at     : {expiry_str}")
            print(f"  Time remaining : {remaining}s")
        except Exception:
            self._expires_at = time.time() + 3600
            print("  Token expiry   : unknown (will refresh proactively)")

    def _refresh(self):
        """
        Refresh the access token using OAC's own token refresh endpoint.
        This is NOT the IDCS /oauth2/v1/token endpoint.

        OAC refresh flow (from tokens.json download instructions):
          POST /api/dv/api/v1/tokens/token/refresh
          Authorization: Bearer <current_access_token>
          Content-Type: text/plain
          Body: <refresh_token> (plain text)
        """
        refresh_url = f"{OAC_BASE_URL}/api/dv/api/v1/tokens/token/refresh"

        resp = requests.post(
            refresh_url,
            data=self._refresh_token,          # plain text body
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type":  "text/plain"
            },
            timeout=30
        )

        if resp.status_code == 401:
            raise RuntimeError(
                "❌ Refresh token rejected (401 Unauthorized).\n"
                "   Your tokens.json has likely expired (refresh tokens expire ~3600s).\n"
                "   Fix: OAC → Profile → Access Tokens → Download tokens\n"
                "        Re-upload tokens.json to /Workspace/Shared/ and re-run."
            )
        resp.raise_for_status()
        data = resp.json()

        # OAC returns camelCase keys
        self._access_token  = data.get("accessToken")  or data.get("access_token")
        if not self._access_token:
            raise ValueError(f"❌ Refresh response missing accessToken. Response: {data}")

        # Update refresh token if a new one is returned
        if "refreshToken" in data:
            self._refresh_token = data["refreshToken"]
        elif "refresh_token" in data:
            self._refresh_token = data["refresh_token"]

        # OAC refresh response may not include expires_in — default to 3600
        expires_in       = int(data.get("expiresIn") or data.get("expires_in") or 3600)
        self._expires_at = time.time() + expires_in
        expiry_str = datetime.fromtimestamp(
            self._expires_at, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"🔑 Token refreshed via OAC endpoint. "
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


# Instantiate — loads and validates tokens.json immediately
print("Loading tokens.json...")
token_mgr = OACTokenManager(TOKENS_FILE)
print("=" * 50)
print("  SECTION 2 COMPLETE: Token Manager Ready")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 3: OAC API HELPERS
# ─────────────────────────────────────────────────────────────

def get_headers(token=None):
    """Use token_mgr by default; accepts manual override for testing."""
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return token_mgr.get_headers()


def b64_encode_id(object_path):
    """
    Base64URL-safe encode a catalog object path/ID.
    Required format for the OAC getACL endpoint.
    """
    return base64.urlsafe_b64encode(
        object_path.encode("utf-8")
    ).decode("utf-8").rstrip("=")


def get_catalog_items(catalog_type):
    """
    Paginate through all catalog items of a given type.
    Reads oa-page-count response header to loop all pages.
    Token auto-refreshes via token_mgr on every request.
    """
    items = []
    page  = 1
    base  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}"

    while True:
        params = {"search": "*", "limit": PAGE_SIZE, "page": page}
        resp   = requests.get(base, headers=get_headers(), params=params, timeout=30)

        if resp.status_code == 404:
            print(f"  ⚠️  {catalog_type}: no items found (404) — skipping")
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
    POST to getACL endpoint for a single catalog item.
    item_id must already be base64url-encoded.
    Returns list of ACL entries or empty list on 403/404.
    """
    url  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}/{item_id}/actions/getACL"
    hdrs = get_headers()
    hdrs["Content-Length"] = "0"
    resp = requests.post(url, headers=hdrs, timeout=30)
    if resp.status_code in (403, 404):
        return []
    resp.raise_for_status()
    return resp.json()


print("=" * 50)
print("  SECTION 3 COMPLETE: API Helpers Loaded")
print(f"  Functions: get_headers, b64_encode_id,")
print(f"             get_catalog_items, get_acl_for_item")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 4: EXTRACT — Fetch All Items + ACLs
# ─────────────────────────────────────────────────────────────

def extract_all_acls():
    """
    For each catalog type, fetches all items then calls getACL
    per item. Returns a flat list — one row per item+principal.
    Items with no ACL entries are recorded with NULL permission fields.
    """
    all_records  = []
    extracted_at = datetime.now(tz=timezone.utc).isoformat()

    for catalog_type in CATALOG_TYPES:
        print(f"\n{'─' * 50}")
        print(f"🔍 Processing: {catalog_type.upper()}")
        print(f"{'─' * 50}")

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
                "CATALOG_TYPE":  catalog_type,
                "ITEM_ID":       raw_id,
                "ITEM_NAME":     name,
                "ITEM_PATH":     path,
                "ITEM_OWNER":    owner_nm,
                "ITEM_CREATED":  created,
                "ITEM_MODIFIED": modified,
                "EXTRACTED_AT":  extracted_at
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

    print("=" * 50)
    print("  SECTION 4 COMPLETE: Extraction Done")
    print(f"  Total records: {len(all_records):,}")
    print("=" * 50)
    return all_records


print("=" * 50)
print("  SECTION 4 READY: Extract Function Loaded")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 5: LOAD — Write to ADW via AIDP External Catalog
# ─────────────────────────────────────────────────────────────

def load_to_adw(records):
    """
    Writes ACL records to arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
    via the AIDP External Catalog Spark connection.

    KEY RULES for External ADW Catalogs:
      - DO NOT use CREATE SCHEMA — schema must pre-exist in ADW
      - DO NOT use .format("delta") — ADW does not support Delta tables
      - Use saveAsTable() with 3-part name — Spark handles the JDBC write
      - mode("overwrite") truncates and reloads on every run
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType
    )

    spark = SparkSession.builder.appName("oac_acl_bronze").getOrCreate()

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

    # Write via External Catalog connection — no Delta, no CREATE SCHEMA
    (df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FULL_TABLE_NAME))

    count = spark.table(FULL_TABLE_NAME).count()
    print("=" * 50)
    print("  SECTION 5 COMPLETE: Load Done")
    print(f"  Table : {FULL_TABLE_NAME}")
    print(f"  Rows  : {count:,}")
    print("  Status: Queryable in AIDP Master Catalog + OAC")
    print("=" * 50)


print("=" * 50)
print("  SECTION 5 READY: Load Function Loaded")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 6: RUN PIPELINE
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("  OAC CATALOG ACL EXTRACTOR — BRONZE LAYER")
print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
print("=" * 60)

# Step 1: Validate token
print("\n[1/3] Authenticating with OAC...")
_ = token_mgr.token
print(f"  ✅ Token valid. {token_mgr.seconds_remaining}s remaining.")

# Step 2: Extract
print("\n[2/3] Extracting catalog items and ACLs...")
records = extract_all_acls()

# Step 3: Load
print("\n[3/3] Loading to ADW...")
if records:
    df_preview = pd.DataFrame(records)
    print(f"\n📊 Preview (first 5 rows):")
    print(df_preview.head())
    print(f"\nShape: {df_preview.shape}")
    load_to_adw(records)
else:
    print("⚠️  No records extracted.")
    print("    Check: token is valid, user has BI Service Administrator role.")

print("\n" + "=" * 60)
print("  🏁 BRONZE PIPELINE COMPLETE")
print("=" * 60)