# ============================================================
# OAC CATALOG ACL EXTRACTOR — BRONZE LAYER
# Version: 1.1 — added section comments
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
# All configuration is centralised here so the notebook can be
# pointed at a different OAC instance or ADW target without
# touching any logic in Sections 2–6.
#
# Key decisions documented here:
#
# TOKENS_FILE: tokens.json is downloaded from OAC Profile and
#   uploaded to the AIDP workspace. It contains both an access
#   token and a refresh token. The OACTokenManager in Section 2
#   handles auto-refresh so long-running extracts do not fail
#   mid-run due to token expiry.
#
# CLIENT_ID: This is the OAC instance's own built-in IDCS app
#   ID decoded from the JWT. The _APPID suffix is stripped
#   automatically by the token manager before calling the IDCS
#   refresh endpoint — it is kept here as-is to match the
#   original JWT value exactly.
#
# CATALOG_TYPES: The five types extracted cover the full set
#   of ACL-managed assets in OAC. Additional types supported
#   by the API (e.g. sequences, scripts, models) can be added
#   to this list if needed in a future iteration.
#
# PAGE_SIZE: The OAC Catalog API paginates results. 100 is the
#   practical maximum per page. Total pages are read from the
#   oa-page-count response header on each call.
#
# RATE_LIMIT_WAIT: A 0.2s sleep between ACL calls prevents
#   the notebook from hammering the OAC API. Increase this
#   value if the OAC instance shows signs of throttling.
#
# TOKEN_REFRESH_BUFFER: Tokens are proactively refreshed 300s
#   (5 minutes) before expiry. This ensures no API call is
#   made with an expired token even in large catalogs.
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
# Tokens expire in ~3600s. The refresh token is used
# automatically — re-download tokens.json only when both
# access and refresh tokens have expired.
TOKENS_FILE     = "/Workspace/Shared/tokens.json"
IDCS_DOMAIN_URL = "https://idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com"
CLIENT_ID       = "gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID"

# ── Target Table (AIDP External Catalog → ADW) ───────────────
# arganoadw_oacuser_sh is an External ADW Catalog registered
# in the AIDP Master Catalog. The oacuser schema must already
# exist in ADW. Do NOT attempt CREATE SCHEMA — this will fail
# against an External Catalog with a 502 Bad Gateway error.
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
PAGE_SIZE            = 100
RATE_LIMIT_WAIT      = 0.2
TOKEN_REFRESH_BUFFER = 300

print("=" * 50)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  OAC  : {OAC_BASE_URL}")
print(f"  Table: {FULL_TABLE_NAME}")
print(f"  Types: {CATALOG_TYPES}")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 2: TOKEN MANAGER (Auto-Refresh via Refresh Token)
# ─────────────────────────────────────────────────────────────
# Manages the full OAuth 2.0 token lifecycle for OAC API calls.
#
# Why tokens.json instead of client credentials?
#   The OAC Catalog/ACL APIs require a user-context Bearer
#   token — client credentials (client_id + client_secret)
#   alone are not sufficient. The simplest approach is to
#   download tokens.json directly from the OAC Profile page,
#   which requires no IDCS app configuration.
#
# How _load_tokens works:
#   Reads accessToken and refreshToken from tokens.json.
#   Decodes the JWT payload (no signature verification needed)
#   to extract the exp claim, giving the exact expiry time.
#   Both camelCase (accessToken) and snake_case (access_token)
#   key formats are supported.
#
# How _refresh works:
#   Calls the OAC refresh endpoint — NOT the IDCS token
#   endpoint. This was a critical discovery during development:
#   calling IDCS /oauth2/v1/token for refresh returns 401.
#   The correct endpoint is on the OAC hostname:
#     POST /api/dv/api/v1/tokens/token/refresh
#     Authorization: Bearer <current_access_token>
#     Content-Type: text/plain
#     Body: <refresh_token> (plain text, not JSON)
#   The _APPID suffix is stripped from CLIENT_ID before
#   calling — the token endpoint expects the raw GUID only.
#
# How auto-refresh works:
#   Every call to token_mgr.token checks time_remaining.
#   If within TOKEN_REFRESH_BUFFER seconds of expiry, _refresh
#   is called proactively before returning the token. This
#   ensures no mid-extract API call uses an expired token.
# ─────────────────────────────────────────────────────────────

class OACTokenManager:

    def __init__(self, tokens_file):
        self._access_token  = None
        self._refresh_token = None
        self._expires_at    = 0
        self._load_tokens(tokens_file)

    def _load_tokens(self, tokens_file):
        import json, json as _json
        with open(tokens_file, "r") as f:
            data = json.load(f)

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

        try:
            payload = self._access_token.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
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
        refresh_url   = f"{OAC_BASE_URL}/api/dv/api/v1/tokens/token/refresh"
        client_id_raw = CLIENT_ID.replace("_APPID", "")

        resp = requests.post(
            refresh_url,
            data=self._refresh_token,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type":  "text/plain"
            },
            timeout=30
        )

        if resp.status_code == 401:
            raise RuntimeError(
                "❌ Refresh token rejected (401 Unauthorized).\n"
                "   Your tokens.json has likely expired (tokens expire ~3600s).\n"
                "   Fix: OAC → Profile → Access Tokens → Download tokens\n"
                "        Re-upload tokens.json to /Workspace/Shared/ and re-run."
            )
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data.get("accessToken") or data.get("access_token")
        if not self._access_token:
            raise ValueError(f"❌ Refresh response missing accessToken. Response: {data}")

        if "refreshToken"  in data: self._refresh_token = data["refreshToken"]
        elif "refresh_token" in data: self._refresh_token = data["refresh_token"]

        expires_in       = int(data.get("expiresIn") or data.get("expires_in") or 3600)
        self._expires_at = time.time() + expires_in
        expiry_str = datetime.fromtimestamp(
            self._expires_at, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"🔑 Token refreshed. Valid for {expires_in}s → expires at {expiry_str}")

    @property
    def token(self):
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


print("Loading tokens.json...")
token_mgr = OACTokenManager(TOKENS_FILE)
print("=" * 50)
print("  SECTION 2 COMPLETE: Token Manager Ready")
print("=" * 50)


# ─────────────────────────────────────────────────────────────
# SECTION 3: OAC API HELPERS
# ─────────────────────────────────────────────────────────────
# Three utility functions used by the extraction loop in
# Section 4. All API calls go through these helpers so that
# token refresh, error handling, and encoding logic are
# defined once and reused consistently.
#
# get_headers():
#   Returns the Authorization and Content-Type headers needed
#   for every OAC REST API call. Calls token_mgr.token which
#   triggers a proactive refresh if the token is near expiry.
#   An optional token override is supported for testing.
#
# b64_encode_id():
#   The OAC getACL endpoint requires catalog object paths to
#   be Base64 URL-safe encoded before passing them as a URL
#   path parameter. Standard base64 characters + and / are
#   replaced with - and _ respectively, and trailing = padding
#   is stripped. This is the mirror operation of the Silver
#   layer's decode_base64_id UDF.
#
# get_catalog_items():
#   Paginates through all items of a given catalog type using
#   the OAC search endpoint with wildcard search=*.
#   The oa-page-count response header tells the function how
#   many pages exist so all items are retrieved regardless of
#   catalog size. A rate limit sleep between pages prevents
#   overwhelming the OAC API.
#
# get_acl_for_item():
#   Calls the getACL action endpoint for a single catalog item.
#   403 (forbidden) and 404 (not found) are handled silently —
#   both return an empty list so the extraction continues.
#   This covers cases where the calling user does not have
#   permission to view an item's ACL, or the item was deleted
#   between the catalog list call and the ACL call.
# ─────────────────────────────────────────────────────────────

def get_headers(token=None):
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return token_mgr.get_headers()


def b64_encode_id(object_path):
    return base64.urlsafe_b64encode(
        object_path.encode("utf-8")
    ).decode("utf-8").rstrip("=")


def get_catalog_items(catalog_type):
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
# The core extraction loop. For each catalog type, all items
# are fetched first (paginated), then getACL is called for
# each item individually. Results are flattened into a list
# of dicts — one row per item+principal combination.
#
# Why one row per item+principal?
#   Each catalog item can have multiple principals (users or
#   roles) with different permission sets. A flat structure
#   with one row per combination is the most query-friendly
#   format for OAC reporting and SQL aggregation in Silver.
#
# Item metadata field names vary slightly by catalog type:
#   - id vs objectId (connections use objectId)
#   - path vs objectPath (same variation)
#   - owner may be a dict with displayName or a plain string
#   - lastModified vs modified (type-dependent)
#   These variations are handled defensively with .get() chains.
#
# Items with no ACL entries:
#   If getACL returns an empty list (403, 404, or genuinely
#   empty), the item is still recorded with NULL permission
#   fields. This preserves catalog coverage — an admin needs
#   to know which items have no visible ACL as much as which
#   ones do.
#
# EXTRACTED_AT is set once at the start of the function and
#   stamped on every row so the full snapshot has a consistent
#   extraction timestamp regardless of how long the loop takes.
# ─────────────────────────────────────────────────────────────

def extract_all_acls():
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
# Writes the extracted ACL records to ADW via the AIDP
# External Catalog Spark connection. No wallet, no cx_Oracle,
# no JDBC string required — Spark resolves the 3-part catalog
# path through the External Catalog metadata layer.
#
# Key design decisions:
#
# No CREATE SCHEMA:
#   Running spark.sql('CREATE SCHEMA IF NOT EXISTS ...') against
#   an External ADW Catalog returns a 502 Bad Gateway from the
#   AIDP Metastore service. The schema must pre-exist in ADW.
#   This was discovered during initial development and is
#   documented in docs/aidp-setup-notes.md.
#
# No Delta format:
#   ADW External Catalogs do not support Delta table format.
#   saveAsTable() without .format('delta') uses the default
#   format for the catalog type, which ADW handles correctly.
#
# Full overwrite strategy:
#   mode('overwrite') truncates and reloads on every run.
#   This is appropriate because ACL permissions can change at
#   any time — a full snapshot is more reliable than an
#   incremental merge for a permissions audit use case.
#   The table is small enough (~200 rows) that a full reload
#   has negligible performance impact.
#
# overwriteSchema=true:
#   Allows column additions between runs without requiring
#   a manual DROP TABLE in ADW. Safe here because Bronze is
#   always fully regenerated from the API on every run.
# ─────────────────────────────────────────────────────────────

def load_to_adw(records):
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
# Orchestrates the three pipeline steps in sequence:
#   [1/3] Auth    — validates token_mgr loaded successfully
#                   and prints remaining token lifetime
#   [2/3] Extract — calls extract_all_acls() which loops all
#                   catalog types and returns flat record list
#   [3/3] Load    — previews the DataFrame then calls
#                   load_to_adw() to write to ADW
#
# The pandas preview before load serves two purposes:
#   1. Visual confirmation the data looks correct before
#      committing to ADW
#   2. Shape check — if record count is 0 or unexpectedly low,
#      stop here and investigate token validity and OAC
#      permissions before proceeding to the write step
#
# tokens.json must be uploaded to /Workspace/Shared/ before
# running this section. The token manager will print the
# expiry time in Step 1 — if it shows 'Expired or expiring
# soon', re-download tokens.json from OAC Profile first.
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
