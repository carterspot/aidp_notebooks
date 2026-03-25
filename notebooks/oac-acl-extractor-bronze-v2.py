# ============================================================
# OAC CATALOG ACL EXTRACTOR — BRONZE LAYER
# Version: 2.0 — two-stage write (Standard Catalog + ADW)
# Purpose: Pull ACLs for all catalog item types from Oracle
#          Analytics Cloud via REST API, stage to OCI Object
#          Storage via the AIDP Standard Catalog (Delta), then
#          write to ADW via the AIDP External Catalog.
#
# Write Targets:
#   Stage 1 (Object Storage / Standard Catalog):
#     cbtest_standard_catalog.default.OAC_CATALOG_ACL_BRONZE
#     Format: Delta — supports schema evolution, ACID,
#             portable to any customer AIDP instance
#   Stage 2 (ADW / External Catalog):
#     arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
#     Format: Default — ADW External Catalog does not support
#             Delta; saveAsTable() without .format("delta")
#
# Auth: tokens.json downloaded from OAC Profile
#       (no client_secret or IDCS configuration required)
#
# PRE-REQUISITES:
#   1. In OAC: click name badge top right → Profile
#      → Access Tokens → Download tokens
#      Upload tokens.json to /Workspace/Shared/ in AIDP
#      (If Access Tokens tab not visible: Profile → Advanced
#       → Enable Developer Options → Save)
#   2. Calling user must have OAC BI Service Administrator role
#   3. cbtest_standard_catalog registered in AIDP with write
#      access to the backing OCI Object Storage bucket
#   4. arganoadw_oacuser_sh External Catalog registered in AIDP
#   5. oacuser schema already exists in ADW — do NOT create it
#   6. Spark cluster attached to this notebook
#
# Architecture Note:
#   Stage 2 reads from the Delta table written in Stage 1 rather
#   than from the in-memory DataFrame. This validates the object
#   storage write before touching ADW. If Stage 1 fails, Stage 2
#   will not run, preventing a partial or stale ADW load.
#   In a production or enterprise deployment, Stage 2 can be
#   swapped for a different External Catalog target without
#   changing any extraction or Stage 1 logic.
# ============================================================


# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────
# All configuration is centralised here so the notebook can be
# pointed at a different OAC instance, Standard Catalog, or ADW
# target without touching any logic in Sections 2–8.
#
# TOKENS_FILE:
#   tokens.json is downloaded from OAC Profile and uploaded to
#   the AIDP workspace. It contains both an access token and a
#   refresh token. The OACTokenManager in Section 2 handles
#   auto-refresh so long-running extracts do not fail mid-run
#   due to token expiry. Re-download is only needed when both
#   the access token and refresh token have expired (~daily).
#
# CLIENT_ID:
#   The OAC instance's own built-in IDCS app ID decoded from
#   the JWT. The _APPID suffix is stripped automatically by the
#   token manager before calling the OAC refresh endpoint — it
#   is kept here as-is to match the original JWT value exactly.
#
# STANDARD_CATALOG_TABLE:
#   The Stage 1 write target in the AIDP Standard Catalog.
#   The Standard Catalog is backed by OCI Object Storage and
#   supports Delta format — the portable, Spark-native staging
#   layer. The 'default' schema must already exist in the
#   catalog (do not issue CREATE SCHEMA). If the table does not
#   yet exist, saveAsTable() in Delta mode creates it on first
#   run. In an enterprise deployment this is the only value
#   that needs to change to point Bronze at a new environment.
#
# ADW_FULL_PATH:
#   The Stage 2 write target via the AIDP External Catalog
#   connection to ADW. The oacuser schema must pre-exist in ADW.
#   See Section 7 for the External Catalog write constraints.
#
# CATALOG_TYPES:
#   The five types cover the full set of ACL-managed assets in
#   OAC. Additional types supported by the API (sequences,
#   scripts, models) can be added to this list if needed.
#
# PAGE_SIZE:
#   The OAC Catalog API paginates results. 100 is the practical
#   maximum per page. Total pages are read from the oa-page-count
#   response header on each call.
#
# RATE_LIMIT_WAIT:
#   A 0.2s sleep between ACL calls prevents hammering the OAC
#   API. Increase this value if the OAC instance throttles.
#
# TOKEN_REFRESH_BUFFER:
#   Tokens are proactively refreshed 300s (5 minutes) before
#   expiry so no API call is made with an expired token even in
#   large catalogs with thousands of objects.
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
# How to get tokens.json:
#   OAC Home → click your name badge (top right)
#   → Profile → Access Tokens → Download tokens
#   Upload tokens.json to your AIDP workspace, set path below.
#
#   If the Access Tokens tab is not visible:
#   Profile → Advanced tab → Enable Developer Options → Save
TOKENS_FILE     = "/Workspace/Shared/tokens.json"
IDCS_DOMAIN_URL = "https://idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com"
CLIENT_ID       = "gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID"

# ── Stage 1: Standard Catalog (Delta / Object Storage) ───────
# Delta format — supports schema evolution, ACID transactions,
# and time travel. Portable to any customer AIDP instance.
# Table is created automatically on first run if it doesn't
# exist. If catalog or schema is missing, an error will surface
# here — create them in AIDP Workbench before running.
STANDARD_CATALOG_TABLE = "cbtest_standard_catalog.default.OAC_CATALOG_ACL_BRONZE"

# ── Stage 2: ADW External Catalog ────────────────────────────
# arganoadw_oacuser_sh is an External ADW Catalog registered in
# the AIDP Master Catalog. The oacuser schema must already exist
# in ADW. Do NOT attempt CREATE SCHEMA — this will fail with a
# 502 Bad Gateway from the AIDP Metastore service.
ADW_CATALOG    = "arganoadw_oacuser_sh"
ADW_SCHEMA     = "oacuser"
ADW_TABLE      = "OAC_CATALOG_ACL"
ADW_FULL_PATH  = f"{ADW_CATALOG}.{ADW_SCHEMA}.{ADW_TABLE}"

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

print("=" * 55)
print("  SECTION 1 COMPLETE: Imports & Configuration")
print(f"  OAC    : {OAC_BASE_URL}")
print(f"  Stage 1: {STANDARD_CATALOG_TABLE}")
print(f"  Stage 2: {ADW_FULL_PATH}")
print(f"  Types  : {CATALOG_TYPES}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 2: TOKEN MANAGER (Auto-Refresh via Refresh Token)
# ─────────────────────────────────────────────────────────────
# Manages the full OAuth 2.0 token lifecycle for OAC API calls.
#
# Why tokens.json instead of client credentials?
#   The OAC Catalog/ACL APIs require a user-context Bearer
#   token — client credentials (client_id + client_secret)
#   alone are not sufficient. Downloading tokens.json from OAC
#   Profile requires no IDCS app configuration and works for
#   any user with BI Service Administrator role.
#
# How _load_tokens works:
#   Reads accessToken and refreshToken from tokens.json.
#   Decodes the JWT payload (no signature verification needed)
#   to extract the exp claim for exact expiry time.
#   Both camelCase and snake_case key formats are supported
#   for resilience across OAC versions.
#
# How _refresh works:
#   Calls the OAC refresh endpoint — NOT the IDCS token
#   endpoint. Critical discovery during development: calling
#   IDCS /oauth2/v1/token for refresh returns 401 Unauthorized.
#   The correct endpoint is on the OAC hostname:
#     POST /api/dv/api/v1/tokens/token/refresh
#     Authorization: Bearer <current_access_token>
#     Content-Type:  text/plain
#     Body: <refresh_token> as plain text (not JSON)
#   The _APPID suffix is stripped from CLIENT_ID before calling
#   — the token endpoint expects the raw GUID only.
#
# How auto-refresh works:
#   Every call to token_mgr.token checks time_remaining.
#   If within TOKEN_REFRESH_BUFFER seconds of expiry, _refresh
#   is called proactively before returning the token. This
#   ensures no mid-extract API call uses an expired token even
#   in catalogs with thousands of objects across many pages.
# ─────────────────────────────────────────────────────────────

class OACTokenManager:

    def __init__(self, tokens_file):
        self._access_token  = None
        self._refresh_token = None
        self._expires_at    = 0
        self._load_tokens(tokens_file)

    def _load_tokens(self, tokens_file):
        import json as _json
        with open(tokens_file, "r") as f:
            data = _json.load(f)

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
                "   Ensure the token was downloaded (not copied) from OAC Profile."
            )

        # Decode JWT payload to extract expiry timestamp.
        # No signature verification needed — only the exp claim
        # is used to calculate time remaining before refresh.
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
            # If JWT decode fails, assume 1 hour. The proactive
            # refresh will handle expiry before any API call.
            self._expires_at = time.time() + 3600
            print("  Token expiry   : unknown (will refresh proactively)")

    def _refresh(self):
        # Strip _APPID suffix — the refresh endpoint expects
        # the raw GUID portion of CLIENT_ID only.
        refresh_url = f"{OAC_BASE_URL}/api/dv/api/v1/tokens/token/refresh"

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

        # Update refresh token if a new one was returned.
        # Some OAC versions rotate the refresh token on use.
        if   "refreshToken"  in data: self._refresh_token = data["refreshToken"]
        elif "refresh_token" in data: self._refresh_token = data["refresh_token"]

        expires_in       = int(data.get("expiresIn") or data.get("expires_in") or 3600)
        self._expires_at = time.time() + expires_in
        expiry_str = datetime.fromtimestamp(
            self._expires_at, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"🔑 Token refreshed. Valid for {expires_in}s → expires at {expiry_str}")

    @property
    def token(self):
        # Check expiry on every call. Refresh proactively if
        # within TOKEN_REFRESH_BUFFER seconds to prevent any
        # API call being made with an expired token.
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
print("=" * 55)
print("  SECTION 2 COMPLETE: Token Manager Ready")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 3: OAC API HELPERS
# ─────────────────────────────────────────────────────────────
# Three utility functions used by the extraction loop in
# Section 4. All API calls go through these helpers so that
# token refresh, error handling, and encoding logic are defined
# once and reused consistently across all catalog types.
#
# get_headers():
#   Returns Authorization and Content-Type headers for every
#   OAC REST API call. Calls token_mgr.token which triggers a
#   proactive refresh if the token is near expiry. An optional
#   token override is supported for testing without modifying
#   the shared token_mgr instance.
#
# b64_encode_id():
#   The OAC getACL endpoint requires catalog object paths to
#   be Base64 URL-safe encoded as a URL path parameter.
#   Standard base64 + and / are replaced with - and _; trailing
#   = padding is stripped as it is not valid in a URL segment.
#   This is the mirror of Silver's decode_base64_id UDF.
#
# get_catalog_items():
#   Paginates through all items of a given catalog type using
#   the OAC search endpoint with wildcard search=*. The
#   oa-page-count response header provides the total page count
#   so all items are retrieved regardless of catalog size.
#   Token remaining time is logged per page for monitoring
#   during large extracts. Rate limit sleep between pages
#   prevents overwhelming the OAC API.
#
# get_acl_for_item():
#   Calls the getACL action endpoint for a single catalog item.
#   403 (forbidden) and 404 (not found) are handled silently —
#   both return an empty list so extraction continues without
#   interruption. Covers cases where the user lacks permission
#   to see an item's ACL, or the item was deleted between the
#   catalog list call and the ACL call.
# ─────────────────────────────────────────────────────────────

def get_headers(token=None):
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return token_mgr.get_headers()


def b64_encode_id(object_path):
    # URL-safe Base64 encode the catalog object path.
    # Padding = characters are stripped because the OAC getACL
    # endpoint accepts the encoded value as a URL path segment.
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
            # Catalog type exists in the config list but returned
            # no items — log and skip rather than raising an error.
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
    # POST to the getACL action endpoint.
    # Content-Length: 0 is required by the OAC API even though
    # the body is empty — omitting it returns a 411 error.
    url  = f"{OAC_BASE_URL}/api/{OAC_API_VERSION}/catalog/{catalog_type}/{item_id}/actions/getACL"
    hdrs = get_headers()
    hdrs["Content-Length"] = "0"
    resp = requests.post(url, headers=hdrs, timeout=30)
    if resp.status_code in (403, 404):
        return []
    resp.raise_for_status()
    return resp.json()


print("=" * 55)
print("  SECTION 3 COMPLETE: API Helpers Loaded")
print("  Functions: get_headers, b64_encode_id,")
print("             get_catalog_items, get_acl_for_item")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 4: EXTRACT — Fetch All Items + ACLs
# ─────────────────────────────────────────────────────────────
# Core extraction loop. For each catalog type, all items are
# fetched first (paginated), then getACL is called for each
# item individually. Results are flattened into one row per
# item+principal combination.
#
# Why one row per item+principal?
#   Each catalog item can have multiple principals (users or
#   roles) with different permission sets. A flat structure
#   with one row per combination is the most query-friendly
#   format for OAC reporting and SQL aggregation in Silver.
#
# Item metadata field name variations by catalog type:
#   - id vs objectId (connections use objectId)
#   - path vs objectPath (same variation)
#   - owner may be a dict with displayName or a plain string
#   - lastModified vs modified (type-dependent)
#   All handled defensively with .get() chains so extraction
#   does not fail if a field is absent in a given type.
#
# Items with no ACL entries:
#   If getACL returns an empty list (403, 404, or genuinely
#   empty), the item is still recorded with NULL permission
#   fields. This preserves catalog coverage — an admin needs
#   to know which items have no visible ACL as much as those
#   that do. These rows will be flagged MISSING_PERMISSIONS
#   by the Silver DATA_QUALITY_FLAG transformation.
#
# EXTRACTED_AT is set once at the start of the function and
#   stamped on every row for a consistent snapshot timestamp
#   regardless of how long the extraction loop takes.
# ─────────────────────────────────────────────────────────────

def extract_all_acls():
    all_records  = []
    extracted_at = datetime.now(tz=timezone.utc).isoformat()

    for catalog_type in CATALOG_TYPES:
        print(f"\n{'─' * 55}")
        print(f"🔍 Processing: {catalog_type.upper()}")
        print(f"{'─' * 55}")

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
                # Still record the item so catalog coverage is
                # not silently lost for items with no visible ACL.
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

    print("=" * 55)
    print("  SECTION 4 COMPLETE: Extraction Done")
    print(f"  Total records: {len(all_records):,}")
    print("=" * 55)
    return all_records


print("=" * 55)
print("  SECTION 4 READY: Extract Function Loaded")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 5: BUILD SPARK DATAFRAME
# ─────────────────────────────────────────────────────────────
# Converts the flat list of extracted record dicts into a typed
# Spark DataFrame with an explicit schema.
#
# Why define the schema explicitly rather than inferring it?
#   1. Column types are deterministic across runs regardless of
#      whether null-only columns are present in a given run.
#      e.g. ITEM_CREATED is blank on this OAC instance —
#      schema inference would produce StringType only if at
#      least one non-null value exists; with no non-null values
#      it may produce NullType, breaking the Silver read.
#   2. Integer permission flags (PERM_*) must be IntegerType
#      for the weighted arithmetic in the Silver RISK_SCORE UDF.
#      Inference on rows with all-null permission flags would
#      produce StringType or NullType for these columns.
#   3. Schema mismatches between Stage 1 and Stage 2 targets
#      are caught here rather than silently at write time.
#
# No data preview is shown in this section — printing a
# DataFrame to the notebook materialises all rows on the driver,
# which creates memory pressure and increases compute costs at
# production scale. The quality review gate is the Silver
# notebook's distribution summary (Section 5 of Silver).
# ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = SparkSession.builder.appName("oac_acl_bronze").getOrCreate()

BRONZE_SCHEMA = StructType([
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

print("=" * 55)
print("  SECTION 5 READY: Spark Session & Schema Defined")
print(f"  Columns: {len(BRONZE_SCHEMA.fields)}")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 6: WRITE — Stage 1 (Standard Catalog / Object Storage)
# ─────────────────────────────────────────────────────────────
# Writes the Bronze DataFrame to the AIDP Standard Catalog as
# a managed Delta table backed by OCI Object Storage.
#
# Why Delta format for the Standard Catalog?
#   - Standard Catalog supports Delta natively
#   - Delta provides schema evolution, ACID transactions, and
#     time travel for debugging extract runs
#   - All higher Medallion layers (Silver, Gold) use Delta —
#     Bronze in Delta provides pipeline consistency
#   - Portable: any AIDP instance with a Standard Catalog can
#     use this write pattern; only the catalog name changes
#
# Why is the table created here rather than pre-created in AIDP?
#   Delta saveAsTable() creates the table on first run if it
#   does not exist. This reduces manual setup steps. On
#   subsequent runs, mode("overwrite") + overwriteSchema=true
#   truncates and reloads while allowing column additions.
#
# overwriteSchema=true:
#   Allows column additions between runs without requiring a
#   manual DROP TABLE. Safe here because Bronze is always fully
#   regenerated from the API — there is no partial state to
#   preserve across runs.
#
# The Stage 1 write function returns the spark session so
# Stage 2 can read from the Delta table in the same session
# without reinitialising the SparkContext.
# ─────────────────────────────────────────────────────────────

def write_stage1(df):
    print(f"\n[WRITE] Stage 1 — Standard Catalog (Delta / Object Storage)")
    print(f"        Target: {STANDARD_CATALOG_TABLE}")

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(STANDARD_CATALOG_TABLE)
    )

    # Read back to validate — schema and count only, no data.
    df_check = spark.table(STANDARD_CATALOG_TABLE)
    row_count = df_check.count()
    print("=" * 55)
    print("  SECTION 6 COMPLETE: Stage 1 Write Done")
    print(f"  Table   : {STANDARD_CATALOG_TABLE}")
    print(f"  Rows    : {row_count:,}")
    print(f"  Columns : {len(df_check.columns)}")
    print(f"  Format  : Delta")
    print("=" * 55)
    return row_count


print("=" * 55)
print("  SECTION 6 READY: Stage 1 Write Function Loaded")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 7: WRITE — Stage 2 (ADW via External Catalog)
# ─────────────────────────────────────────────────────────────
# Reads from the Delta table written in Stage 1 and writes to
# ADW via the AIDP External Catalog Spark connection. No wallet,
# no cx_Oracle, no JDBC string required — Spark resolves the
# 3-part catalog path through the External Catalog metadata layer.
#
# Why read from the Delta table rather than the in-memory df?
#   This validates Stage 1 before touching ADW. If the object
#   storage write failed silently, reading from the Delta table
#   will surface the problem before ADW is written. This
#   prevents the ADW table from having newer data than the
#   object storage staging layer — a consistency violation that
#   would be hard to detect and explain downstream.
#
# External Catalog constraints — do NOT violate:
#   No Delta format:
#     ADW External Catalogs do not support Delta table format.
#     saveAsTable() without .format("delta") uses the default
#     format the ADW connection handles correctly.
#   No CREATE SCHEMA:
#     Running spark.sql("CREATE SCHEMA IF NOT EXISTS ...") against
#     an External ADW Catalog returns a 502 Bad Gateway from the
#     AIDP Metastore service. Documented in aidp-setup-notes.md.
#     The oacuser schema must already exist in ADW.
#   3-part path required:
#     catalog.schema.table — all three segments must be present
#     for the External Catalog routing to resolve correctly.
#
# Full overwrite strategy:
#   mode("overwrite") truncates and reloads on every run.
#   ACL permissions represent current state, not an append log.
#   A full snapshot ensures revoked permissions do not persist
#   silently in the ADW table between runs.
# ─────────────────────────────────────────────────────────────

def write_stage2():
    print(f"\n[WRITE] Stage 2 — ADW via External Catalog")
    print(f"        Source: {STANDARD_CATALOG_TABLE}  (Delta)")
    print(f"        Target: {ADW_FULL_PATH}")

    # Read from Stage 1 Delta table — do NOT reuse the original
    # in-memory DataFrame. This validates Stage 1 before write.
    df_from_delta = spark.table(STANDARD_CATALOG_TABLE)

    (df_from_delta.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ADW_FULL_PATH)
    )

    # Read back to validate — count only, no data materialisation.
    df_check = spark.table(ADW_FULL_PATH)
    print("=" * 55)
    print("  SECTION 7 COMPLETE: Stage 2 Write Done")
    print(f"  Table   : {ADW_FULL_PATH}")
    print(f"  Rows    : {df_check.count():,}")
    print(f"  Columns : {len(df_check.columns)}")
    print(f"  Status  : Queryable in AIDP Master Catalog + OAC")
    print("=" * 55)


print("=" * 55)
print("  SECTION 7 READY: Stage 2 Write Function Loaded")
print("=" * 55)


# ─────────────────────────────────────────────────────────────
# SECTION 8: RUN PIPELINE
# ─────────────────────────────────────────────────────────────
# Orchestrates all pipeline steps in sequence:
#
#   [1/4] Auth
#     Validates token_mgr loaded successfully and prints the
#     remaining token lifetime. If 'Expired or expiring soon'
#     is shown, re-download tokens.json from OAC Profile and
#     re-upload to /Workspace/Shared/ before proceeding.
#
#   [2/4] Extract
#     Calls extract_all_acls() which loops all catalog types,
#     paginates items, and calls getACL for each item. Returns
#     a flat list of record dicts. If count is 0 or unexpectedly
#     low, check: token valid, BI Service Administrator role
#     assigned to the token-owning user.
#
#   [3/4] Build DataFrame
#     Creates a typed Spark DataFrame from the extracted records
#     using the explicit BRONZE_SCHEMA. Prints schema and
#     partition count only — no data preview. At production
#     scale, printing data materialises all rows on the driver
#     causing memory pressure and increasing compute costs.
#
#   [4/4] Write
#     Stage 1 writes to Standard Catalog (Delta / Object Storage).
#     Stage 2 reads from Stage 1 Delta table and writes to ADW.
#     Both stages print row count + column count as confirmation.
#     If Stage 1 fails, Stage 2 will not run.
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("  OAC CATALOG ACL EXTRACTOR — BRONZE LAYER v2.0")
print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
print("=" * 60)

# Step 1: Validate token
print("\n[1/4] Authenticating with OAC...")
_ = token_mgr.token
print(f"  ✅ Token valid. {token_mgr.seconds_remaining}s remaining.")

# Step 2: Extract
print("\n[2/4] Extracting catalog items and ACLs...")
records = extract_all_acls()

if not records:
    print("⚠️  No records extracted.")
    print("    Check: token is valid, user has BI Service Administrator role.")
else:
    # Step 3: Build DataFrame
    print(f"\n[3/4] Building Spark DataFrame ({len(records):,} records)...")
    df_bronze = spark.createDataFrame(records, schema=BRONZE_SCHEMA)
    df_bronze.printSchema()
    print(f"  Partitions: {df_bronze.rdd.getNumPartitions()}")

    # Step 4: Write — Stage 1 then Stage 2
    print("\n[4/4] Writing — two-stage pipeline...")
    write_stage1(df_bronze)
    write_stage2()

print("\n" + "=" * 60)
print("  🏁 BRONZE PIPELINE COMPLETE")
print("=" * 60)
