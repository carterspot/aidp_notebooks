# AIDP Setup Notes — OAC Admin Reporting Project

> Technical decisions, lessons learned, and platform gotchas from the initial build.  
> Author: Carter Beaton · Argano  
> Date: March 2026  
> Audience: AIDP developers and data engineers onboarding to this project

---

## 📋 Project Summary

This project extracts Oracle Analytics Cloud (OAC) Catalog Manager ACL (Access Control List) permissions via the OAC REST API and loads them into an Autonomous Data Warehouse (ADW) table. The resulting dataset powers an OAC admin report showing who has access to which catalog assets (workbooks, folders, datasets, dataflows, and connections).

The pipeline is built as a Python notebook running on a Spark cluster in Oracle AI Data Platform (AIDP) Workbench.

---

## 🏗 Architecture Overview

```
OAC REST API
  (Catalog + getACL endpoints)
          ↓
  AIDP Notebook
  (Python + Spark, Bronze layer)
          ↓
  ADW Table
  arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
          ↓
  OAC Admin Report
  (Dataset → Workbook → Visualizations)
```

---

## 🔐 Authentication — Key Decisions & Lessons Learned

### What We Tried First

**Attempt 1 — OCI API Signing Key**  
Initial assumption was to use an OCI API key attached to the user profile. This approach works for OCI services (Object Storage, ADW provisioning, etc.) but does **not** apply to OAC Catalog/ACL REST APIs. OAC requires a Bearer token, not an OCI request signature.

**Attempt 2 — Resource Principal**  
OAC documentation references Resource Principal authentication. After investigation, Resource Principal applies **only to the OAC Snapshot API** (for OCI Object Storage access). It does not work for Catalog or ACL APIs. Do not pursue this path for catalog operations.

**Attempt 3 — Confidential Application (Client Credentials / Resource Owner)**  
Attempted to create a Confidential Application in the IDCS/IAM domain and use the Resource Owner password grant (`client_id` + `client_secret` + `username` + `password`). This approach is valid but requires:
- Admin access to OCI Console → Identity → Domains → Integrated Applications
- The `CLIENT_SECRET` from the app configuration
- The calling user to be a **non-federated** IDCS/IAM native user

We did not pursue this fully because the `CLIENT_SECRET` was not immediately available.

**✅ Final Approach — OAC Profile tokens.json**  
The simplest and most reliable method requires no IDCS app configuration:
1. OAC Home → Profile → Access Tokens → Download tokens
2. Upload `tokens.json` to AIDP workspace
3. Notebook reads `accessToken` and `refreshToken` from the file
4. Auto-refreshes using OAC's own refresh endpoint

**Critical discovery on token refresh:**  
The token refresh endpoint is **NOT** the IDCS `/oauth2/v1/token` endpoint. Calling IDCS for refresh returns a 401. The correct endpoint is on the OAC hostname:

```
POST https://<oac-hostname>/api/dv/api/v1/tokens/token/refresh
Authorization: Bearer <current_access_token>
Content-Type: text/plain
Body: <refresh_token> (plain text — not JSON, not form-encoded)
```

**Token lifetime:** Both access and refresh tokens expire in ~3600 seconds (1 hour). Re-download `tokens.json` from OAC Profile when expired.

**Token JSON keys:** OAC uses camelCase keys (`accessToken`, `refreshToken`), not snake_case (`access_token`, `refresh_token`). The notebook handles both formats.

---

## 🗄 Catalog & Schema Decisions

### External Catalog vs Standard Catalog

AIDP supports two catalog types:

| Type | Description | Use case |
|---|---|---|
| **External Catalog** | Connects to an existing Oracle Autonomous Database. No data movement — metadata sync only. | Writing to ADW schemas that already exist |
| **Standard Catalog** | For object storage or file uploads. Supports Delta/Iceberg table formats. | Medallion pipelines with object storage |

**Decision:** We use an **External Catalog** (`arganoadw_oacuser_sh`) because the ADW instance and schema already exist and are managed separately.

### Critical Rules for External ADW Catalogs

These will cause hard failures if violated:

| Rule | Why |
|---|---|
| ❌ Do NOT run `CREATE SCHEMA` via Spark SQL | External Catalog metadata is managed by AIDP's Metastore — DDL against the ADW schema via Spark returns a 502 Bad Gateway |
| ❌ Do NOT use `.format("delta")` | ADW does not support Delta table format. Only standard Oracle table formats are supported via External Catalog writes |
| ✅ Schema must pre-exist in ADW | Create the schema (user/schema) in ADW directly before referencing it in AIDP |
| ✅ Use `saveAsTable()` with 3-part name | `catalog.schema.table` — Spark handles the JDBC write through the External Catalog connection |
| ✅ Use `mode("overwrite")` for full refresh | Truncates and reloads on every run — appropriate for a full ACL snapshot |

### 3-Part Naming Convention

All AIDP table references use 3-part naming:
```
catalog_name.schema_name.table_name

Example:
arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
```

### Why We Changed Catalogs Mid-Build

Initially targeted `catalog_manager.catalog_manager.OAC_CATALOG_ACL`. Switched to `arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL` because:
- `oacuser` is the appropriate ADW schema for OAC-related reporting data
- `arganoadw_oacuser_sh` maps to the correct ADW instance for OAC integration
- Keeps OAC admin data separate from the `catalog_manager` schema used for other purposes

---

## 🔌 OAC REST API — Key Findings

### Catalog Types Available
The OAC Catalog API supports the following types:
```
workbooks, folders, datasets, connections, dataflows,
models, sequences, subjectAreas, analysis, reports,
dashboardgroupfolders, dashboardfolders, dashboardpages,
dashboards, scripts
```

We extract: `workbooks`, `folders`, `datasets`, `dataflows`, `connections`

### ACL API Behavior
- The API returns **only items the calling user has permission to access**
- It does not provide an admin view of all objects regardless of ACL
- The calling user must have the **BI Service Administrator** application role to see all catalog items
- Items without ACL entries (access denied or no explicit ACL) are recorded with NULL permission fields

### ID Encoding
All catalog object IDs must be **Base64URL-safe encoded** before calling the `getACL` endpoint:
```python
base64.urlsafe_b64encode(object_path.encode("utf-8")).decode("utf-8").rstrip("=")
```

### Pagination
Results are paged. Use the `oa-page-count` response header to determine total pages:
```
GET /api/20210901/catalog/{type}?search=*&limit=100&page=1
Response headers: oa-page-count, oa-total-items, oa-current-page
```

---

## ⚙️ AIDP Platform — Key Configuration Notes

### Spark Cluster
- Cluster: `cbtest3_cludster`
- Must be **attached to the notebook** before running any code cells
- `SparkSession` is obtained via `SparkSession.builder.appName("...").getOrCreate()`
- `from pyspark.sql import SparkSession` must be inside the function when using notebook cells

### Workspace Structure
- Notebooks stored in: `AIDP_WS_CBTEST3 → Shared`
- `tokens.json` stored in: `/Workspace/Shared/tokens.json`
- Do not store credentials in the catalog

### Resource Principal — Not Applicable Here
Resource Principal auth in AIDP applies to OCI services accessed from notebooks (Object Storage, etc.) — not to OAC Catalog APIs. Do not configure Resource Principal policies for this use case.

---

## 📦 Bronze Layer Design

### Why ADW (not Object Storage) as Bronze

For a large-scale Medallion pipeline, raw data typically lands in OCI Object Storage first. We chose ADW directly as the Bronze layer for this project because:

- Dataset is small (~200 rows per extract)
- No transformation occurs — the ADW table IS the raw data
- OAC connects directly to ADW for reporting — no additional hop needed
- Simpler infrastructure — no bucket permissions, no object storage catalog setup

If this pipeline grows to multiple OAC instances or requires historical trending at scale, introduce Object Storage as the true Bronze landing zone at that point.

### Write Pattern
Full overwrite on every run — no incremental merge. This is appropriate because:
- ACL permissions can change any time (grants, revocations)
- The full dataset is small enough to reload completely
- A clean snapshot is more reliable than incremental delta tracking for a permissions report

---

## 🧰 Requirements

The following Python libraries are used. All are available in the standard AIDP cluster environment or via `requirements.txt`:

```
requests       # OAC REST API calls
pandas         # DataFrame preview before Spark write
base64         # Catalog ID encoding (stdlib)
time           # Rate limiting between API calls (stdlib)
datetime       # Timestamp handling (stdlib)
pyspark        # Spark DataFrame + saveAsTable (provided by cluster)
```

---

## 🔮 Future Considerations

| Item | Notes |
|---|---|
| **Silver layer** | Cleanse and enrich — resolve `ACCOUNT_GUID` to full display names, normalize permission flags, join to OAC user directory |
| **Gold layer** | Aggregated permission summary by user/role for OAC admin report consumption |
| **Scheduling** | Wrap Bronze notebook in an AIDP Workflow, schedule nightly. Token refresh must be solved for unattended runs (tokens.json expires hourly) |
| **Unattended auth** | For scheduled runs, a Confidential App with Resource Owner grant or a service account approach will be needed to avoid manual token refresh |
| **Multiple OAC instances** | Parameterize the notebook with `OAC_BASE_URL` as a Workflow job parameter to run across multiple instances |
| **Historical trending** | Add a run date partition column and switch from `mode("overwrite")` to `mode("append")` to retain history |
| **GitHub integration** | Connect AIDP workspace to GitHub repo for version control. See AIDP Workbench docs for Git integration setup |

---

## 📌 Quick Reference

| Item | Value |
|---|---|
| OAC hostname | `argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com` |
| IDCS domain | `idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com` |
| OAC IDCS App Client ID | `gkligdfeuzql4yw7pb74ka6ecx3rjsga_APPID` |
| Token refresh endpoint | `<oac-hostname>/api/dv/api/v1/tokens/token/refresh` |
| AIDP Workbench | `AIDP_Workbench_cbtest3` |
| Workspace | `AIDP_WS_CBTEST3` |
| Cluster | `cbtest3_cludster` |
| External Catalog | `arganoadw_oacuser_sh` |
| ADW Schema | `oacuser` |
| Bronze Table | `arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL` |
| tokens.json path | `/Workspace/Shared/tokens.json` |
| Bronze notebook | `notebooks/oac-acl-extractor-bronze.ipynb` |
