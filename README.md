# AIDP Notebooks – Argano OAC Admin Reporting

> Oracle AI Data Platform (AIDP) notebook repository for OAC catalog administration and reporting.  
> Maintained by: Carter Beaton · Argano  
> Platform: Oracle AIDP Workbench (`AIDP_Workbench_cbtest3`)

---

## 📋 Overview

This repository contains PySpark/Python notebooks built and executed in Oracle AI Data Platform (AIDP) Workbench. The primary goal is to extract Oracle Analytics Cloud (OAC) catalog metadata — specifically ACL (Access Control List) permissions — and load them into an Autonomous Data Warehouse (ADW) for use in an OAC admin report.

Notebooks follow a **Medallion Architecture** pattern:

| Layer | Purpose | Status |
|---|---|---|
| 🥉 Bronze | Raw API extract, landed as-is into ADW | ✅ Complete |
| 🥈 Silver | Cleanse, enrich, normalize | 🔜 Planned |
| 🥇 Gold | Aggregated, OAC-ready permission summary | 🔜 Planned |

---

## 🗂 Repository Structure

```
aidp-notebooks/
│
├── README.md                          # This file
├── .gitignore                         # Excludes tokens.json and secrets
│
├── notebooks/
│   ├── oac-acl-extractor-bronze.ipynb # Bronze: OAC Catalog ACL extract → ADW
│   ├── oac-acl-transform-silver.ipynb # Silver: Cleanse + enrich (planned)
│   └── oac-acl-report-gold.ipynb      # Gold: Aggregated permission view (planned)
│
└── docs/
    ├── token-refresh-guide.md         # OAC token auth + refresh pattern
    └── aidp-setup-notes.md            # Catalog/schema decisions, key learnings
```

---

## 🚀 Getting Started

### Prerequisites
- Access to Oracle AIDP Workbench (`AIDP_Workbench_cbtest3`)
- OAC instance: `argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com`
- Spark cluster attached to notebook (`cbtest3_cludster`)
- OAC user with **BI Service Administrator** application role
- Fresh `tokens.json` downloaded from OAC Profile (see [Token Refresh Guide](docs/token-refresh-guide.md))

### First-Time Setup
1. Clone this repository into your AIDP workspace
2. Upload `tokens.json` to `/Workspace/Shared/` (see token guide — **never commit this file**)
3. Attach `cbtest3_cludster` to your notebook
4. Run sections top to bottom

---

## 🥉 Bronze Notebook — `oac-acl-extractor-bronze.ipynb`

### What it does
Extracts ACL permissions for all OAC catalog item types via the OAC REST API and loads them as a full snapshot into ADW.

### Source
- **API**: `GET /api/20210901/catalog/{type}` + `POST /api/20210901/catalog/{type}/{id}/actions/getACL`
- **Catalog types**: workbooks, folders, datasets, dataflows, connections

### Target
```
arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL
```

### Output Schema

| Column | Type | Description |
|---|---|---|
| `CATALOG_TYPE` | VARCHAR | workbooks / folders / datasets / dataflows / connections |
| `ITEM_ID` | VARCHAR | Base64-encoded catalog object path |
| `ITEM_NAME` | VARCHAR | Display name of the catalog item |
| `ITEM_PATH` | VARCHAR | Full catalog path (e.g. `/@Catalog/shared/Sales/...`) |
| `ITEM_OWNER` | VARCHAR | Owner email address |
| `ITEM_CREATED` | VARCHAR | ISO 8601 creation timestamp |
| `ITEM_MODIFIED` | VARCHAR | ISO 8601 last modified timestamp |
| `ACCOUNT_GUID` | VARCHAR | User ID or application role name |
| `ACCOUNT_TYPE` | VARCHAR | `User` or `ApplicationRole` |
| `ACCOUNT_NAME` | VARCHAR | Display name of the principal |
| `PERM_READ` | INT | 1 = granted, 0 = denied |
| `PERM_WRITE` | INT | 1 = granted, 0 = denied |
| `PERM_LIST` | INT | 1 = granted, 0 = denied |
| `PERM_DELETE` | INT | 1 = granted, 0 = denied |
| `PERM_CHANGE_PERM` | INT | 1 = granted, 0 = denied |
| `PERM_TAKE_OWN` | INT | 1 = granted, 0 = denied |
| `EXTRACTED_AT` | VARCHAR | ISO 8601 UTC timestamp of extract run |

### Key Design Decisions
- **Auth**: OAC Profile `tokens.json` (access + refresh token). No client_secret required.
- **Token refresh**: Uses OAC's own refresh endpoint (`/api/dv/api/v1/tokens/token/refresh`), not IDCS
- **Write pattern**: Full overwrite on every run — clean snapshot, no incremental merge
- **Write method**: Spark `saveAsTable()` via AIDP External Catalog connection (no Delta format, no wallet)
- **No DDL**: Schema must pre-exist in ADW — `CREATE SCHEMA` cannot be run against an External Catalog

---

## 🔐 Authentication

OAC REST API access uses OAuth 2.0 tokens downloaded directly from the OAC Profile page.

See **[docs/token-refresh-guide.md](docs/token-refresh-guide.md)** for full instructions.

**Critical**: `tokens.json` contains sensitive credentials. It is listed in `.gitignore` and must never be committed to this repository.

---

## 📐 AIDP Platform Details

| Item | Value |
|---|---|
| Workbench instance | `AIDP_Workbench_cbtest3` |
| Workspace | `AIDP_WS_CBTEST3` |
| Cluster | `cbtest3_cludster` |
| OAC hostname | `argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com` |
| IDCS domain | `idcs-55a83f44a5c945af86ee0605a1856068.identity.oraclecloud.com` |
| ADW External Catalog | `arganoadw_oacuser_sh` |
| ADW Schema | `oacuser` |
| Bronze table | `arganoadw_oacuser_sh.oacuser.OAC_CATALOG_ACL` |

---

## 🗒 Notebook Naming Convention

```
{subject}-{layer}.ipynb

Examples:
  oac-acl-extractor-bronze.ipynb
  oac-acl-transform-silver.ipynb
  oac-acl-report-gold.ipynb
  supplier-pipeline-bronze.ipynb
```

---

## 📄 Related Documentation

- [Oracle AIDP Workbench Docs](https://docs.oracle.com/en/cloud/paas/ai-data-platform/)
- [OAC REST API – Catalog Endpoints](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/api-catalog.html)
- [OAC REST API – getACL](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/op-20210901-catalog-type-id-actions-getacl-post.html)
- [OAC OAuth Token Guide](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/obtain-oauth-2.0-token-oracle-analytics-cloud.html)
