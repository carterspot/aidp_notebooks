# OAC Token Refresh Guide

> Authentication reference for OAC REST API access in AIDP notebooks.  
> OAC Instance: `argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com`

---

## Overview

OAC REST API calls require an OAuth 2.0 Bearer token. The simplest and most reliable method is to download `tokens.json` directly from your OAC Profile — no client_secret, no IDCS app configuration required.

Tokens expire in **~3600 seconds (1 hour)**. The notebook's `OACTokenManager` class handles automatic refresh during long-running extracts using OAC's own refresh endpoint.

---

## Step 1 — Enable Developer Options (first time only)

If you don't see the **Access Tokens** tab in your OAC Profile:

1. Go to your OAC home page
2. Click your **name badge** (circle with initials, top right)
3. Click **Profile**
4. Click the **Advanced** tab
5. Click **Enable Developer Options**
6. Click **Save**

---

## Step 2 — Download tokens.json

1. Go to your OAC home page
2. Click your **name badge** → **Profile**
3. Click the **Access Tokens** tab
4. Click **Download tokens**
5. Save the `tokens.json` file

The file contains:
```json
{
  "accessToken": "eyJ...",
  "refreshToken": "eyJ..."
}
```

> ⚠️ **Security**: `tokens.json` contains sensitive credentials.  
> Never commit it to Git. It is listed in `.gitignore`.

---

## Step 3 — Upload to AIDP Workspace

Upload `tokens.json` to your AIDP workspace at:
```
/Workspace/Shared/tokens.json
```

In AIDP Workbench:
- Open your Workspace → Shared folder
- Use the Upload button to upload the file
- Overwrite the existing file if prompted

---

## How the Notebook Uses the Token

The `OACTokenManager` class in Section 2 of each notebook:

1. **Loads** `tokens.json` on startup and decodes the JWT `exp` claim to know exact expiry
2. **Monitors** the token on every API call — if within 300 seconds of expiry, refreshes automatically
3. **Refreshes** using OAC's own refresh endpoint (not IDCS):

```
POST /api/dv/api/v1/tokens/token/refresh
Authorization: Bearer <current_access_token>
Content-Type: text/plain
Body: <refresh_token>
```

> ⚠️ **Important**: The refresh endpoint is on the **OAC hostname**, not the IDCS domain.  
> Using the IDCS `/oauth2/v1/token` endpoint for refresh will return a 401.

---

## Token Expiry Reference

| Token | Lifetime | Notes |
|---|---|---|
| Access Token | ~3600s (1 hour) | Auto-refreshed by notebook |
| Refresh Token | ~3600s (1 hour) | Must re-download `tokens.json` when expired |

> Both tokens expire at roughly the same time. If you see a 401 on refresh,  
> re-download `tokens.json` from OAC Profile and re-upload to the workspace.

---

## Manual Refresh (curl)

If you need to refresh manually outside the notebook:

```bash
# Build token files from tokens.json
python3 -c 'import json; d=json.load(open("tokens.json")); print("Authorization: Bearer "+d["accessToken"])' > token.txt
python3 -c 'import json; d=json.load(open("tokens.json")); print(d["refreshToken"])' > refresh.txt

# Call OAC refresh endpoint
curl -i \
  --header @token.txt \
  --header 'Content-Type: text/plain' \
  --request POST 'https://argano4oracleanalytics-idsmdul6idrs-ia.analytics.ocp.oraclecloud.com/api/dv/api/v1/tokens/token/refresh' \
  --data @refresh.txt
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `401 on refresh` | Both tokens expired | Re-download `tokens.json` from OAC Profile |
| `Token expiring in -Xs` | Stale `tokens.json` uploaded | Re-download and re-upload fresh file |
| `access_token key not found` | OAC uses `accessToken` (camelCase) | Notebook handles both formats automatically |
| `Access Tokens tab missing` | Developer Options not enabled | Profile → Advanced → Enable Developer Options |

---

## .gitignore Entry

Ensure the following is in your `.gitignore`:

```
# OAC Auth tokens — never commit
tokens.json
token.txt
refresh.txt
*.pem
*.key
wallet/
```
