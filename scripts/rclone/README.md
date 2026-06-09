# COE Allocations — drag-and-drop upload via rclone

Mounts the OCI Object Storage bucket behind the AIDP volume
`/Volumes/cbtest_standard_catalog/default/COE_Allocations/` as a Windows
drive letter. Drop the weekly `Tech COE Allocation Summary*.xlsx` into that
drive and the bronze notebook auto-picks it up on the next run — no renaming.

## One-time setup

### 1. Install the tools
```powershell
winget install Rclone.Rclone     # the sync tool
# WinFsp is REQUIRED for `rclone mount` on Windows:
winget install WinFsp.WinFsp     # or download from https://winfsp.dev/rel/
```

### 2. Create an OCI API signing key
This authenticates rclone to OCI as you (user principal).

1. OCI Console → profile icon (top-right) → **My profile** → **API keys** → **Add API key**.
2. Choose **Generate API key pair**, **download the private key**, click **Add**.
3. OCI shows a **Configuration file preview** — copy it into `C:\Users\<you>\.oci\config`.
4. Save the downloaded private key as `C:\Users\<you>\.oci\oci_api_key.pem` and make sure
   the `key_file=` line in `config` points to it.

Your `~/.oci/config` will look like:
```
[DEFAULT]
user=ocid1.user.oc1..xxxx
fingerprint=aa:bb:cc:...
tenancy=ocid1.tenancy.oc1..xxxx
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
```

### 3. Fill in the rclone remote
Find your rclone config file: `rclone config file`, then paste the `[coe]` block
from [rclone-coe.conf](./rclone-coe.conf) into it and replace the placeholders:

| Placeholder | Where to find it |
|---|---|
| `<OBJECT_STORAGE_NAMESPACE>` | OCI Console → Object Storage → any bucket → **Namespace** field. Or run `oci os ns get`. |
| `<COMPARTMENT_OCID>` | OCI Console → Identity → Compartments → the compartment holding the bucket → copy **OCID**. |
| `<REGION>` | Your home region, e.g. `us-ashburn-1` (same as in `~/.oci/config`). |

### 4. Verify it works
```powershell
rclone lsd coe:                 # should list buckets — note the COE bucket name
rclone ls  coe:<BUCKET_NAME>/COE_Allocations   # should list current files
```

## Weekly use
```powershell
# from this folder:
.\mount-coe.ps1 -Bucket <BUCKET_NAME>
```
A drive (default `X:`) appears. Drag the new `.xlsx` into `X:\`. That's it —
the file lands in the bucket and the notebook will select it as the newest.
Press `Ctrl+C` in the window to unmount.

> Tip: edit the `-Bucket` default in [mount-coe.ps1](./mount-coe.ps1) so you can
> just double-click / run `.\mount-coe.ps1` with no arguments. You can also create
> a Scheduled Task or shortcut to auto-mount at logon.

## Security notes
- The private key in `~/.oci/oci_api_key.pem` grants your OCI permissions — keep it
  local, never commit it. (`.oci/` is outside this repo by design.)
- rclone grants whatever your OCI user can do. If you want upload-only, ask your OCI
  admin for a scoped IAM policy or use a write-only Pre-Authenticated Request instead.
