<#
.SYNOPSIS
    Mounts the COE_Allocations OCI bucket as a Windows drive so you can
    drag-and-drop the weekly "Tech COE Allocation Summary*.xlsx" into it.

.DESCRIPTION
    Requires:
      1. rclone   -> https://rclone.org/downloads/  (or: winget install Rclone.Rclone)
      2. WinFsp   -> https://winfsp.dev/rel/         (required for `rclone mount` on Windows)
      3. The [coe] remote configured (see rclone-coe.conf + README.md)

    Leave this window open while you use the drive; Ctrl+C unmounts.

.EXAMPLE
    .\mount-coe.ps1            # mounts to X:
    .\mount-coe.ps1 -Drive M:  # mounts to M:
#>
param(
    [string]$Drive  = "X:",
    # Bucket name that backs the AIDP volume. Confirm in the OCI console
    # (Object Storage > Buckets) or with: rclone lsd coe:
    [string]$Bucket = "<BUCKET_NAME>",
    [string]$Prefix = "COE_Allocations"
)

if ($Bucket -eq "<BUCKET_NAME>") {
    Write-Error "Set -Bucket (or edit the default in this script) to the real OCI bucket name. List them with:  rclone lsd coe:"
    exit 1
}

Write-Host "Mounting coe:$Bucket/$Prefix  ->  $Drive" -ForegroundColor Cyan
Write-Host "Drop files into $Drive\  to upload. Press Ctrl+C to unmount.`n" -ForegroundColor DarkGray

rclone mount "coe:$Bucket/$Prefix" $Drive `
    --vfs-cache-mode writes `
    --dir-cache-time 10s `
    --no-modtime
