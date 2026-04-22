# Parameterized Postgres backup helper for a Windows + WSL Praxis database host.
#
# Streams pg_dump from WSL to a gzip file on the Windows filesystem, verifies
# the dump trailer and gzip integrity, then prunes old backups.

param(
    [string]$WslDistro = $env:PRAXIS_WSL_DISTRO,
    [string]$DatabaseName = $env:PRAXIS_DB_NAME,
    [string]$DatabaseUser = $env:PRAXIS_DB_USER,
    [string]$BackupDir = $env:PRAXIS_BACKUP_DIR,
    [int]$RetainDays = $(if ($env:PRAXIS_BACKUP_RETAIN_DAYS) { [int]$env:PRAXIS_BACKUP_RETAIN_DAYS } else { 14 })
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($WslDistro)) { $WslDistro = 'Ubuntu' }
if ([string]::IsNullOrWhiteSpace($DatabaseName)) { $DatabaseName = 'praxis' }
if ([string]::IsNullOrWhiteSpace($DatabaseUser)) { $DatabaseUser = 'postgres' }
if ([string]::IsNullOrWhiteSpace($BackupDir)) {
    $BackupDir = Join-Path $env:USERPROFILE 'PraxisBackups'
}

New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
$ResolvedBackupDir = (Resolve-Path $BackupDir).Path
$LogFile = Join-Path $ResolvedBackupDir 'backup.log'
$Stamp = Get-Date -Format 'yyyy-MM-dd_HHmmss'
$OutFile = Join-Path $ResolvedBackupDir "$DatabaseName-$Stamp.sql.gz"

function Log {
    param([string]$Message)
    $Line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Host $Line
    Add-Content -Path $LogFile -Value $Line
}

function Convert-ToWslPath {
    param([string]$WindowsPath)
    $Converted = wsl.exe -d $WslDistro -- wslpath -a "$WindowsPath"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($Converted)) {
        throw "Could not convert Windows path to WSL path: $WindowsPath"
    }
    return "$Converted".Trim()
}

try {
    $WslOutFile = Convert-ToWslPath $OutFile
    Log "Starting backup for database '$DatabaseName' -> $OutFile"

    $DumpCommand = "set -euo pipefail; pg_dump --format=plain --clean --if-exists '$DatabaseName' | gzip -9 > '$WslOutFile'"
    wsl.exe -d $WslDistro -u $DatabaseUser -- bash -lc $DumpCommand
    if ($LASTEXITCODE -ne 0) { throw "pg_dump pipeline failed with exit $LASTEXITCODE" }

    $SizeBytes = (Get-Item $OutFile).Length
    if ($SizeBytes -lt 100) { throw "Backup too small ($SizeBytes bytes)" }

    $TrailerCommand = "gunzip -c '$WslOutFile' | grep -c 'PostgreSQL database dump complete'"
    $TrailerPresent = wsl.exe -d $WslDistro -- bash -lc $TrailerCommand 2>$null
    if ("$TrailerPresent".Trim() -eq '0') {
        throw 'Backup missing PostgreSQL dump completion trailer'
    }

    wsl.exe -d $WslDistro -- gzip -t "$WslOutFile"
    if ($LASTEXITCODE -ne 0) { throw 'gzip integrity check failed' }

    $SizeMb = [math]::Round($SizeBytes / 1MB, 2)
    Log "Backup complete and verified: $SizeMb MB"

    $Cutoff = (Get-Date).AddDays(-$RetainDays)
    Get-ChildItem $ResolvedBackupDir -Filter "$DatabaseName-*.sql.gz" |
        Where-Object { $_.LastWriteTime -lt $Cutoff } |
        ForEach-Object {
            Remove-Item $_.FullName -Force
            Log "Pruned $($_.Name)"
        }

    exit 0
} catch {
    Log "FAIL: $_"
    exit 1
}
