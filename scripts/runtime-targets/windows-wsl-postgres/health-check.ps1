# Parameterized health check for a Windows + WSL Praxis Postgres runtime target.

param(
    [switch]$Quiet,
    [string]$WslDistro = $env:PRAXIS_WSL_DISTRO,
    [string]$DatabaseName = $env:PRAXIS_DB_NAME,
    [string]$DatabaseUser = $env:PRAXIS_DB_USER,
    [int]$DatabasePort = $(if ($env:PRAXIS_DB_PORT) { [int]$env:PRAXIS_DB_PORT } else { 5432 }),
    [string]$FirewallRuleName = $env:PRAXIS_FIREWALL_RULE,
    [string]$BackupDir = $env:PRAXIS_BACKUP_DIR,
    [string]$StatusUrl = $env:PRAXIS_STATUS_URL
)

if ([string]::IsNullOrWhiteSpace($WslDistro)) { $WslDistro = 'Ubuntu' }
if ([string]::IsNullOrWhiteSpace($DatabaseName)) { $DatabaseName = 'praxis' }
if ([string]::IsNullOrWhiteSpace($DatabaseUser)) { $DatabaseUser = 'postgres' }
if ([string]::IsNullOrWhiteSpace($FirewallRuleName)) { $FirewallRuleName = 'Praxis Postgres (LAN)' }
if ([string]::IsNullOrWhiteSpace($BackupDir)) {
    $BackupDir = Join-Path $env:USERPROFILE 'PraxisBackups'
}

$Results = @()
$Failures = 0

function Add-Result {
    param([string]$Name, [string]$Status, [string]$Detail)
    $script:Results += [pscustomobject]@{ Check = $Name; Status = $Status; Detail = $Detail }
    if ($Status -ne 'OK') { $script:Failures++ }
}

function Check {
    param(
        [string]$Name,
        [scriptblock]$Test,
        [string]$OkDetail = 'ok',
        [string]$FailDetail = 'failed'
    )
    try {
        $Value = & $Test
        if ($Value) {
            Add-Result $Name 'OK' $OkDetail
        } else {
            Add-Result $Name 'FAIL' $FailDetail
        }
    } catch {
        Add-Result $Name 'ERROR' "$_"
    }
}

Check 'WSL distro reachable' {
    $null = wsl.exe -d $WslDistro -- true 2>$null
    $LASTEXITCODE -eq 0
} -OkDetail $WslDistro -FailDetail "WSL distro '$WslDistro' is not reachable"

Check 'Postgres accepting connections' {
    $null = wsl.exe -d $WslDistro -u $DatabaseUser -- pg_isready -q -d $DatabaseName 2>$null
    $LASTEXITCODE -eq 0
} -OkDetail "$DatabaseName ready" -FailDetail "Postgres is not ready for '$DatabaseName'"

Check 'pgvector extension installed' {
    $Version = wsl.exe -d $WslDistro -u $DatabaseUser -- psql -d $DatabaseName -tAc "SELECT extversion FROM pg_extension WHERE extname='vector';" 2>$null
    "$Version".Trim() -match '^\d'
} -OkDetail 'vector present' -FailDetail "pgvector is missing in '$DatabaseName'"

Check "Port $DatabasePort listening on Windows" {
    (Get-NetTCPConnection -LocalPort $DatabasePort -State Listen -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0
} -OkDetail "port $DatabasePort listening" -FailDetail "nothing listening on port $DatabasePort"

Check 'Port proxy targets current WSL IP' {
    $WslIp = (wsl.exe -d $WslDistro -- hostname -I 2>$null | Out-String).Trim().Split(' ')[0]
    if (-not $WslIp) { return $false }
    $Proxy = (netsh interface portproxy show v4tov4 2>&1 | Out-String)
    $Proxy -match [regex]::Escape($WslIp)
} -OkDetail 'proxy current' -FailDetail 'port proxy is missing or stale'

Check 'Firewall rule active' {
    $Out = (netsh advfirewall firewall show rule name="$FirewallRuleName" 2>&1 | Out-String)
    ($Out -match 'Enabled:\s*Yes') -and ($Out -match 'Action:\s*Allow')
} -OkDetail $FirewallRuleName -FailDetail "firewall rule '$FirewallRuleName' missing or disabled"

Check 'Disk space above threshold' {
    $FreeGb = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
    $FreeGb -gt 20
} -OkDetail "$([math]::Round((Get-PSDrive C).Free / 1GB, 1)) GB free" -FailDetail 'less than 20 GB free on system drive'

Check 'Recent backup present' {
    if (-not (Test-Path $BackupDir)) { return $false }
    $Latest = Get-ChildItem $BackupDir -Filter "$DatabaseName-*.sql.gz" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $Latest) { return $false }
    (New-TimeSpan -Start $Latest.LastWriteTime -End (Get-Date)).TotalHours -lt 48
} -OkDetail 'backup younger than 48 hours' -FailDetail 'no recent compressed backup found'

if (-not [string]::IsNullOrWhiteSpace($StatusUrl)) {
    Check 'Status endpoint reachable' {
        try {
            $Response = Invoke-WebRequest -UseBasicParsing -Uri $StatusUrl -TimeoutSec 5
            $Response.StatusCode -eq 200
        } catch {
            $false
        }
    } -OkDetail $StatusUrl -FailDetail "status endpoint unreachable: $StatusUrl"
}

if (-not $Quiet) {
    Write-Host ''
    Write-Host '=== Praxis Windows WSL Postgres Health Check ==='
    Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host ''
    foreach ($Result in $Results) {
        Write-Host ("  [{0,-5}] {1,-35} {2}" -f $Result.Status, $Result.Check, $Result.Detail)
    }
    Write-Host ''
    if ($Failures -eq 0) {
        Write-Host 'All configured checks passed.'
    } else {
        Write-Host "$Failures check(s) failed."
    }
}

exit $Failures
