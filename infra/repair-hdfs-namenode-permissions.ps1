# Repairs the Windows ACL that prevents Hadoop NameNode from reading VERSION.
# The script self-elevates because the directory is owned by Administrators.

$principal = New-Object Security.Principal.WindowsPrincipal(
    [Security.Principal.WindowsIdentity]::GetCurrent()
)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$PSCommandPath`""
    )
    exit
}

$nameNodeDir = "C:\hadoop\hadoop-3.2.4\data\namenode"
$account = [Security.Principal.WindowsIdentity]::GetCurrent().Name

if (-not (Test-Path -LiteralPath $nameNodeDir)) {
    throw "NameNode directory was not found: $nameNodeDir"
}

& takeown.exe /F $nameNodeDir /R /D Y
if ($LASTEXITCODE -ne 0) { throw "takeown failed with exit code $LASTEXITCODE" }

& icacls.exe $nameNodeDir /grant:r "$($account):(OI)(CI)F" /T
if ($LASTEXITCODE -ne 0) { throw "icacls failed with exit code $LASTEXITCODE" }

Write-Host "Repaired NameNode access for $account."
Get-Acl (Join-Path $nameNodeDir "current\VERSION") | Format-List Owner, AccessToString
