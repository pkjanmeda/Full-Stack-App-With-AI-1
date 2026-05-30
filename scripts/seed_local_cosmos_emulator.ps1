param(
    [string]$Endpoint = "https://localhost:8081/",
    [string]$Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
    [switch]$SkipInstall
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")
$loaderPath = Join-Path $repoRoot "services/cosmos-emulator/load_data.py"
$initFilePath = Join-Path $repoRoot "services/cosmos-emulator/cosmos-init.sql"
$requirementsPath = Join-Path $repoRoot "services/cosmos-emulator/requirements.txt"

if (-not (Test-Path $loaderPath)) {
    throw "Loader script not found: $loaderPath"
}
if (-not (Test-Path $initFilePath)) {
    throw "Init SQL file not found: $initFilePath"
}

$pythonExe = Join-Path $repoRoot ".venv/Scripts/python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

if (-not $SkipInstall) {
    & $pythonExe -m pip install -r $requirementsPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install Cosmos loader dependencies."
    }
}

& $pythonExe $loaderPath $initFilePath --endpoint $Endpoint --key $Key
if ($LASTEXITCODE -ne 0) {
    throw "Failed to seed local Cosmos emulator."
}

Write-Host "Local Cosmos emulator seeded successfully."
Write-Host "Containers created/updated: factory_ops.shift_data, factory_ops.kpi_data"
