# Downloads the BGL dataset from the official Loghub record on Zenodo (57.5 MB zip)
# and splits it into monthly files in data\landing\.
# Run from the project folder:   powershell -ExecutionPolicy Bypass -File scripts\download_data.ps1
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"          # makes Invoke-WebRequest much faster
Set-Location (Split-Path $PSScriptRoot -Parent)

$url = "https://zenodo.org/records/8196385/files/BGL.zip?download=1"
New-Item -ItemType Directory -Force -Path data\download | Out-Null
Invoke-WebRequest $url -OutFile data\download\BGL.zip

# A failed download is often an empty or tiny file: stop with a clear message
$size = (Get-Item data\download\BGL.zip).Length
if ($size -lt 50000000) { throw "Download incomplete ($size bytes). Please try again." }

Expand-Archive data\download\BGL.zip -DestinationPath data\download -Force
$log = Get-ChildItem data\download -Recurse -Filter BGL.log | Select-Object -First 1
python scripts\split_by_month.py $log.FullName
