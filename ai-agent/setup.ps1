param(
  [switch]$Reinstall
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (-not (Test-Path ".venv") -or $Reinstall) {
  if ($Reinstall -and (Test-Path ".venv")) {
    Remove-Item -Recurse -Force ".venv"
  }
  python -m venv .venv
}

& ".\.venv\Scripts\Activate.ps1"
python -m pip install --upgrade pip
pip install -r requirements.txt

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "Created ai-agent/.env from .env.example. Add your GROQ_API_KEY before running app.py."
}

python check_env.py

