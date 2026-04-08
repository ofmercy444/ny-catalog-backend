param(
  [string]$CommitMessage = "Rework catalog for bundle-parent shoes + detail bundle links",
  [string]$ApiService = "YOUR_API_SERVICE_NAME",
  [string]$WorkerService = "YOUR_WORKER_SERVICE_NAME"
)

$ErrorActionPreference = "Stop"

Set-Location "C:\Users\Daniella\ny-catalog-backend"

Write-Host "==> Git add"
git add server.js crawler.js

Write-Host "==> Git commit (if changes staged)"
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) {
  git commit -m $CommitMessage
} else {
  Write-Host "No staged changes to commit."
}

Write-Host "==> Git push"
git push

Write-Host "==> Railway redeploy API"
railway redeploy --service $ApiService

Write-Host "==> Railway redeploy Worker"
railway redeploy --service $WorkerService

Write-Host "Done. Watch worker logs and test shoes tab."