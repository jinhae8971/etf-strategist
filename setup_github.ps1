# PowerShell script to setup GitHub repository
$repoUrl = "https://github.com/jinhae8971/etf-strategist.git"
$repoDir = "etf-strategist"

git clone $repoUrl $repoDir
cd $repoDir
git config user.name "ETF Strategist Bot"
git config user.email "bot@etf-strategist.dev"

Write-Host "Repository setup complete"
