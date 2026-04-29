param(
    [switch]$NoPause
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location -LiteralPath $PSScriptRoot

function Wait-BeforeExit {
    if (-not $NoPause) {
        Write-Host ""
        Read-Host (T "5oyJ5Zue6L2m6ZSu5YWz6Zet56qX5Y+j")
    }
}

function T([string]$Base64Text) {
    return [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($Base64Text))
}

function Find-PackagedExe {
    $rootExe = Get-ChildItem -LiteralPath "." -Filter "*.exe" -File -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($rootExe) {
        return $rootExe.FullName
    }

    if (Test-Path -LiteralPath ".\dist") {
        $distExe = Get-ChildItem -LiteralPath ".\dist" -Filter "*.exe" -File -Recurse -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($distExe) {
            return $distExe.FullName
        }
    }

    return $null
}

Write-Host "========================================"
Write-Host (T "5Zu95YaF6ZO26KGM5rWB5rC05pW05ZCI5bel5YW3")
Write-Host "========================================"
Write-Host ""
Write-Host (T "6K+356Gu6K6k6ZO26KGM5rWB5rC05paH5Lu25bey57uP5pS+5YWlIGRhdGFcaW5wdXQg5paH5Lu25aS544CC")
Write-Host (T "5paH5Lu25ZG95ZCN56S65L6L77yaQS3kuK3kv6Hpk7booYwueGxzeOOAgUIt5oub5ZWG6ZO26KGMLnhsc3jjgIFDLeW7uuiuvumTtuihjC54bHM=")
Write-Host ""

if (-not $NoPause) {
    Read-Host (T "56Gu6K6k5ZCO5oyJ5Zue6L2m6ZSu5byA5aeL5pW05ZCI")
}

$exitCode = 1
$exePath = Find-PackagedExe
if ($exePath) {
    & $exePath
    $exitCode = $LASTEXITCODE
} elseif (Test-Path -LiteralPath ".\venv\Scripts\python.exe") {
    & ".\venv\Scripts\python.exe" ".\整合.py"
    $exitCode = $LASTEXITCODE
} else {
    Write-Host (T "5pyq5om+5Yiw5Y+v6L+Q6KGM56iL5bqP44CC")
    Write-Host (T "6K+356Gu6K6k5b2T5YmN5paH5Lu25aS55Lit5pyJIOmTtuihjOa1geawtOaVtOWQiC5leGXvvIzmiJblhYjlnKjlvIDlj5HnlLXohJHmiafooYwgYnVpbGRfZXhlLmJhdCDnlJ/miJDnqIvluo/jgII=")
    Wait-BeforeExit
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host (T "5aSE55CG5a6M5oiQ44CC6K+35YiwIGRhdGFcb3V0cHV0IOaWh+S7tuWkueafpeeciyDlm73lhoXpk7booYzmsYfmgLsueGxzeA==")
} else {
    Write-Host (T "5aSE55CG6L+H56iL5Lit5Ye6546w6Zeu6aKY77yM6K+35p+l55yL5LiK5pa55o+Q56S65bm25L+u5q2j5ZCO6YeN5paw6L+Q6KGM44CC")
}

Wait-BeforeExit
exit $exitCode
