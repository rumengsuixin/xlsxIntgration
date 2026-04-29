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

try {
    if (-not (Test-Path -LiteralPath ".\venv\Scripts\python.exe")) {
        throw (T "5pyq5om+5YiwIHZlbnZcU2NyaXB0c1xweXRob24uZXhl77yM6K+35YWI5Yib5bu66Jma5ouf546v5aKD5bm25a6J6KOF5L6d6LWW44CC")
    }

    & ".\venv\Scripts\python.exe" -m pip show pyinstaller 1>$null 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host (T "5q2j5Zyo5a6J6KOFIFB5SW5zdGFsbGVyLi4u")
        & ".\venv\Scripts\python.exe" -m pip install pyinstaller
        if ($LASTEXITCODE -ne 0) {
            throw (T "UHlJbnN0YWxsZXIg5a6J6KOF5aSx6LSl44CC")
        }
    }

    & ".\venv\Scripts\python.exe" -m PyInstaller ".\bank_integration.spec" --clean --noconfirm
    if ($LASTEXITCODE -ne 0) {
        throw (T "5omT5YyF5aSx6LSl44CC")
    }

    Write-Host ""
    Write-Host (T "5omT5YyF5a6M5oiQ77yaZGlzdFzpk7booYzmtYHmsLTmlbTlkIg=")
    Write-Host (T "5oqKIGRpc3Rc6ZO26KGM5rWB5rC05pW05ZCIIOaVtOS4quaWh+S7tuWkueWPkee7meeUqOaIt++8jOeUqOaIt+WPjOWHuyDlvIDlp4vmlbTlkIguYmF0IOWNs+WPr+OAgg==")
    Wait-BeforeExit
    exit 0
} catch {
    Write-Host $_.Exception.Message
    Wait-BeforeExit
    exit 1
}
