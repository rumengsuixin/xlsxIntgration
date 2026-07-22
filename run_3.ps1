param([switch]$NoPause)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location -LiteralPath $PSScriptRoot

function Wait-BeforeExit {
    if (-not $NoPause) {
        Write-Host ""
        Read-Host "按回车键退出"
    }
}

Write-Host "========================================"
Write-Host "游戏订单支付方式匹配（代号3）"
Write-Host "========================================"
Write-Host ""
Write-Host "请把订单文件放入 data\input\3 文件夹："
Write-Host "  admin 开头：Admin 订单主表"
Write-Host "  adyen- 开头：Adyen 平台报告"
Write-Host "  华为 开头：华为平台报告"
Write-Host "  google- 或 googol- 开头：Google Play 报告"
Write-Host ""

if (-not $NoPause) {
    Read-Host "准备好后按回车开始匹配"
}

$exitCode = 1
$exePath = Join-Path $PSScriptRoot "游戏订单匹配.exe"
if (Test-Path -LiteralPath $exePath) {
    & $exePath
    $exitCode = $LASTEXITCODE
} elseif (Test-Path -LiteralPath ".\venv\Scripts\python.exe") {
    & ".\venv\Scripts\python.exe" ".\整合3.py"
    $exitCode = $LASTEXITCODE
} else {
    Write-Host "未找到可运行程序。请先运行 build_exe.bat 打包，或确认 venv 环境存在。"
    Wait-BeforeExit
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "处理完成。请到 data\output 文件夹查看 订单匹配结果_YYYYMMDD.xlsx"
} else {
    Write-Host "处理过程中出现错误，请查看上方日志后重新运行。"
}

Wait-BeforeExit
exit $exitCode
