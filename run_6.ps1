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
        Read-Host "按回车键退出"
    }
}

Write-Host "========================================"
Write-Host "代收代付对账（代号6）"
Write-Host "========================================"
Write-Host ""
Write-Host "请将 admin收款/兑换、Betcat、Cashnewpay、Goldenpay 源文件放入 data\input\6\ 后运行。"
Write-Host ""

$exitCode = 1
$exePath = Join-Path $PSScriptRoot "代收代付对账.exe"
if (Test-Path -LiteralPath $exePath) {
    & $exePath
    $exitCode = $LASTEXITCODE
} elseif (Test-Path -LiteralPath ".\venv\Scripts\python.exe") {
    & ".\venv\Scripts\python.exe" ".\整合6.py"
    $exitCode = $LASTEXITCODE
} else {
    Write-Host "未找到可运行程序。请先运行 build_exe.bat 打包，或确认 venv 环境存在。"
    Wait-BeforeExit
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "对账完成。请到 data\output 文件夹查看结果文件。"
} else {
    Write-Host "处理过程中出现错误，请查看上方日志后重新运行。"
}

Wait-BeforeExit
exit $exitCode
