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
Write-Host "后台充值订单浏览器导出（代号4）"
Write-Host "========================================"
Write-Host ""
Write-Host "请输入支付日期范围，格式必须为 YYYY-MM-DD。"
Write-Host "程序会使用独立 Chrome 登录环境打开导出链接，并集中下载到 data\output\4。"
Write-Host ""

$exitCode = 1
$exePath = Join-Path $PSScriptRoot "后台订单导出.exe"
if (Test-Path -LiteralPath $exePath) {
    & $exePath
    $exitCode = $LASTEXITCODE
} elseif (Test-Path -LiteralPath ".\venv\Scripts\python.exe") {
    & ".\venv\Scripts\python.exe" ".\整合4.py"
    $exitCode = $LASTEXITCODE
} else {
    Write-Host "未找到可运行程序。请先运行 build_exe.bat 打包，或确认 venv 环境存在。"
    Wait-BeforeExit
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "导出链接已打开。请到 data\output\4 文件夹查看下载的 Excel 文件。"
} else {
    Write-Host "处理过程中出现错误，请查看上方日志后重新运行。"
}

Wait-BeforeExit
exit $exitCode
