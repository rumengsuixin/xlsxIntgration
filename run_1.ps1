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
Write-Host "国内银行流水整合（代号1）"
Write-Host "========================================"
Write-Host ""
Write-Host "请把国内银行流水文件放入 data\input\1 文件夹，命名格式：A-中信银行.xlsx"
Write-Host ""

if (-not $NoPause) {
    Read-Host "准备好后按回车开始整合"
}

$exitCode = 1
$exePath = Join-Path $PSScriptRoot "国内银行整合.exe"
if (Test-Path -LiteralPath $exePath) {
    & $exePath
    $exitCode = $LASTEXITCODE
} elseif (Test-Path -LiteralPath ".\venv\Scripts\python.exe") {
    & ".\venv\Scripts\python.exe" ".\整合1.py"
    $exitCode = $LASTEXITCODE
} else {
    Write-Host "未找到可运行程序。请先运行 build_exe.bat 打包，或确认 venv 环境存在。"
    Wait-BeforeExit
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "处理完成。请到 data\output 文件夹查看 国内银行汇总.xlsx"
} else {
    Write-Host "处理过程中出现错误，请查看上方日志后重新运行。"
}

Wait-BeforeExit
exit $exitCode
