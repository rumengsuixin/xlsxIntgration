#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "后台充值订单浏览器导出（代号4）"
echo "========================================"
echo ""
echo "请输入支付日期范围，格式必须为 YYYY-MM-DD。"
echo "程序会使用独立 Chrome 登录环境打开导出链接，并集中下载到 data/output/4。"
echo ""

EXIT_CODE=1
if [ -f "./后台订单导出" ]; then
    ./后台订单导出
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合4.py
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请先运行 build_mac.sh 打包，或确认 venv 环境存在。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "导出链接已打开。请到 data/output/4 文件夹查看下载的 Excel 文件。"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
