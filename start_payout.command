#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "代付订单对账（代号5）"
echo "========================================"
echo ""
echo "请将 admin / IBFYPAY / SUPERPAY / WANGGUYPAY 源文件放入 data/input/5/ 后运行。"
echo ""

EXIT_CODE=1
if [ -f "./payout_order_reconcile" ]; then
    ./payout_order_reconcile
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合5.py
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请确认 macOS 二进制包完整。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "对账完成。请到 data/output 文件夹查看结果文件。"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
