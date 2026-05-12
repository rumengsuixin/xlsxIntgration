#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "游戏订单支付方式匹配（代号3）"
echo "========================================"
echo ""
echo "请把订单文件放入 data/input/3 文件夹："
echo "  admin 开头：Admin 订单主表"
echo "  adyen- 开头：Adyen 平台报告"
echo "  华为 开头：华为平台报告"
echo "  google- 或 googol- 开头：Google Play 报告"
echo ""

read -p "准备好后按回车开始匹配"

EXIT_CODE=1
if [ -f "./order_payment_match" ]; then
    ./order_payment_match
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合3.py
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请确认 macOS 二进制包完整。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "处理完成。请到 data/output 文件夹查看 订单匹配结果_YYYYMMDD.xlsx"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
