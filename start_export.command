#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "后台充值订单浏览器导出（代号4）"
echo "========================================"
echo ""
echo "默认导出上一个自然月；手动指定可追加：--date-range YYYY-MM-DD YYYY-MM-DD --wait-seconds N。"
echo "程序会使用独立 Chrome 登录环境打开导出链接，并集中下载到 data/output/4。"
echo ""

EXIT_CODE=1
if [ -f "./recharge_order_export" ]; then
    ./recharge_order_export "$@"
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合4.py "$@"
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请确认 macOS 二进制包完整。"
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
