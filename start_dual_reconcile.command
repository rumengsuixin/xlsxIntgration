#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "代收代付对账（代号6）"
echo "========================================"
echo ""
echo "请将以下源文件放入 data/input/6/ 后运行："
echo "  Admin收款订单明细*.xlsx / Admin兑换订单明细*.xlsx"
echo "  betcat-payment_*.csv / betcat-payout_*.csv"
echo "  Cashnewpay收款明细*.xlsx / Cashnewpay兑换明细*.xlsx"
echo "  Goldenpay收款明细*.xlsx / Goldenpay兑换明细*.xlsx"
echo ""

read -p "准备好后按回车开始对账"

EXIT_CODE=1
if [ -f "./collection_payout_reconcile" ]; then
    ./collection_payout_reconcile
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合6.py
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请先运行 安装环境.sh 创建 venv，或确认 macOS 二进制包完整。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "对账完成。请到 data/output 文件夹查看 代收代付对账结果_YYYYMMDD.xlsx"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
