#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# -h / --help：仅打印参数说明后退出，不进入抓取流程
case " $* " in
    *" -h "*|*" --help "*)
        if [ -f "./bc_order_export" ]; then
            ./bc_order_export -h
        elif [ -f "./venv/bin/python" ]; then
            ./venv/bin/python ./整合4_bc.py -h
        else
            echo "未找到可运行程序，无法显示帮助。请先运行 安装环境.sh 创建 venv。"
        fi
        exit 0
        ;;
esac

echo "========================================"
echo "BC 平台（betcatpay）订单抓取（代号4-BC）"
echo "========================================"
echo ""
echo "默认抓取代收订单（deposit）；如需抓代付可追加：--mode payout"
echo "可选日期范围：--date-range YYYY-MM-DD YYYY-MM-DD"
echo "程序会使用独立 Chrome 登录环境抓取报表并下载到 data/output/4_bc。"
echo ""

EXIT_CODE=1
if [ -f "./bc_order_export" ]; then
    ./bc_order_export "$@"
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合4_bc.py "$@"
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请先运行 安装环境.sh 创建 venv，或确认 macOS 二进制包完整。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "抓取完成。请到 data/output/4_bc 文件夹查看下载的文件。"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
