#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# -h / --help：仅打印参数说明后退出，不进入抓取流程
case " $* " in
    *" -h "*|*" --help "*)
        if [ -f "./epin_data_extract" ]; then
            ./epin_data_extract -h
        elif [ -f "./venv/bin/python" ]; then
            ./venv/bin/python ./整合4_epin.py -h
        else
            echo "未找到可运行程序，无法显示帮助。请先运行 安装环境.sh 创建 venv。"
        fi
        exit 0
        ;;
esac

echo "========================================"
echo "1epin.com 数据提取（代号4-EPIN）"
echo "========================================"
echo ""
echo "默认抓订单列表并提取 PIN（--mode all）；其他可选 --mode："
echo "  orders=仅抓订单列表  pins=仅提取PIN码  retry-locked=补抓漏抓订单  payments=付款订单列表"
echo "程序会使用独立 Chrome 登录环境抓取并输出到 data/output/4_epin。"
echo ""

EXIT_CODE=1
if [ -f "./epin_data_extract" ]; then
    ./epin_data_extract "$@"
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合4_epin.py "$@"
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请先运行 安装环境.sh 创建 venv，或确认 macOS 二进制包完整。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "提取完成。请到 data/output/4_epin 文件夹查看输出文件。"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
