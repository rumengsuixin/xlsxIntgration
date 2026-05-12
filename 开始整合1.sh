#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "国内银行流水整合（代号1）"
echo "========================================"
echo ""
echo "请把国内银行流水文件放入 data/input/1 文件夹，命名格式：A-中信银行.xlsx"
echo ""

read -p "准备好后按回车开始整合"

EXIT_CODE=1
if [ -f "./国内银行整合" ]; then
    ./国内银行整合
    EXIT_CODE=$?
elif [ -f "./venv/bin/python" ]; then
    ./venv/bin/python ./整合1.py
    EXIT_CODE=$?
else
    echo "未找到可运行程序。请先运行 build_mac.sh 打包，或确认 venv 环境存在。"
    read -p "按回车键退出"
    exit 1
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "处理完成。请到 data/output 文件夹查看 国内银行汇总.xlsx"
else
    echo "处理过程中出现错误，请查看上方日志后重新运行。"
fi

read -p "按回车键退出"
exit $EXIT_CODE
