#!/bin/bash
# Mac 用户首次使用时运行，自动创建 Python 虚拟环境并安装依赖
cd "$(dirname "$0")"

echo "========================================"
echo "初始化 Mac 运行环境（仅首次需要运行）"
echo "========================================"
echo ""

if ! command -v python3 &> /dev/null; then
    echo "未找到 python3，请先安装 Python 3.9 或更高版本："
    echo "  https://www.python.org/downloads/"
    read -p "按回车键退出"
    exit 1
fi

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "检测到 Python $PYTHON_VER"

echo "正在创建虚拟环境..."
python3 -m venv venv || {
    echo "创建虚拟环境失败。"
    read -p "按回车键退出"
    exit 1
}

echo "正在安装依赖（首次可能需要几分钟）..."
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt || {
    echo "依赖安装失败，请检查网络后重试。"
    read -p "按回车键退出"
    exit 1
}

echo ""
echo "环境初始化完成！"
echo ""
echo "后续使用直接运行对应脚本即可："
echo "  ./开始整合1.sh  —— 国内银行流水整合"
echo "  ./开始整合2.sh  —— 海外银行流水整合"
echo "  ./开始整合3.sh  —— 游戏订单匹配"
echo "  ./开始整合4.sh  —— 后台充值订单浏览器导出"
read -p "按回车键退出"
