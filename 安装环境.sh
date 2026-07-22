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
echo "后续使用双击对应的 .command 即可（未打包时自动回退到 venv 运行）："
echo "  start_domestic.command         —— 国内银行流水整合（代号1）"
echo "  start_overseas.command         —— 海外银行流水整合（代号2）"
echo "  start_orders.command           —— 游戏订单匹配（代号3）"
echo "  start_payout.command           —— 代付订单对账（代号5）"
echo "  start_dual_reconcile.command   —— 代收代付对账（代号6）"
read -p "按回车键退出"
