#!/bin/bash
# 注意：此脚本必须在 macOS 上运行，PyInstaller 不支持跨平台编译
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="./venv/bin/python"

wait_exit() {
    echo ""
    read -p "按回车键退出"
}

if [ ! -f "$PYTHON" ]; then
    echo "未找到 venv/bin/python，请先在 Mac 上创建虚拟环境："
    echo ""
    echo "  python3 -m venv venv"
    echo "  venv/bin/pip install -r requirements.txt"
    wait_exit
    exit 1
fi

"$PYTHON" -m pip show pyinstaller > /dev/null 2>&1 || {
    echo "正在安装 PyInstaller..."
    "$PYTHON" -m pip install pyinstaller || {
        echo "PyInstaller 安装失败。"
        wait_exit
        exit 1
    }
}

"$PYTHON" -m PyInstaller "./bank_integration_mac.spec" --clean --noconfirm || {
    echo "构建失败。"
    wait_exit
    exit 1
}

echo ""
echo "构建完成：dist/银行流水整合"
echo "构建产物在 dist/银行流水整合 目录，在终端中运行对应的 .sh 脚本即可。"
wait_exit
