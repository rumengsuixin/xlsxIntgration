#!/bin/sh
# macOS 首次使用配置脚本
# 配置 git 使用项目自带 hooks，并清除当前 quarantine 属性

set -e

# 1. 配置 git hooks 目录
git config core.hooksPath .githooks
echo "✔ git hooks 已配置为 .githooks/"

# 2. 设置 hooks 可执行权限
chmod +x .githooks/post-checkout .githooks/post-merge
echo "✔ hooks 已设置可执行权限"

# 3. 立即清除当前目录的 quarantine 属性
xattr -dr com.apple.quarantine . 2>/dev/null || true
echo "✔ quarantine 属性已清除"

echo ""
echo "配置完成。之后每次 git pull / git checkout 将自动清除 quarantine 属性。"
