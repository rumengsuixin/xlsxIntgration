# 银行流水整合

多业务线的银行流水汇总与订单对账工具，按代号划分。各代号相互独立、单独运行：

| 代号 | 功能 | 源文件目录 | 输出 |
|---|---|---|---|
| 代号1 | 国内银行流水整合 | `data/input/` | `data/output/国内银行汇总.xlsx` |
| 代号2 | 海外银行流水整合（含币种维度、华美银行 PDF） | `data/input/2/` | `data/output/银行汇总.xlsx` |
| 代号3 | 游戏订单匹配（Adyen / 华为 / Google Play） | `data/input/3/` | `data/output/订单匹配结果_{日期}.xlsx` |
| 代号5 | 代付订单对账（IBFYPAY / SUPERPAY / WANGGUYPAY / 话费卡 / EPIN） | `data/input/5/` | `data/output/代付对账结果_{日期}.xlsx` |
| 代号6 | 代收代付对账（Betcat / Cashnewpay） | `data/input/6/` | `data/output/代收代付对账结果_{日期}.xlsx` |

各代号的读取配置、数据流、表结构等细节见 [CLAUDE.md](CLAUDE.md) 与 [docs/](docs/)。

## 目录结构

```text
整合1.py ~ 整合6.py        各代号源码入口
src/bank_integration/      业务代码（config/app 系列）
template/1|2/              代号1/2 汇总模板（必须预先存在）
data/input/               代号1 源文件；input/2 ~ input/6 各代号源文件
data/output/              各代号输出
scripts/pdf_to_excel.py   代号2 华美银行 PDF 转 Excel
tests/                    回归测试
```

## 运行

### 源码运行

```powershell
venv\Scripts\python.exe 整合1.py    # 国内银行流水整合
venv\Scripts\python.exe 整合2.py    # 海外银行流水整合
venv\Scripts\python.exe 整合3.py    # 游戏订单匹配
venv\Scripts\python.exe 整合5.py    # 代付订单对账
venv\Scripts\python.exe 整合6.py    # 代收代付对账
```

### 小白双击

双击对应的 `开始整合1.bat` ~ `开始整合6.bat` 即可（各对应一个代号），详见 [使用说明.txt](使用说明.txt)。

源文件放入对应的 `data\input\<代号>\`（代号1 为 `data\input\`），结果输出到 `data\output\`。

## PDF 转 Excel（代号2 华美银行）

```powershell
venv\Scripts\python.exe scripts\pdf_to_excel.py "华美银行电子对账单-2026.02.pdf"
```

默认输出到 `data/output/{PDF文件名}.xlsx`，优先提取 `DAILY BALANCES` 为 `Daily_Balances` 工作表。仅支持文本型 PDF，扫描件不做 OCR。代号2 整合中华美银行放入 `data/input/2/` 并按 `{公司代号}-华美银行-{币种}.pdf` 命名。

## 打包

### Windows 用户包

```powershell
build_exe.bat
```

打包完成后将 `dist\银行流水整合` 整个文件夹发给用户，用户电脑无需安装 Python。

### macOS 用户包

macOS 二进制不能在 Windows 上直接构建，推荐用 GitHub Actions 云端构建：推送代码后在仓库 Actions 页运行 `Build macOS Binary` 工作流，下载产物解压后把 `bank-integration-mac.zip` 发给 Mac 用户。Mac 用户解压后进入 `bank-integration` 文件夹，双击对应的 `start_*.command` 运行。

本机有 macOS 时也可直接运行 `build_mac.sh`（使用 `bank_integration_mac.spec`）。

## 测试

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```
