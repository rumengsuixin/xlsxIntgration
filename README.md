# 国内银行流水整合

将多家银行导出的流水文件写入统一的 `国内银行汇总.xlsx` 工作簿，并更新“银行余额”工作表中对应月份、公司和银行的期末余额。

## 目录结构

```text
src/bank_integration/     业务代码
template/                 汇总模板，必须包含 国内银行汇总.xlsx
data/input/               待处理源文件，命名为 {公司代号}-{银行全称}.{xls|xlsx|csv}
data/input/raw/           原始备份文件，不参与扫描
data/output/              生成或复用的汇总工作簿
tests/                    回归测试
整合.py                   源码运行入口
开始整合.bat              小白用户双击入口
build_exe.bat             开发电脑打包入口
```

## 小白用户使用方法

1. 把银行流水文件放入 `data/input/`。
2. 文件按规则命名，例如 `A-中信银行.xlsx`、`B-招商银行.xlsx`、`C-建设银行.xls`。
3. 双击 `开始整合.bat`。
4. 到 `data/output/` 查看 `国内银行汇总.xlsx`。

支持的银行：招商银行、建设银行、工商银行、中信银行、浦发银行、农业银行、中国银行。

## 开发运行

```powershell
venv\Scripts\python.exe 整合.py
```

## PDF 转 Excel

```powershell
venv\Scripts\python.exe scripts\pdf_to_excel.py "华美银行电子对账单-2026.02.pdf"
```

默认输出到 `data/output/{PDF文件名}.xlsx`。脚本会优先提取 `DAILY BALANCES` 为 `Daily_Balances` 工作表；如果找不到该段落，再退回到普通表格/文本提取。该脚本用于文本型 PDF，扫描件不做 OCR。

代号2整合中，华美银行是 PDF 源文件，放入 `data/input/2/` 并按 `{公司代号}-华美银行-{币种}.pdf` 命名，例如 `A-华美银行-USD.pdf`。程序会读取 `DAILY BALANCES`，只取账单月份最后一条余额写入汇总。

## 打包

### Windows 用户包

```powershell
build_exe.bat
```

打包完成后，将 `dist\银行流水整合` 整个文件夹发给用户。用户电脑不需要安装 Python。

### macOS 用户包

macOS 二进制不能在 Windows 上直接构建。推荐使用 GitHub Actions 云端构建：

1. 推送代码到 GitHub。
2. 打开仓库的 Actions 页面。
3. 运行 `Build macOS Binary` 工作流。
4. 下载产物 `银行流水整合_mac`，解压后把里面的 `银行流水整合_mac.zip` 发给 Mac 用户。

Mac 用户解压后运行 `开始整合1.sh`、`开始整合2.sh` 或 `开始整合3.sh` 即可，不需要安装 Python。

## 测试

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```
