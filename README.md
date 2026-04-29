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

## 打包

```powershell
build_exe.bat
```

打包完成后，将 `dist\银行流水整合` 整个文件夹发给用户。用户电脑不需要安装 Python。

## 测试

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```
