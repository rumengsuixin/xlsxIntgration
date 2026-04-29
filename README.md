# 国内银行流水整合

将多家银行导出的流水文件写入统一的 `国内银行汇总.xlsx` 工作簿，并更新 `银行余额` 工作表中对应月份、公司和银行的期末余额。

## 目录结构

```text
src/bank_integration/     业务代码
data/input/               待处理源文件，命名为 {公司代号}-{银行全称}.{扩展名}
data/input/raw/           未加公司前缀的原始文件，不参与扫描
data/output/              生成或复用的工作副本
template/                 汇总模板
tests/                    回归测试
整合.py                   兼容入口
```

## 使用方法

1. 将模板文件放在 `template/国内银行汇总.xlsx`。
2. 将待处理源文件放在 `data/input/`，例如 `B-建设银行.xls`、`A-招商银行.xlsx`。
3. 在项目根目录运行：

```powershell
venv\Scripts\python.exe 整合.py
```

结果会写入：

```text
data/output/国内银行汇总.xlsx
```

## 测试

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```
