# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

国内银行流水数据整合项目，将多家银行的交易流水原始导出文件（CSV/XLS/XLSX）汇总到统一的 Excel 文件中，并更新余额汇总表。

## 常用命令

```powershell
# 运行主整合脚本
venv/Scripts/python.exe 整合.py

# 运行全部回归测试
venv/Scripts/python.exe -m unittest discover -s tests

# 运行单个测试方法
venv/Scripts/python.exe -m unittest tests.test_bank_integration.BankIntegrationSampleTests.test_read_samples_and_extract_latest_balance_by_date

# 安装依赖
venv/Scripts/pip.exe install -r requirements.txt
```

## 目录结构

```
整合.py                     # 入口（调用 src/bank_integration/app.main()）
src/bank_integration/
    config.py               # 路径常量、各银行读取配置、余额列/日期列映射
    scanner.py              # 扫描 data/input/ 中符合命名规范的源文件
    readers.py              # 按银行配置读取 CSV/XLS/XLSX，返回清洗后的 DataFrame
    balances.py             # 提取期末余额、定位/写入余额工作表的行列
    workbook.py             # 从模板复制工作副本、写入明细子表、调用余额更新
    app.py                  # main() 入口，串联各模块流程
template/
    国内银行汇总.xlsx        # 模板文件（必须预先存在）
data/
    input/                  # 放置 {公司代号}-{银行全称}.{扩展名} 源文件
    input/raw/              # 原始未命名样例（不被扫描）
    output/
        国内银行汇总.xlsx    # 运行时从模板复制的工作副本（最终输出）
tests/
    test_bank_integration.py
```

## 数据流

1. `prepare_work_copy`：检查 `data/output/国内银行汇总.xlsx` 年份，若不匹配当前年则从 `template/` 复制并刷新余额表日期
2. `scan_source_files`：在 `data/input/` 扫描 `^([A-Z])-(.+)\.(xls|xlsx|csv)$` 格式文件
3. `read_bank_file`：按 `BANK_READ_CONFIG` 的 header 行偏移和引擎读取，全部列作为字符串保留，农业银行额外过滤非日期格式行
4. `get_last_balance`：按 `BANK_DATE_COL` 遍历取最大日期（同日期取靠后行），非零余额写入
5. `write_all_to_summary`：覆盖写入 `{公司代号}-{银行缩写}` 明细子表，调用 `update_balance_sheet` 更新 `银行余额` 工作表对应单元格

## 各银行读取配置（集中在 config.py）

| 银行 | header（0-indexed） | engine | 格式 | 余额列 | 日期列 |
|---|---|---|---|---|---|
| 工商银行 | 1 | openpyxl | XLSX | 余额 | 交易时间 |
| 中信银行 | 15 | openpyxl | XLSX | 账户余额 | 交易日期 |
| 招商银行 | 12 | openpyxl | XLSX | 余额 | 交易日 |
| 农业银行 | 2 | xlrd | XLS | 账户余额 | 交易时间 |
| 建设银行 | 9 | xlrd | XLS | 余额 | 交易时间 |
| 浦发银行 | 4 | xlrd | XLS | 余额 | 交易日期 |
| 中国银行 | 7 | —（CSV） | CSV | 交易后余额 | 交易日期 |

## 汇总文件工作表结构

- **Sheet9**：公司代号映射（A–V = 22 个公司）
- **银行余额**：A 列=月末日期，C 列=银行名，E 列=公司A，Z 列=公司V
  - 每个月块含 8 家银行行（顺序固定：`BALANCE_BANK_ORDER`）+ 1 行"货币资金合计"
  - D 列=该行公司合计（SUM 公式），货币资金合计行 D–Z 列=各银行列合计
- **{公司代号}-{银行缩写}**：明细子表（如 `B-建行`、`A-招行`）

## 关键约束

- **源文件命名**：`{大写字母}-{银行全称}.{xls|xlsx|csv}`，放入 `data/input/`（不处理 `raw/` 子目录）
- **模板必须存在**：`template/国内银行汇总.xlsx` 缺失时脚本直接退出
- **年份自动刷新**：工作副本年份与当前年不同时，自动从模板重新复制并调用 `refresh_balance_sheet_dates`
- **金额清洗**：余额字段中的逗号分隔符和 `+` 前缀在 `get_last_balance` 中处理
- **中国银行 CSV 列头**：含 `[英文]` 后缀，读取后统一截断为 `[` 前的中文部分
- **农业银行**：含非交易行，用日期格式正则 `\d{4}-\d{2}-\d{2}` 过滤
