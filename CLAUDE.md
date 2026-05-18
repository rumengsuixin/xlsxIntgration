# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

银行流水数据整合项目，分为三条业务线：
- **代号1**：国内银行，将多家银行的交易流水原始导出文件（CSV/XLS/XLSX）汇总到 `国内银行汇总.xlsx`
- **代号2**：海外银行，源文件命名携带币种，汇总到 `银行汇总.xlsx`，余额表每行代表"银行+币种"组合
- **代号3**：游戏订单匹配，将游戏方后台 admin 订单表与 Adyen / 华为 / Google Play 商户平台订单通过流水号关联，追加「平台支付方式」列后输出到 `订单匹配结果_{YYYYMMDD}.xlsx`
- **代号4**：后台充值订单浏览器导出，按支付日期逐日打开导出 URL，复用独立 Chrome 登录态并集中下载到 `data/output/4/`
- **代号5**：代付订单对账，将 admin 代付订单与 IBFYPAY / SUPERPAY / WANGGUYPAY 三平台通过订单号关联，输出到 `代付对账结果_{YYYYMMDD}.xlsx`

## 常用命令

```powershell
# 代号1：国内银行整合
venv/Scripts/python.exe 整合1.py
# （或兼容旧入口）
venv/Scripts/python.exe 整合.py

# 代号2：海外银行整合
venv/Scripts/python.exe 整合2.py

# 代号3：游戏订单支付方式匹配
venv/Scripts/python.exe 整合3.py

# 代号4：后台充值订单浏览器导出
venv/Scripts/python.exe 整合4.py

# 代号5：代付订单对账
venv/Scripts/python.exe 整合5.py
# 手动指定日期范围
venv/Scripts/python.exe 整合4.py --date-range 2026-04-01 2026-04-30
# 手动指定日期范围和每批等待秒数
venv/Scripts/python.exe 整合4.py --date-range 2026-04-01 2026-04-30 --wait-seconds 60
# 或在 .env 中设置默认等待时间：MODE4_BATCH_WAIT_SECONDS=60
# 单批导出链接数量：MODE4_BATCH_SIZE=5
# 每批下载不完整时自动重试次数：MODE4_RETRY_LIMIT=3
# 检查缺失文件机会次数：MODE4_MISSING_CHECK_CHANCES=10
# 检查缺失文件间隔秒数：MODE4_CHECK_INTERVAL_SECONDS=2

# 运行全部回归测试
venv/Scripts/python.exe -m unittest discover -s tests

# 运行单个测试方法
venv/Scripts/python.exe -m unittest tests.test_bank_integration.BankIntegrationSampleTests.test_read_samples_and_extract_latest_balance_by_date

# 安装依赖
venv/Scripts/pip.exe install -r requirements.txt
```

## 目录结构

```
整合.py                     # 代号1兼容入口（同 整合1.py）
整合1.py                    # 代号1入口（调用 src/bank_integration/app.main()）
整合2.py                    # 代号2入口（调用 src/bank_integration/app2.main()）
整合3.py                    # 代号3入口（调用 src/bank_integration/app3.main()）
整合4.py                    # 代号4入口（调用 src/bank_integration/app4.main()）
整合5.py                    # 代号5入口（调用 src/bank_integration/app5.main()）
src/bank_integration/
    config.py               # 代号1：路径常量、各银行读取配置、余额列/日期列映射
    config2.py              # 代号2：路径常量、各银行读取配置、余额列/日期列映射
    config3.py              # 代号3：路径常量、各平台列名映射
    config4.py              # 代号4：导出URL模板、Chrome profile和下载目录
    config5.py              # 代号5：路径常量、各平台列名映射、文件识别前缀、输出新增列定义
    scanner.py              # 扫描源文件目录（scan_source_files / scan_source_files_2）
    readers.py              # 按银行配置读取 CSV/XLS/XLSX，返回清洗后的 DataFrame
    balances.py             # 提取期末余额、定位/写入余额工作表的行列
    workbook.py             # 从模板复制工作副本、写入明细子表、调用余额更新
    app.py                  # 代号1 main()，串联各模块流程
    app2.py                 # 代号2 main()，串联各模块流程
    app3.py                 # 代号3 main()，游戏订单匹配逻辑
    app4.py                 # 代号4 main()，充值订单浏览器导出逻辑
    app5.py                 # 代号5 main()，代付订单对账逻辑
template/
    1/
        国内银行汇总.xlsx   # 代号1模板（必须预先存在）
    2/
        银行汇总.xlsx       # 代号2模板（必须预先存在）
data/
    input/                  # 代号1：{公司代号}-{银行全称}.{扩展名} 源文件
    input/2/                # 代号2：{公司代号}-{银行全称}-{币种}.{扩展名} 源文件
    input/3/                # 代号3：admin + ADYEN- + 华为平台- + Googol- 源文件
    input/raw/              # 原始未命名样例（不被扫描）
    input/raw/5/            # 代号5：admin- / ibf平台 / superpay- / wangupay- 源文件
    browser_profile/4/      # 代号4：独立 Chrome 登录态目录（运行时生成）
    output/
        国内银行汇总.xlsx           # 代号1运行时从模板复制的工作副本（最终输出）
        银行汇总.xlsx               # 代号2运行时从模板复制的工作副本（最终输出）
        订单匹配结果_{YYYYMMDD}.xlsx # 代号3运行时生成（每日覆盖）
        4/                          # 代号4浏览器下载目录
        代付对账结果_{YYYYMMDD}.xlsx # 代号5运行时生成（每日覆盖）
tests/
    test_bank_integration.py
```

## 代号1数据流

1. `prepare_work_copy`：检查 `data/output/国内银行汇总.xlsx` 年份，若不匹配当前年则从 `template/1/` 复制并刷新余额表日期
2. `scan_source_files`：在 `data/input/` 扫描 `^([A-Z])-(.+)\.(xls|xlsx|csv)$` 格式文件
3. `read_bank_file`：按 `BANK_READ_CONFIG` 的 header 行偏移和引擎读取，全部列作为字符串保留，农业银行额外过滤非日期格式行
4. `get_monthly_balances`：按月提取期末余额（同月取最新日期行）
5. `write_all_to_summary`：覆盖写入 `{公司代号}-{银行缩写}` 明细子表，调用 `update_balance_sheet` 更新 `银行余额` 工作表对应单元格

## 代号2数据流

1. `prepare_work_copy(template_path=TEMPLATE_PATH_2, output_path=OUTPUT_PATH_2, ...)`：检查 `data/output/银行汇总.xlsx` 年份，若不匹配则从 `template/2/` 复制并刷新余额表日期
2. `scan_source_files_2`：在 `data/input/2/` 扫描 `^([A-Z])-(.+)-([A-Z]{2,4})\.(xls|xlsx|csv|pdf)$` 格式文件，返回含 `currency` 字段的列表；PDF 仅支持华美银行
3. `read_bank_file`：传入 `BANK_READ_CONFIG_2` / `BANK_DATE_COL_2`；支持额外选项：`encoding`、`col_map`、`strip_col_suffix_char`、`row_filter_col`/`row_filter_prefix`/`row_filter_val`，华美银行 PDF 读取 `DAILY BALANCES`
4. `get_monthly_balances`：传入 `BANK_BALANCE_COL_2` / `BANK_DATE_COL_2` 按月提取期末余额
5. `write_all_to_summary_2`：覆盖写入 `{公司代号}-{银行缩写}-{币种}` 明细子表（如 `A-东亚-HKD`），调用 `update_balance_sheet_2` 更新余额工作表

## 代号4数据流

1. 无参数时自动取上一个自然月；手动指定时使用 `--date-range START END`，严格校验 `YYYY-MM-DD`
2. 按日期区间逐日生成 URL：当天同时作为 `pay_sdate` 和 `pay_edate`
3. `p=[PAGE]` 按需求原样保留，不做分页替换
4. 查找 Google Chrome，使用 `data/browser_profile/4/` 作为独立用户目录
5. 更新 Chrome `Default/Preferences`，将下载目录设置为 `data/output/4/`
6. 若独立 profile 尚无 `Default/Cookies` 数据，先打开固定登录页 `https://aim1.567okey.com/Public/login.html` 让用户登录并等待回车，再打开所有导出 URL
7. 每批打开导出 URL 前先检查对应日期文件是否已存在，已存在日期直接跳过，只打开缺失日期 URL
8. 按批次等待下载完成；`--wait-seconds` / `MODE4_BATCH_WAIT_SECONDS` 保留兼容，实际重试由检查机会和检查间隔控制，机会用完才重试
9. 全部日期齐备后，按日期选择最新完成文件并合并为 `data/output/4/后台充值订单导出合并_{start}_{end}.xlsx`
10. 合并读取 `.xls` 时若 `xlrd` 无法直接读取，会尝试用 LibreOffice 或 Windows Excel 临时转换为 `.xlsx` 后再读取

## 代号5数据流

1. `scan_source_files_5`：在 `data/input/raw/5/` 扫描 `.xls`/`.xlsx`，按 stem 小写前缀分类（跳过 `~$` 临时文件）
2. `read_admin_5`：engine=xlrd，sheet="Simple"，找不到时回退查找含 `订单号` 列的 sheet；`dropna+fillna`
3. `read_ibfpay_5`：自动检测资金流水账格式（有 `类型` 列，两行合并）或订单明细格式（无 `类型` 列，手续费置0）
4. `read_superpay_5` / `read_wangguypay_5`：`_select_sheet_by_columns_5` 自动定位含必要列的 sheet
5. `build_*_lookup_5`：三平台各构建以关联键为索引的查找表，去重并打 warning
6. `enrich_admin_5`：admin 为主表，优先级 IBFYPAY > SUPERPAY > WANGGUYPAY 逐行匹配，追加 7 个新增列；末尾追加平台多余行（`机构` 填平台名称）
7. `write_output_5`：输出 3 个 sheet 到 `data/output/代付对账结果_{YYYYMMDD}.xlsx`

## 各平台读取配置——代号5（config5.py）

| 平台 | 格式 | sheet | header | 关联键（平台侧） | 关联键（admin侧） | 特殊处理 |
|---|---|---|---|---|---|---|
| Admin | XLS | Simple | 0 | — | — | engine=xlrd；sheet 不存在时回退查找 |
| IBFYPAY（资金流水账） | XLSX | Sheet | 0 | 系统流水号 | 第三方订单号 | 两行合并：代付扣款+代付扣除手续费 |
| IBFYPAY（订单明细） | XLSX | Sheet | 0 | 系统流水号 | 第三方订单号 | 无 `类型` 列时自动识别，手续费置0 |
| SUPERPAY | XLSX | sheet1 | 0 | 商户订单号 | 订单号 | 手续费=代付金额−实收（取绝对值） |
| WANGGUYPAY | XLSX | 付款订单 | 1 | 商户订单号 | 订单号 | header=1（第1行为冗余标题） |

## 代号5输出结构

输出文件：`data/output/代付对账结果_{YYYYMMDD}.xlsx`，含 3 个 sheet：

| Sheet | 内容 |
|---|---|
| 代付对账结果 | admin 全部原始列 + 7 个新增列（是否匹配/平台流水号/平台代付金额/平台状态/手续费/到账金额/交易日期） |
| 匹配失败订单 | `是否匹配 == "否"` 的行 |
| 平台汇总 | 按 `机构` 聚合：笔数/代付金额合计/手续费合计/到账金额合计（仅统计匹配成功行） |

## 各银行读取配置——代号1（config.py）

| 银行 | header（0-indexed） | engine | 格式 | 余额列 | 日期列 |
|---|---|---|---|---|---|
| 工商银行 | 1 | openpyxl | XLSX | 余额 | 交易时间 |
| 中信银行 | 15 | openpyxl | XLSX | 账户余额 | 交易日期 |
| 招商银行 | 12 | openpyxl | XLSX | 余额 | 交易日 |
| 农业银行 | 2 | xlrd | XLS | 账户余额 | 交易时间 |
| 建设银行 | 9 | xlrd | XLS | 余额 | 交易时间 |
| 浦发银行 | 4 | xlrd | XLS | 余额 | 交易日期 |
| 中国银行 | 7 | —（CSV） | CSV | 交易后余额 | 交易日期 |

## 各银行读取配置——代号2（config2.py）

| 银行 | header | 格式 | 余额列 | 日期列 | 特殊处理 |
|---|---|---|---|---|---|
| 汇丰银行 | 1 | CSV | 账面结余（前缀匹配） | 日期 | `strip_col_suffix_char="("` |
| 东亚银行 | 0 | CSV | 存入金额 | 日期及时间 | `row_filter_prefix="总结余"`；余额行的 `日期及时间` 含嵌入日期（`总结余（截至 YYYY年M月D日)`），`_parse_date_str` 用 `re.search` 自动提取 |
| 华侨银行 | 0 | CSV (GBK) | 余额 | 交易日期 | `encoding="gbk"`；`col_map` 仅在目标列不存在时重命名，避免重复 `交易日期` |
| 渣打银行空中云汇 | 0 | XLSX | Account Balance | Time | — |
| 华美银行 | 0 | PDF | Amount | Date | 读取 `DAILY BALANCES`，只保留账单月份最后一条余额 |
| 大华银行（UOB) | 3 | XLSX | Ledger Balance | Value Date | `row_filter_val="D2"` |
| 联昌国际银行（CIMB） | 5 | XLSX | Balance | Transaction Date | — |
| 招商银行 | 12 | XLSX | 余额 | 交易日 | — |
| 工商银行 | 1 | XLSX | 余额 | 交易时间 | 列不存在时自动跳过余额提取 |

## 代号1汇总文件工作表结构

- **Sheet9**：公司代号映射（A–V = 22 个公司）
- **银行余额**：A 列=月末日期，C 列=银行名，E 列=公司A，Z 列=公司V
  - 每个月块含 8 家银行行（顺序固定：`BALANCE_BANK_ORDER`）+ 1 行"货币资金合计"
  - D 列=该行公司合计（SUM 公式）
- **{公司代号}-{银行缩写}**：明细子表（如 `B-建行`、`A-招行`）

## 代号2汇总文件工作表结构

- **MIG银行余额（YYYYMMDD）**：余额工作表，名称以 `MIG银行余额` 开头（含年份后缀）
  - A 列=月末日期，C 列=银行名，F 列=币种，G–U 列=公司A–O（共15个公司）
  - D 列=折合人民币，E 列=合计
  - 每个月块固定 16 行（`BALANCE_BLOCK_SIZE_2 = 16`），无合计汇总行
- **汇率**：汇率参考表（受保护，不写入）
- **{公司代号}-{银行缩写}-{币种}**：明细子表（如 `A-东亚-HKD`、`B-大华-SGD`）

## 关键约束

### 代号1
- **源文件命名**：`{大写字母}-{银行全称}.{xls|xlsx|csv}`，放入 `data/input/`（不处理 `raw/` 子目录）
- **模板**：`template/1/国内银行汇总.xlsx` 缺失时脚本直接退出
- **年份自动刷新**：工作副本年份与当前年不同时，自动从模板重新复制并调用 `refresh_balance_sheet_dates`
- **中国银行 CSV 列头**：含 `[英文]` 后缀，读取后统一截断为 `[` 前的中文部分
- **农业银行**：含非交易行，用日期格式正则 `\d{4}-\d{2}-\d{2}` 过滤

### 代号2
- **源文件命名**：`{大写字母}-{银行全称}-{币种大写}.{xls|xlsx|csv}`，放入 `data/input/2/`；华美银行使用 `.pdf`，如 `A-华美银行-USD.pdf`
- **模板**：`template/2/银行汇总.xlsx` 缺失时脚本直接退出
- **年份自动刷新**：余额工作表名称以 `MIG银行余额` 开头，`_find_balance_sheet_2` 动态定位
- **汇丰银行余额列**：列名含动态货币后缀（如 `账面结余(HKD 港元)`），通过前缀匹配 `_resolve_col` 定位
- **东亚银行余额**：余额汇总行位于 `日期及时间` 列以 `总结余` 开头的行，余额值在 `存入金额` 列；`row_filter_prefix="总结余"` 过滤后仅剩余额行，`_parse_date_str` 通过 `re.search` 从 `总结余（截至 YYYY年M月D日)` 中提取日期
- **华侨银行余额**：使用 `余额` + `交易日期` 提取月末余额，日期为 `YYYYMMDD` 格式
- **华美银行 PDF**：仅支持文本型 PDF，不做 OCR；从 `DAILY BALANCES` 取账单月份最后一条余额
- **金额清洗**：余额字段中的逗号分隔符和 `+` 前缀在 `get_monthly_balances` 中处理

### 代号4
- **日期输入**：只接受 `YYYY-MM-DD`，不接受 `YYYY/MM/DD`、`YYYYMMDD` 或不存在日期
- **Chrome 依赖**：需要用户电脑安装 Google Chrome
- **登录态**：不自行构造 HTTP 请求；通过独立 Chrome profile 保存登录状态
- **登录入口**：无 Cookie 时只打开 `https://aim1.567okey.com/Public/login.html`，不使用导出链接探测登录
- **下载目录**：程序尽量通过 Chrome Preferences 指定为 `data/output/4/`；如浏览器策略限制下载行为，以 Chrome 实际行为为准
- **批次等待**：`.env` 可配置 `MODE4_BATCH_WAIT_SECONDS`；命令行 `--wait-seconds` 优先级最高，但不再提前截断检查机会
- **单批数量**：`.env` 可配置 `MODE4_BATCH_SIZE`；默认每批打开 5 个导出链接
- **重试次数**：`.env` 可配置 `MODE4_RETRY_LIMIT`；默认每批缺失文件最多重试 3 次
- **检查机会**：`.env` 可配置 `MODE4_MISSING_CHECK_CHANCES` 和 `MODE4_CHECK_INTERVAL_SECONDS`；默认每 2 秒检查 1 次，最多 10 次，机会用完后才重试
- **已存在跳过**：打开导出 URL 前先检查日期文件，已存在的标准文件或 Chrome 重复下载文件会直接跳过
- **重复下载**：识别 `YYYY-MM-DD,YYYY-MM-DD (1).xlsx` 这类 Chrome 重复下载文件；合并时同一天取最后修改时间最新的完成文件
- **合并输出**：下载齐备后自动合并为单个 `.xlsx`，不新增来源列，首个文件决定表头
- **XLS 合并兜底**：`.xls` 文件可被 Excel 打开但 `xlrd` 读取失败时，优先尝试 LibreOffice，再尝试 Windows Excel COM 临时转换为 `.xlsx`
- **页码**：`p=[PAGE]` 原样保留，本版本不做分页循环
- **macOS 复用**：启动 Chrome 时固定 `--user-data-dir=data/browser_profile/4` 和 `--profile-directory=Default`；普通 Chrome 手动打开不共享该登录态
- **Cookie 判定**：只把 `data/browser_profile/4/Default/Cookies` 作为可复用登录态判断依据，`Default/Network/Cookies` 仅打印诊断日志

### 代号5
- **源文件目录**：`data/input/raw/5/`，所有文件平铺放置
- **文件识别规则**（stem 小写前缀匹配，来自 `PLATFORM_PREFIXES_5`）：
  - `admin-` → admin 后台主表（XLS）
  - `ibfpay-` → IBFYPAY 订单明细格式（无 `类型` 列，手续费置0）
  - `ibf平台` → IBFYPAY 资金流水账格式（有 `类型` 列，两行合并）
  - `superpay-` → SUPERPAY 平台
  - `wangupay-` 或 `wangguypay-` → WANGGUYPAY 平台
- **匹配优先级**：IBFYPAY > SUPERPAY > WANGGUYPAY
- **IBFYPAY 关联键**：admin.`第三方订单号` ↔ ibfpay.`系统流水号`
- **SUPERPAY / WANGGUYPAY 关联键**：admin.`订单号` ↔ 平台.`商户订单号`
- **手续费计算**：IBFYPAY = 代付金额 − 手续费（源文件已有）；SUPERPAY = abs(代付金额 − 实收)；WANGGUYPAY = 源文件 `手续费(try)` 列
- **平台多余行**：平台有、admin 无的记录，`是否匹配 = "平台多余"`，`机构` 填平台名称（IBFYPAY/SUPERPAY/WANGGUYPAY）
- **admin 必须存在**：找不到 admin 文件时直接退出并打印错误

### 代号3
- **源文件目录**：`data/input/3/`，所有文件平铺放置（均为 `.xlsx`）
- **文件识别规则**（stem 小写前缀匹配）：
  - `admin` 开头 → admin 订单主表（工作表："汇总"）
  - `adyen-` 开头 → Adyen 平台报告（工作表："Data"）
  - `华为` 开头 → 华为平台报告（工作表："Sheet0"）
  - `googol-` 或 `google-` 开头 → Google Play 报告（取第一个工作表，名称含日期后缀）
- **流水号关联**：admin.`流水号` ↔ Adyen.`Psp Reference` / 华为.`华为订单号` / Google.`Description`
- **Adyen 去重**：同一 Psp Reference 有多行（Received/Authorised/SentForSettle），按 `ADYEN_RECORD_TYPE_PRIORITY = ["SentForSettle", "Authorised"]` 优先取 SentForSettle 行
- **Google 去重**：只保留 `Transaction Type == "Charge"` 行（排除 Google fee 和退款行）
- **平台支付方式填充**：Adyen → `Payment Method`（mc/visa/troy）；华为 → `支付方式`（World Pay/Adyen/话费）；Google → 固定值 "Google Play"
- **输出文件**：`data/output/订单匹配结果_{YYYYMMDD}.xlsx`，单工作表"订单匹配结果"，包含 admin 全部原始列 + 末尾追加「平台支付方式」列

## 日期格式支持

`_parse_date_str()` 支持以下格式（按优先级）：
1. `YYYY-MM-DD` / `YYYY/MM/DD`（含时间变体）
2. `YYYYMMDD`（无分隔符）
3. `YYYY年M月D日`（中文）
4. `DD/MM/YYYY`（大华银行 UOB 格式）
