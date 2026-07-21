# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

银行流水数据整合项目,按业务线分代号:
- **代号1**:国内银行,将多家银行的交易流水原始导出文件(CSV/XLS/XLSX)汇总到 `国内银行汇总.xlsx`
- **代号2**:海外银行,源文件命名携带币种,汇总到 `银行汇总.xlsx`,余额表每行代表"银行+币种"组合
- **代号3**:游戏订单匹配,admin 订单表与 Adyen / 华为 / Google Play 商户平台订单通过流水号关联,输出 `订单匹配结果_{YYYYMMDD}.xlsx`
- **代号4**:后台充值订单浏览器导出,按支付日期逐日打开导出 URL,复用独立 Chrome 登录态并下载到 `data/output/4/`(含 `整合4_epin.py`、`整合4_bc.py` 子功能)
- **代号5**:代付订单对账,admin 代付订单与 IBFYPAY / SUPERPAY / WANGGUYPAY / 话费卡 / EPIN 通过订单号关联,输出 `代付对账结果_{YYYYMMDD}.xlsx`
- **代号6**:代收代付对账(Betcat / Cashnewpay),admin 收款/兑换订单与平台关联,输出 `代收代付对账结果_{YYYYMMDD}.xlsx`
- **代号7**:汇率抓取,从 XE.com 按日期抓取货币对 USD 汇率(本地 JSON 缓存),输出 `汇率_{start}_{end}.xlsx`

## 文档维护原则(铁律)

保持所有项目文档**简洁、去重、可索引**。改动文档前后都遵守:

1. **单一事实来源**:同一信息只记一处,其它地方用链接引用,禁止跨文件复制粘贴。
2. **CLAUDE.md 只放高层指引与索引**:概述、命令、关键约束(护栏)、文档索引。代码级细节(读取配置、单元格坐标、数据流步骤、运行参数)一律外移到 `docs/*.html`,正文只留指向链接。
3. **细节文档进 docs/**:新增参考类文档优先 HTML、放 `docs/`,并在下方「文档索引」登记一行。
4. **历史只写一处**:修复/优化历史统一进 [docs/变更记录.html](docs/变更记录.html)(倒序),不再分散到多文件。
5. **先索引后正文**:新增前先想"是否已有归属文档",能追加不新建、能精简不扩写。
6. **过期即删**:代码变更使描述失效时,同步更新或删除,不留陈旧描述。

## 常用命令

```powershell
# 各代号入口(代号1 兼容旧入口 整合.py)
venv/Scripts/python.exe 整合1.py    # 国内银行整合
venv/Scripts/python.exe 整合2.py    # 海外银行整合
venv/Scripts/python.exe 整合3.py    # 游戏订单支付方式匹配
venv/Scripts/python.exe 整合4.py    # 后台充值订单浏览器导出
venv/Scripts/python.exe 整合5.py    # 代付订单对账
venv/Scripts/python.exe 整合6.py    # 代收代付对账
venv/Scripts/python.exe 整合7.py    # 汇率抓取

# 代号4 手动指定日期范围(严格 YYYY-MM-DD;运行参数/环境变量见 docs/读取配置参考.html)
venv/Scripts/python.exe 整合4.py --date-range 2026-04-01 2026-04-30

# 运行全部回归测试
venv/Scripts/python.exe -m unittest discover -s tests
# 运行单个测试方法
venv/Scripts/python.exe -m unittest tests.test_bank_integration.BankIntegrationSampleTests.test_read_samples_and_extract_latest_balance_by_date

# 安装依赖
venv/Scripts/pip.exe install -r requirements.txt
```

## 目录结构

```
整合1.py ~ 整合7.py          # 各代号入口(整合.py=代号1兼容入口;整合4_epin.py/整合4_bc.py 为代号4子功能)
src/bank_integration/
    config.py ~ config7.py    # 各代号:路径常量、读取配置、列名映射(config4_epin/config4_bc 为子功能)
    scanner.py / readers.py   # 扫描源文件目录、按配置读取 CSV/XLS/XLSX
    balances.py / workbook.py # 提取期末余额、复制工作副本并写入子表
    app.py ~ app7.py          # 各代号 main() 主流程
    exchange_rate.py          # 代号7 汇率抓取/缓存/查询
    browser_operator.py       # 代号4 浏览器操作、pdf_daily_balance.py 代号2 PDF 解析
template/1|2/                 # 代号1/2 模板(必须预先存在)
data/
    input/                    # 代号1 源文件;input/2 ~ input/6 各代号源文件;input/raw 原始样例(不扫描)
    browser_profile/4/        # 代号4 独立 Chrome 登录态(运行时生成)
    output/                   # 各代号最终输出(汇总/匹配/对账/汇率文件)
docs/                         # 参考文档(HTML):数据流/读取配置/表结构/变更记录
tests/test_bank_integration.py
```

## 关键约束

各代号读取配置、数据流步骤、表结构、代号4运行参数等细节见「文档索引」下的 `docs/*.html`。以下为必须遵守的护栏。

### 代号1
- **源文件命名**:`{大写字母}-{银行全称}.{xls|xlsx|csv}`,放入 `data/input/`(不处理 `raw/` 子目录)
- **模板**:`template/1/国内银行汇总.xlsx` 缺失时脚本直接退出
- **年份自动刷新**:工作副本年份与当前年不同时,自动从模板重新复制并调用 `refresh_balance_sheet_dates`
- **中国银行 CSV 列头**:含 `[英文]` 后缀,读取后统一截断为 `[` 前的中文部分
- **农业银行**:含非交易行,用日期格式正则 `\d{4}-\d{2}-\d{2}` 过滤

### 代号2
- **源文件命名**:`{大写字母}-{银行全称}-{币种大写}.{xls|xlsx|csv}`,放入 `data/input/2/`;华美银行使用 `.pdf`(如 `A-华美银行-USD.pdf`)
- **模板**:`template/2/银行汇总.xlsx` 缺失时脚本直接退出
- **年份自动刷新**:余额工作表名称以 `MIG银行余额` 开头,`_find_balance_sheet_2` 动态定位
- **汇丰银行余额列**:列名含动态货币后缀(如 `账面结余(HKD 港元)`),通过前缀匹配 `_resolve_col` 定位
- **东亚银行余额**:余额汇总行位于 `日期及时间` 列以 `总结余` 开头的行,余额值在 `存入金额` 列;`_parse_date_str` 通过 `re.search` 从 `总结余(截至 YYYY年M月D日)` 提取日期
- **华美银行 PDF**:仅支持文本型 PDF,不做 OCR;从 `DAILY BALANCES` 取账单月份最后一条余额
- **金额清洗**:余额字段中的逗号分隔符和 `+` 前缀在 `get_monthly_balances` 中处理

### 代号3
- **源文件目录**:`data/input/3/`,平铺放置
- **文件识别**(stem 小写前缀):`admin`→主表 / `adyen-`→Adyen / `华为`→华为 / `googol-`|`google-`→Google Play(详见 docs/读取配置参考.html)
- **流水号关联**:admin.`流水号` ↔ Adyen.`Psp Reference` / 华为.`华为订单号` / Google.`Description`
- **Adyen 去重**:按 `ADYEN_RECORD_TYPE_PRIORITY` 只保留优先类型行(避免 Received/Refused 误判成功)
- **Google 去重**:只保留 `Transaction Type == "Charge"` 行
- **输出**:`data/output/订单匹配结果_{YYYYMMDD}.xlsx`,admin 全部原始列 + 末尾追加「平台支付方式」列(每日覆盖)

### 代号4
- **日期输入**:只接受 `YYYY-MM-DD`,不接受 `YYYY/MM/DD`、`YYYYMMDD` 或不存在日期
- **Chrome 依赖**:需要安装 Google Chrome;不自行构造 HTTP 请求,通过独立 Chrome profile(`data/browser_profile/4/`)保存登录态
- **登录入口**:无 Cookie 时只打开 `https://aim1.567okey.com/Public/login.html`,不使用导出链接探测登录;仅以 `Default/Cookies` 作为可复用登录态判断依据
- **下载目录**:尽量通过 Chrome Preferences 指定为 `data/output/4/`,以 Chrome 实际行为为准
- **已存在跳过**:打开导出 URL 前先检查日期文件,已存在的标准文件或 Chrome 重复下载文件(`YYYY-MM-DD,YYYY-MM-DD (1).xlsx`)直接跳过
- **合并输出**:下载齐备后自动合并为单个 `.xlsx`,不新增来源列,首个文件决定表头;`.xls` 读取失败时依次尝试 LibreOffice / Windows Excel COM 临时转 `.xlsx`
- **运行参数**:`--date-range`、`--wait-seconds` 及 `MODE4_*` 环境变量见 [docs/读取配置参考.html](docs/读取配置参考.html);`p=[PAGE]` 原样保留,不做分页循环

### 代号5
- **源文件目录**:`data/input/5/`,平铺放置;按 stem 小写前缀识别:`admin-` / `ibfpay-`(订单明细,手续费置0)/ `ibf平台`(资金流水账,两行合并)/ `superpay-` / `wangupay-`|`wangguypay-`
- **匹配优先级**:IBFYPAY > SUPERPAY > WANGGUYPAY(话费卡、EPIN 见 docs 与开发计划)
- **关联键**:IBFYPAY 用 admin.`第三方订单号` ↔ ibfpay.`系统流水号`;SUPERPAY/WANGGUYPAY 用 admin.`订单号` ↔ 平台.`商户订单号`
- **手续费**:IBFYPAY = 代付金额 − 手续费(源文件已有);SUPERPAY = abs(代付金额 − 实收);WANGGUYPAY = 源文件 `手续费(try)` 列
- **平台多余行**:平台有、admin 无的记录,`是否匹配 = "平台多余"`,`机构` 填平台名称
- **admin 必须存在**:找不到 admin 文件时直接退出并打印错误

## 日期格式支持

`_parse_date_str()` 支持以下格式(按优先级):
1. `YYYY-MM-DD` / `YYYY/MM/DD`(含时间变体)
2. `YYYYMMDD`(无分隔符)
3. `YYYY年M月D日`(中文)
4. `DD/MM/YYYY`(大华银行 UOB 格式)

## 文档索引

| 文档 | 内容 |
|---|---|
| [docs/数据流参考.html](docs/数据流参考.html) | 代号1/2/4/5 数据流步骤(函数调用链) |
| [docs/读取配置参考.html](docs/读取配置参考.html) | 各银行(代号1/2)+ 平台(代号5)读取配置表、代号3 平台识别/去重、代号4 运行参数 |
| [docs/表结构参考.html](docs/表结构参考.html) | 代号1/2 汇总表工作表结构、代号3/5 输出结构 |
| [docs/变更记录.html](docs/变更记录.html) | 修复/优化历史(唯一来源,倒序) |
| [开发计划.md](开发计划.md) | 各代号功能、待办、工程状态(当前未决 ⚠️ 记于各代号「待办」) |
| [README.md](README.md) | 面向使用者的运行/打包说明 |
