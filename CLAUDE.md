# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

银行流水数据整合项目,按业务线分代号:
- **代号1**:国内银行,将多家银行的交易流水原始导出文件(CSV/XLS/XLSX)汇总到 `国内银行汇总.xlsx`
- **代号2**:海外银行,源文件命名携带币种,汇总到 `银行汇总.xlsx`,余额表每行代表"银行+币种"组合
- **代号3**:游戏订单匹配,admin 订单表与 Adyen / 华为 / Google Play 商户平台订单通过流水号关联,输出 `订单匹配结果_{YYYYMMDD}.xlsx`
- **代号5**:代付订单对账,admin 代付订单与 IBFYPAY / SUPERPAY / WANGGUYPAY / 话费卡 / EPIN 通过订单号关联,输出 `代付对账结果_{YYYYMMDD}.xlsx`
- **代号6**:代收代付对账(Betcat / Cashnewpay / Goldenpay),admin 收款/兑换订单与平台关联,输出 `代收代付对账结果_{YYYYMMDD}.xlsx`

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
# 各代号入口
venv/Scripts/python.exe 整合1.py    # 国内银行整合
venv/Scripts/python.exe 整合2.py    # 海外银行整合
venv/Scripts/python.exe 整合3.py    # 游戏订单支付方式匹配
venv/Scripts/python.exe 整合5.py    # 代付订单对账
venv/Scripts/python.exe 整合6.py    # 代收代付对账

# 运行全部回归测试
venv/Scripts/python.exe -m unittest discover -s tests
# 运行单个测试方法
venv/Scripts/python.exe -m unittest tests.test_bank_integration.BankIntegrationSampleTests.test_read_samples_and_extract_latest_balance_by_date

# 安装依赖
venv/Scripts/pip.exe install -r requirements.txt
```

## 目录结构

```
整合1.py ~ 整合6.py          # 各代号入口
src/bank_integration/
    config.py ~ config6.py    # 各代号:路径常量、读取配置、列名映射
    scanner.py / readers.py   # 扫描源文件目录、按配置读取 CSV/XLS/XLSX
    balances.py / workbook.py # 提取期末余额、复制工作副本并写入子表
    app.py ~ app6.py          # 各代号 main() 主流程
    pdf_daily_balance.py      # 代号2 华美银行 PDF 解析
    platform_spec.py          # 平台外置:声明数据类/CANON/OutputColumn/OutputSchema/handler 注册表
    platform_engine.py        # 平台外置:通用读取/查找表/enrich_admin_generic(代号6)/enrich_admin_columnar(代号5)
    platform_loader.py        # 平台外置:定位 platforms/、内置→JSON→插件深合并
    platform_handlers_5.py    # 代号5 平台外置:GenericPayout5 + IBFYPAY/EPIN handler + SCHEMA_5
template/1|2/                 # 代号1/2 模板(必须预先存在)
platforms/                    # 平台外置配置(exe 旁,免重打包):5|6/*.json 声明式 + plugins/*.py 插件
data/
    input/                    # 代号1 源文件;input/2 ~ input/6 各代号源文件;input/raw 原始样例(不扫描)
    output/                   # 各代号最终输出(汇总/匹配/对账文件)
docs/                         # 参考文档(HTML):数据流/读取配置/表结构/平台插件/变更记录
tests/test_bank_integration.py / test_platform_plugin_6.py
```

## 关键约束

各代号读取配置、数据流步骤、表结构等细节见「文档索引」下的 `docs/*.html`。以下为必须遵守的护栏。

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

### 代号5
- **源文件目录**:`data/input/5/`,平铺放置;按 stem 小写前缀识别:`admin-` / `ibfpay-`(订单明细,手续费置0)/ `ibf平台`(资金流水账,两行合并)/ `superpay-` / `wangupay-`|`wangguypay-` / `usdt奖品发放信息`|`binance-`|`merged-`(Binance 聚合型,打款日取自文件名)
- **匹配优先级**:IBFYPAY > SUPERPAY > WANGGUYPAY(话费卡、EPIN 见 docs 与开发计划)
- **关联键**:IBFYPAY 用 admin.`第三方订单号` ↔ ibfpay.`系统流水号`;SUPERPAY/WANGGUYPAY 用 admin.`订单号` ↔ 平台.`商户订单号`
- **手续费**:IBFYPAY = 代付金额 − 手续费(源文件已有);SUPERPAY = abs(代付金额 − 实收);WANGGUYPAY = 源文件 `手续费(try)` 列
- **平台多余行**:平台有、admin 无的记录,`是否匹配 = "平台多余"`,`机构` 填平台名称
- **admin 必须存在**:找不到 admin 文件时直接退出并打印错误
- **平台配置外置化(护栏)**:代号5 用**列式引擎** `platform_engine.enrich_admin_columnar`(与代号6 的 `enrich_admin_generic` 物理隔离,按平台**原生列名**取值,不走 CANON);内置声明 `config5.BUILTIN_SPECS_5`,取值逻辑在 `platform_handlers_5`(`GenericPayout5Handler` 通用 + `IbfypayHandler`/`EpinHandler` 覆盖 `match_values`),输出计划 `SCHEMA_5`。`app5.enrich_admin_5` 是薄壳,**其 6 参位置签名被单测依赖不得改**;`build_*_lookup_5`/`read_*_5` 亦被单测依赖。**加结构类似 SUPERPAY 的直连平台改 `platforms/5/*.json` 不改 app5**;IBFYPAY/WANGGUYPAY/EPIN/PHONECARD 的 read/build 仍在 app5 由 `main` 按 `BUILTIN_PLATFORM_KEYS_5` 分派,外部/插件平台走通用 handler。`platforms/5/*.json` 须与 `BUILTIN_SPECS_5` 一致(测试 `test_repo_json_matches_builtin_specs` code"5" 守护)。详见 [docs/平台插件参考.html](docs/平台插件参考.html)
- **聚合型平台(护栏)**:平台**无订单号、一收款ID对多笔**的走**第二种范式**(如 Binance),`spec.recon_mode="aggregate"` + `handler="aggregate_recon"`,由**通用** `platform_handlers_5.AggregateReconHandler` 全程按 `spec.recon` 声明驱动(**无平台专属分支**),两侧各按「收款ID+日期」`aggregate_by_keys` 求和+计数、`reconcile_aggregate` full-outer 四状态判定,产出**独立 sheet**(原 4 sheet **逐字节不变、净增**)。**加同为聚合型的新平台只丢 `platforms/5/*.json`(`recon` 块声明取值/口径/输出),不改代码不重打包**;派生仅支持「列+正则(`amount_regex`)+去前缀(`id_strip_prefix`)」,超出者(查汇率表/跨表 join)才需插件或改引擎。日期口径 `recon.date_match_mode` 为 JSON 字段可切 `exact`/`period`/`t1_window`(Binance 用 T+1 窗口对齐)。引擎原语 `derive_series`/`aggregate_by_keys`/`reconcile_aggregate` 与 `enrich_admin_columnar` 独立、**不得触碰逐行 1:1 路径**;`app5.main` 在 enrich 后收集 `recon_mode=="aggregate"` 的附加 sheet,`write_output_5(extra_sheets=…)` 净增写入,`read_admin_5` 按扩展名 `excel_engine` 自适应(支持 .xlsx admin,.xls 金标准路径不变)

### 代号6
- **源文件目录**:`data/input/6/`,平铺放置;按 stem 小写前缀识别:`admin收款` / `admin兑换` / `betcat-payment` / `betcat-payout` / `cashnewpay收款` / `cashnewpay兑换` / `goldenpay收款` / `goldenpay兑换`
- **多格式自适应**:各平台源文件支持 `.csv/.xls/.xlsx` 任一格式,由 `_read_source_table_6` 按扩展名分派(`.csv`→多编码 utf-8-sig/gbk/gb18030/utf-8、`.xls`→xlrd、`.xlsx`→openpyxl);扫描白名单已放行三格式
- **关联键**:admin.`订单号` ↔ Betcat.`MerOrderNo` / Cashnewpay.`商户订单号` / Goldenpay.`商户单号`;匹配优先级 Betcat > Cashnewpay > Goldenpay
- **Goldenpay 收/付表头不同**:与 Betcat/Cashnewpay「收付同表头」不同,Goldenpay 收款(sheet `代收订单导出`,金额列 `订单金额`、平台单号列 `订单号`)与兑换(sheet `代付订单导出`,金额列 `金额`、平台单号列 `订单编号`)列名不一致;现由 `BUILTIN_SPECS_6` 的 `directions.<方向>.columns` 分别声明,通用引擎归一化到 `CANON` 内部列,无命令式 rename
- **admin 必须存在**:代收/代付 admin 均缺失时直接退出
- **平台配置外置化(护栏)**:平台定义走"内置 `config6.BUILTIN_SPECS_6` → exe 旁 `platforms/6/*.json` → `platforms/plugins/*.py`"三层深合并(`platform_loader`),通用读取/匹配在 `platform_engine`,声明数据类在 `platform_spec`。`app6.py` 的 `build_*_lookup_6`/`enrich_admin_6`/`scan`/`main` 是薄壳,**改匹配/输出逻辑改引擎、加平台优先改 JSON/插件不改 `app6.py`**;`build_betcat_lookup_6`/`build_goldenpay_lookup_6`/`enrich_admin_6` 三处签名被单测依赖,不得改。查找表内部列必须用 `CANON`(`__amt__` 等),enrich 结尾丢弃、不得泄漏到输出。`platforms/6/*.json` 须与 `BUILTIN_SPECS_6` 保持一致(测试 `test_repo_json_matches_builtin_specs` 守护),改内置时同步重生成 JSON。详见 [docs/平台插件参考.html](docs/平台插件参考.html)

## 日期格式支持

`_parse_date_str()` 支持以下格式(按优先级):
1. `YYYY-MM-DD` / `YYYY/MM/DD`(含时间变体)
2. `YYYYMMDD`(无分隔符)
3. `YYYY年M月D日`(中文)
4. `DD/MM/YYYY`(大华银行 UOB 格式)

## 文档索引

| 文档 | 内容 |
|---|---|
| [docs/数据流参考.html](docs/数据流参考.html) | 代号1/2/5 数据流步骤(函数调用链) |
| [docs/读取配置参考.html](docs/读取配置参考.html) | 各银行(代号1/2)+ 平台(代号5)读取配置表、代号3 平台识别/去重 |
| [docs/表结构参考.html](docs/表结构参考.html) | 代号1/2 汇总表工作表结构、代号3/5 输出结构 |
| [docs/平台插件参考.html](docs/平台插件参考.html) | 代号6 平台外置化:`platforms/` JSON schema + 插件契约(接新平台免重打包) |
| [docs/变更记录.html](docs/变更记录.html) | 修复/优化历史(唯一来源,倒序) |
| [开发计划.md](开发计划.md) | 各代号功能、待办、工程状态(当前未决 ⚠️ 记于各代号「待办」) |
| [README.md](README.md) | 面向使用者的运行/打包说明 |
