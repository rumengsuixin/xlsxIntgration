# BUG 记录

## 修复记录

### [2026-05-13] Adyen手续费字段错误计算 + 未匹配订单状态错误置为"失败"

**内容**
1. 代号3订单匹配结果中，Adyen 支付类型的订单行手续费（`手续费` 列）仍在计算 Markup/Scheme Fees/Interchange 的加和，但业务要求该字段统一置为 `0`。
2. `是否匹配 = "否"` 的订单（包括非 Adyen 类型）统一将 `状态` 置为 `"失败"`，但"未匹配"不等于"支付失败"，逻辑错误。

**根本原因**
- `enrich_admin()` 和 `_build_platform_only_rows()` 中，Adyen 手续费由三个费用分量相加得出，未考虑业务侧需要将其归零的需求。
- 状态赋值逻辑直接用 `matched` 决定 `状态`，导致"未匹配"与"支付失败"耦合，Adyen Refused（真实失败）和"无平台数据"（数据缺失）被错误地统一处理。

**修复（[app3.py](src/bank_integration/app3.py)）**

1. **手续费置0**：`enrich_admin()` Adyen 分支直接 `fee_amt = "0"`，移除三分量求和逻辑；`_build_platform_only_rows()` Adyen 多余行同样改为 `row[FEE_COL] = "0"`。

2. **状态独立判定**：`build_adyen_lookup()` 返回类型改为 `tuple[DataFrame, set]`，额外返回 `refused_keys`（被排除的 Refused PSP Reference 集合）。`enrich_admin()` 新增 `adyen_refused` 参数，状态逻辑改为：
   - Adyen 订单：`matched=True` → 成功；PSP Reference 在 refused_keys 中 → 失败；其余 → 空
   - 非 Adyen 订单：`matched=True` → 成功；未匹配 → 空

**效果**
- 所有 Adyen 订单行（含平台多余行）`手续费 = 0` ✓
- Adyen Refused 订单 `状态 = 失败`，平台数据缺失时 `状态 = 空`（不再误报失败）✓
- 非 Adyen 未匹配订单 `状态 = 空` ✓

---

### [2026-05-13] 苹果支付数据出现在"订单匹配结果"Sheet

**内容**
代号3输出的"订单匹配结果"Sheet 中出现支付方式为苹果支付的数据行，而苹果支付无法通过订单流水号匹配，应仅出现在专属的"苹果支付"Sheet 中。

**根本原因**
`enrich_admin()` 用 `"苹果支付" in src`（子串匹配）识别苹果支付，但 `write_output()` 第1418行用 `.eq("苹果支付Lua")`（精确匹配）过滤。Admin 数据中存在值为"苹果支付"（无"Lua"后缀）的记录，被 `enrich_admin()` 处理为 `matched=True`，却通不过 `write_output()` 的过滤条件，导致出现在主结果 Sheet。

**修复（[app3.py:1418](src/bank_integration/app3.py#L1418)）**
将精确匹配改为子串匹配，与 `enrich_admin()` 保持一致：
```python
is_apple = main_df[ADMIN_PAYMENT_COL].astype(str).str.strip().str.contains("苹果支付", na=False)
```

**效果**
所有包含"苹果支付"的支付方式变体均被排除出"订单匹配结果"Sheet，仅写入"苹果支付"Sheet ✓

---

### [2026-05-13] 日志警告中未匹配数量与"匹配失败订单"sheet 实际行数不一致

**内容**
代号3脚本打印 `[WARN] 【Google支付】存在 3807 条未匹配记录` 但结果文件"匹配失败订单"sheet 实际有 3850 行，相差 43 条。

**根本原因**
`log_match_stats()` 用 `STATUS_COL == "失败"` 计数（3807），而 `write_output()` 写入失败 sheet 时用 `MATCH_STATUS_COL == "否"` 筛选（3850）。
差值 43 条 = **已退款但未匹配到平台数据的订单**：`refund_flag == "已退款"` 时第739行优先将 `STATUS_COL` 置为 `"退款"`，但这些行的 `MATCH_STATUS_COL` 仍为 `"否"`，因此被写入失败 sheet 却不被日志计入"失败"计数。

**修复（[app3.py:794-799](src/bank_integration/app3.py#L794-L799)）**
警告行改用 `MATCH_STATUS_COL == "否"` 统计未匹配数量，与写入 sheet 的筛选条件保持一致：
```python
unmatched_count = (
    rows[MATCH_STATUS_COL].astype(str).str.strip().eq("否").sum()
    if MATCH_STATUS_COL in rows.columns
    else failed
)
if unmatched_count > 0:
    logging.warning("【%s】存在 %d 条未匹配记录，请查看【匹配失败订单】sheet", label, unmatched_count)
```

**效果**
日志警告中的未匹配数量与"匹配失败订单"sheet 实际行数完全一致 ✓

---

### [2026-05-08] Adyen Refused 订单被错误标记为"成功"（两轮修复）

**内容**
代号3订单匹配中，Adyen 数据文件中 Record Type = "Refused" 的拒付订单，在输出结果中仍显示"成功"状态。

**根本原因（第二轮排查）**
Adyen 拒付流程会为同一 PSP Reference 生成两条记录：`Received`（请求已接收）和 `Refused`（已拒付），两者优先级均为 fallback=2。`sort_values` 稳定排序时 `Received` 在前，`drop_duplicates(keep="first")` 保留了 `Received`。  
第一轮修复只过滤 `Refused`，但 **`Received` 有非空 Main Amount** → `matched = True` → 状态仍为"成功"❌

**第一轮误判原因**
第一轮修复假设被保留的记录就是 "Refused"，但实际上被保留的是同等优先级下排在前面的 "Received"。

**最终修复（[app3.py:180](src/bank_integration/app3.py#L180)）**
将"排除 Refused"改为"只保留优先级列表内的类型"：
```python
valid_mask = result[ADYEN_RECORD_TYPE_COL].str.strip().isin(ADYEN_RECORD_TYPE_PRIORITY)
```
只有 SentForSettle / Authorised 进入查找表，Received、Refused、Cancelled 等所有非成功类型全部过滤。同时使用 `.str.strip()` 防止空格问题。

**效果**
- 拒付订单（Refused/Received）对应的 admin 订单 → 状态="失败" ✓
- 正常成交订单（SentForSettle/Authorised）→ 状态="成功"，不受影响 ✓
- 日志：`【Adyen】过滤 N 条非成功状态记录（Refused/Received 等），不计入匹配`

---

### [2026-05-08] Adyen 平台多余行结算币种为空导致汇总多余分组

**内容**
某笔平台多余行（Adyen 有记录、admin 无对应）的 SentForSettle 行中 "Payable (SC)" 为空，
导致结算货币列也为空，`build_summary_sheet` 将其分入独立的 `(Adyen/TRY/"")` 分组，
汇总表中多出一行结算币种为空的数据。

**原因**
[app3.py:452](src/bank_integration/app3.py#L452) 和 [app3.py:314-316](src/bank_integration/app3.py#L314-L316)
中均有 `if _to_float(settle_amt) else ""` 的条件：Payable 为空时结算币种也置为空，
本意是"避免汇总出空金额行"，但空结算币种本身成了新的分组维度，反而多出一行。

**修复**
结算币种始终从 Adyen 的 `Settlement Currency` 字段取值，不与 Payable 是否为空挂钩；
结算金额仍然只在 Payable 有值时填写（行为不变）。

**效果**
- 即使 Payable (SC) 为空，结算币种仍能正确归入 USD/HKD 分组 ✓
- 汇总表不再出现多余的"结算币种为空"分组行 ✓
