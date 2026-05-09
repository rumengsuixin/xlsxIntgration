# BUG 记录

## 修复记录

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
