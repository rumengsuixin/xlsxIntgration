# platforms/6/ —— 代号6 平台声明（JSON）

每个平台一个 `.json`。程序启动时读取本目录，与内置默认**深合并**（JSON 覆盖/新增同名字段），
按 `priority` 升序参与匹配。改完保存、重新运行程序即可，无需重打包。

## 字段说明

| 字段 | 必填 | 说明 |
|---|---|---|
| `key` | 是 | 平台标识（大写），会作为结果里的"来源平台"值 |
| `priority` | 是 | 越小越优先。内置 Betcat=10 / Cashnewpay=20 / Goldenpay=30，新平台建议 ≥40 |
| `join_col` | 是 | 平台侧关联键列名（对应 admin 的"订单号"） |
| `handler` | 否 | 默认 `"generic"`；用插件自定义读取时填插件里注册的名字 |
| `sheet` | 否 | Excel 优先读取的 sheet 名（CSV 忽略） |
| `use_first_sheet` | 否 | 找不到 `sheet` 时是否回退首个 sheet，默认 `true` |
| `columns` | 是 | **规范字段 → 源列名** 映射（见下），顶层为默认值 |
| `status_map` | 否 | 原始状态 → 标准状态（成功/失败/处理中/关闭）精确映射 |
| `status_prefix_map` | 否 | 状态含动态后缀时按前缀映射（如 `USER_REFUND-xxx`） |
| `directions` | 是 | `collection`（代收）/`payout`（代付）各自的 `prefixes`、可选 `sheet`、可选 `columns`（覆盖顶层） |
| `enabled` | 否 | 设 `false` 可停用某平台（含内置平台） |

### columns 的规范字段名（固定这几个）

| 规范字段 | 含义 |
|---|---|
| `platform_no` | 平台内部流水号 → 结果"平台流水号" |
| `amount` | 平台金额（**命中判断依据**，必须能取到） |
| `fee` | 手续费 |
| `status` | 原始状态（中文） |
| `status_desc` | 原始状态描述（英文，若有则优先于 `status` 做映射） |
| `finish_time` | 完成时间（结果"交易日期"首选） |
| `create_time` | 创建时间（完成时间为空时回退） |

## 新增一个"结构类似"平台的步骤

1. 复制 `betcat.json` 为 `新平台.json`。
2. 改 `key`、`priority`（≥40）、`join_col`。
3. 把 `columns` 里每个规范字段对到该平台源文件的真实列名。
4. 在 `directions.collection.prefixes` / `directions.payout.prefixes` 填源文件名前缀（小写）。
5. 按平台实际状态值补 `status_map`。
6. 保存，把源文件放进 `data/input/6/`，重新运行程序。

> Goldenpay 的收/付列名不同：看 `goldenpay.json`，在 `directions.<方向>.columns` 里分别覆盖 `platform_no`/`amount` 即可，不必写任何代码。

结构实在特殊（两行合并、聚合、PDF 等）时，改用 `../plugins/`。
