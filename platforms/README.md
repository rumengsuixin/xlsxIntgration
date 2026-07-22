# platforms/ —— 平台配置外置目录（接新平台免重打包）

这个目录就在程序（.exe）旁边，**修改后无需重新打包，重新运行程序即可生效**。

```
platforms/
├── 6/                代号6（代收代付对账）的平台声明，每个平台一个 .json
│   ├── betcat.json
│   ├── cashnewpay.json
│   ├── goldenpay.json
│   └── README.md     ← JSON 字段说明 + 新增平台步骤
├── plugins/          疑难平台的 .py 插件（声明式表达不了时才用）
│   ├── example_platform.py.txt   ← 样例（改名为 .py 才会加载）
│   └── README.md
└── vendor/           可选：放纯 Python 第三方库（插件 import 得到）
```

## 接一个新平台，怎么选？

| 情况 | 做法 | 是否要重打包 |
|---|---|---|
| 新平台只是列名/表头/状态值不同，结构和已有平台类似 | 往 `6/` 放一个 `.json`（照抄 `betcat.json` 改） | 否 |
| 新平台需要特殊清洗（两行合并、聚合、倒推汇率、PDF 等） | 往 `plugins/` 放一个 `.py`（照抄 `example_platform.py.txt`） | 否 |
| 新平台需要一个程序没带的第三方库（如读 `.xlsb`） | 联系开发者重新打包一次 | 是（少见） |

改坏了不用怕：某个 `.json`/`.py` 出错只会被跳过并在日志里提示，其它平台照常运行；把 `6/` 清空则回到内置默认（Betcat/Cashnewpay/Goldenpay）。

详细字段与写法见 `6/README.md`、`plugins/README.md`，以及项目 `docs/平台插件参考.html`。
