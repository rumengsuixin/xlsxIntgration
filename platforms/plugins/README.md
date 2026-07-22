# platforms/plugins/ —— 疑难平台 Python 插件

当平台读取/清洗**无法用 `../6/*.json` 纯声明表达**时（两行合并、聚合计算、多 sheet 联表、
倒推汇率、PDF 解析等），在这里放一个 `.py` 插件。放文件即生效，**无需重打包**。

## 启用

- 把 `example_platform.py.txt` 复制/改名为真正的 `.py`（如 `mypay.py`），按需修改。
- 文件名以 `_` 开头的会被忽略；`.txt` 结尾不会被加载（所以样例安全）。

## 插件写法（二选一）

- **纯声明**：定义模块级 `PLATFORM = {...}`（结构同 `../6/*.json`），走内置 `generic` handler。
- **自定义 handler**：定义 `def register(api):`，在里面
  `api.register_platform(spec_dict, handler=你的handler实例)`。

可选：`CODE = "6"`（或 `CODES = ["5","6"]`）声明只在某代号加载。

## handler 契约

handler 需实现两个方法：

```python
def read(self, spec, direction, filepath) -> pandas.DataFrame: ...
def build_lookup(self, spec, direction, df) -> pandas.DataFrame: ...
```

`build_lookup` 返回的查找表必须：
- 以 `spec.join_col`（对应 admin 订单号）为 **DataFrame 索引**；
- 列名用 **CANON 内部列名**（`platform_engine.CANON`）：
  `__no__ / __amt__ / __fee__ / __status__ / __desc__ / __ftime__ / __ctime__`。

引擎据此统一取值、映射状态、生成结果的 7 个新增列；这些内部列不会泄漏到输出表。

可直接复用 `platform_engine` 里的 `read_source_table`、`dedup_lookup`、`build_lookup_from_columns` 等工具。

## 失败隔离

单个插件抛异常只会被跳过并打中文 warning，不影响其它平台/插件。

## 第三方依赖

- 程序已内置 `pandas / openpyxl / xlrd`，插件可直接 import。
- 需要程序没带的**纯 Python** 库时，把库源码/解压后的 wheel 放到 `../vendor/`，加载器会自动加入搜索路径。
- 需要**带 C 扩展**的新库（如 `.xlsb` 的 `pyxlsb`）时，才需要联系开发者重新打包一次。

完整示例见 `example_platform.py.txt`；总体说明见 `../README.md` 与 `docs/平台插件参考.html`。
