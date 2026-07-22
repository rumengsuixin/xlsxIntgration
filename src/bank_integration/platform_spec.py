"""平台声明式规格（PlatformSpec）、输出 schema 与 handler 注册表。

与具体代号无关：承载"平台 = 前缀识别 + 列名映射 + 状态映射 + 收/付方向"的
纯声明数据，供 platform_engine 的通用 reader / build_lookup / enrich 消费。
外置化的基础：JSON / 插件加载后统一转成 PlatformSpec，代码只认这套结构。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ── 规范内部列名 ──────────────────────────────────────────────────────────────
# build_lookup 把各平台源列名归一化到这套内部列名；仅供引擎内部取值使用，
# enrich 结尾会丢弃所有内部列，绝不进入输出（防止 __amt__ 之类泄漏到结果表）。
CANON: Dict[str, str] = {
    "platform_no":  "__no__",     # 平台内部流水号
    "amount":       "__amt__",    # 平台金额（命中判断依据）
    "fee":          "__fee__",    # 手续费
    "status":       "__status__",  # 原始状态（中文）
    "status_desc":  "__desc__",   # 原始状态描述（英文，优先于 status 映射）
    "finish_time":  "__ftime__",  # 完成时间（交易日期首选）
    "create_time":  "__ctime__",  # 创建时间（交易日期回退）
}

# 收 / 付两个方向的固定名称
DIRECTIONS = ("collection", "payout")


@dataclass
class DirectionSpec:
    """单个方向（代收 collection / 代付 payout）的声明。"""

    prefixes: List[str] = field(default_factory=list)   # 文件名 stem 小写前缀识别
    sheet: Optional[str] = None                          # Excel 优先 sheet（覆盖顶层）
    columns: Dict[str, str] = field(default_factory=dict)  # 规范字段→源列名（覆盖顶层）


@dataclass
class PlatformSpec:
    """一个平台的完整声明。"""

    key: str                                             # 平台标识（大写），= 输出"来源平台"
    priority: int                                        # 越小越优先（匹配 & 追加顺序）
    join_col: str                                        # 平台侧关联键（查找表索引列）
    handler: str = "generic"                             # 使用的 handler 名
    handler_params: dict = field(default_factory=dict)   # 传给自定义 handler 的参数
    sheet: Optional[str] = None                          # Excel 优先 sheet（顶层默认）
    use_first_sheet: bool = True                         # 未命中 sheet 时回退首个
    columns: Dict[str, str] = field(default_factory=dict)  # 规范字段→源列名（顶层默认）
    status_map: Dict[str, str] = field(default_factory=dict)         # 原始状态→标准状态
    status_prefix_map: Dict[str, str] = field(default_factory=dict)  # 状态前缀→标准状态
    directions: Dict[str, DirectionSpec] = field(default_factory=dict)
    enabled: bool = True

    def cols_for(self, direction: str) -> Dict[str, str]:
        """顶层 columns 与方向 columns 逐键合并（方向覆盖顶层）。"""
        merged = dict(self.columns)
        d = self.directions.get(direction)
        if d:
            merged.update(d.columns)
        return merged

    def sheet_for(self, direction: str) -> Optional[str]:
        """方向 sheet 优先，回退顶层 sheet。"""
        d = self.directions.get(direction)
        if d and d.sheet:
            return d.sheet
        return self.sheet

    @classmethod
    def from_dict(cls, data: dict) -> "PlatformSpec":
        """从声明式 dict（内置常量 / JSON / 插件）构造，字段缺失时给合理默认。"""
        directions: Dict[str, DirectionSpec] = {}
        for name, d in (data.get("directions") or {}).items():
            directions[name] = DirectionSpec(
                prefixes=list(d.get("prefixes", [])),
                sheet=d.get("sheet"),
                columns=dict(d.get("columns", {})),
            )
        return cls(
            key=data["key"],
            priority=int(data["priority"]),
            join_col=data["join_col"],
            handler=data.get("handler", "generic"),
            handler_params=dict(data.get("handler_params", {})),
            sheet=data.get("sheet"),
            use_first_sheet=bool(data.get("use_first_sheet", True)),
            columns=dict(data.get("columns", {})),
            status_map=dict(data.get("status_map", {})),
            status_prefix_map=dict(data.get("status_prefix_map", {})),
            directions=directions,
            enabled=bool(data.get("enabled", True)),
        )


@dataclass
class OutputSchema:
    """各代号输出的 7 个新增列列名 + admin 关联键候选（供通用 enrich 使用）。

    通用引擎与代号无关，所需的具体列名 / 关联键由各代号构造此对象传入。
    """

    match_status_col: str
    platform_source_col: str
    platform_order_no_col: str
    platform_amount_col: str
    platform_status_col: str
    fee_col: str
    transaction_date_col: str
    admin_join_candidates: List[str]
    match_yes: str = "是"
    match_no: str = "否"
    match_extra: str = "平台多余"

    def output_cols(self) -> List[str]:
        """按固定顺序返回 7 个新增列（与各代号 OUTPUT_NEW_COLS 顺序一致）。"""
        return [
            self.match_status_col,
            self.platform_source_col,
            self.platform_order_no_col,
            self.platform_amount_col,
            self.platform_status_col,
            self.fee_col,
            self.transaction_date_col,
        ]


# ── handler 注册表 ────────────────────────────────────────────────────────────
# name → handler 实例（须实现 read(spec, direction, filepath) 与
# build_lookup(spec, direction, df)）。声明式平台用内置 "generic"，
# 疑难平台由插件 register_handler 注册自定义实现。
HANDLER_REGISTRY: Dict[str, object] = {}


def register_handler(name: str, handler: object) -> None:
    """注册（或覆盖）一个 handler。"""
    HANDLER_REGISTRY[name] = handler


def get_handler(name: str):
    """按名取 handler，未注册返回 None。"""
    return HANDLER_REGISTRY.get(name)
