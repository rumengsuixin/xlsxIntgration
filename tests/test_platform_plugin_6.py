"""代号6 平台外置化（两层插件）测试。

覆盖：内置回退、JSON 覆盖/新增、插件注册与失败隔离、方向列覆盖、内部列不泄漏，
并守护 repo 内 platforms/6/*.json 与 BUILTIN_SPECS_6 不漂移。
"""

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.bank_integration.platform_loader import get_platforms_dir, load_platform_registry
from src.bank_integration.platform_spec import CANON, get_handler
from src.bank_integration.platform_engine import enrich_admin_generic
from src.bank_integration.app6 import (
    _SCHEMA_6,
    build_betcat_lookup_6,
    build_goldenpay_lookup_6,
    enrich_admin_6,
)
from src.bank_integration.config6 import (
    ADMIN_COLLECTION_JOIN_COL_6,
    BETCAT_AMOUNT_COL_6,
    BETCAT_FEE_COL_6,
    BETCAT_JOIN_COL_6,
    BETCAT_PAY_TIME_COL_6,
    BETCAT_PLATFORM_NO_COL_6,
    BETCAT_STATUS_COL_6,
    FEE_COL_6,
    GOLDENPAY_COLLECTION_AMOUNT_SRC_6,
    GOLDENPAY_COLLECTION_PLATFORM_NO_SRC_6,
    GOLDENPAY_PAYOUT_AMOUNT_SRC_6,
    GOLDENPAY_PAYOUT_PLATFORM_NO_SRC_6,
    MATCH_STATUS_COL_6,
    OUTPUT_NEW_COLS_6,
    PLATFORM_AMOUNT_COL_6,
    PLATFORM_ORDER_NO_COL_6,
    PLATFORM_SOURCE_COL_6,
    PLATFORM_STATUS_COL_6,
)


def _keys(specs):
    return [s.key for s in specs]


class BuiltinFallbackTests(unittest.TestCase):
    """无外置目录 / 空目录时，注册表 = 内置三平台，行为与外置化前一致。"""

    def test_missing_platforms_dir_falls_back_to_builtins(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("6", platforms_dir=Path(tmp) / "不存在")
        self.assertEqual(_keys(specs), ["BETCAT", "CASHNEWPAY", "GOLDENPAY"])
        self.assertEqual([s.priority for s in specs], [10, 20, 30])

    def test_empty_platforms_dir_equals_builtins(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
        self.assertEqual(_keys(specs), ["BETCAT", "CASHNEWPAY", "GOLDENPAY"])

    def test_repo_json_matches_builtin_specs(self):
        """repo 内 platforms/6/*.json 深合并后必须与内置声明逐字段一致（防漂移）。"""
        specs_repo = load_platform_registry("6", platforms_dir=get_platforms_dir())
        with tempfile.TemporaryDirectory() as tmp:
            specs_builtin = load_platform_registry("6", platforms_dir=Path(tmp))
        self.assertEqual(specs_repo, specs_builtin)


class JsonOverrideTests(unittest.TestCase):
    """外部 JSON 增量合并 / 新增平台。"""

    def _write(self, tmp, name, obj):
        code_dir = Path(tmp) / "6"
        code_dir.mkdir(parents=True, exist_ok=True)
        (code_dir / name).write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")

    def test_json_override_merges_status_map(self):
        with tempfile.TemporaryDirectory() as tmp:
            # 仅提供增量：给 BETCAT 增加一个状态映射
            self._write(tmp, "betcat.json", {"key": "BETCAT", "status_map": {"部分退款": "关闭"}})
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
            betcat = next(s for s in specs if s.key == "BETCAT")
            # 新增映射生效
            self.assertEqual(betcat.status_map["部分退款"], "关闭")
            # 内置映射仍保留（深合并，非替换）
            self.assertEqual(betcat.status_map["支付成功"], "成功")
            # 其它字段不受影响
            self.assertEqual(betcat.join_col, BETCAT_JOIN_COL_6)

    def test_json_adds_new_platform_participates_in_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write(tmp, "newpay.json", {
                "key": "NEWPAY", "priority": 40, "join_col": "商户订单号",
                "columns": {"platform_no": "流水", "amount": "金额"},
                "status_map": {"成功": "成功"},
                "directions": {
                    "collection": {"prefixes": ["newpay收款"]},
                    "payout": {"prefixes": ["newpay兑换"]},
                },
            })
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
        self.assertIn("NEWPAY", _keys(specs))
        self.assertEqual(_keys(specs)[-1], "NEWPAY")  # priority 40 排最后

        admin = pd.DataFrame([{ADMIN_COLLECTION_JOIN_COL_6: "ORD-1", "金额": "10"}])
        newpay_lk = pd.DataFrame(
            {CANON["amount"]: ["10"], CANON["platform_no"]: ["N-1"], CANON["status"]: ["成功"]},
            index=["ORD-1"],
        )
        result = enrich_admin_generic(admin, {"NEWPAY": newpay_lk}, specs, _SCHEMA_6)
        row = result.iloc[0]
        self.assertEqual(row[MATCH_STATUS_COL_6], "是")
        self.assertEqual(row[PLATFORM_SOURCE_COL_6], "NEWPAY")
        self.assertEqual(row[PLATFORM_ORDER_NO_COL_6], "N-1")
        self.assertEqual(row[PLATFORM_STATUS_COL_6], "成功")

    def test_disabled_platform_is_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write(tmp, "goldenpay.json", {"key": "GOLDENPAY", "enabled": False})
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
        self.assertNotIn("GOLDENPAY", _keys(specs))
        self.assertEqual(_keys(specs), ["BETCAT", "CASHNEWPAY"])

    def test_broken_json_is_skipped_keeping_builtin(self):
        with tempfile.TemporaryDirectory() as tmp:
            code_dir = Path(tmp) / "6"
            code_dir.mkdir(parents=True, exist_ok=True)
            (code_dir / "betcat.json").write_text("{ 这不是合法 json", encoding="utf-8")
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
        # 坏 JSON 被忽略，保留内置三平台
        self.assertEqual(_keys(specs), ["BETCAT", "CASHNEWPAY", "GOLDENPAY"])


class PluginTests(unittest.TestCase):
    """外部 .py 插件注册与失败隔离。"""

    _GOOD = (
        "CODE = '6'\n"
        "import pandas as pd\n"
        "class H:\n"
        "    def read(self, spec, direction, filepath):\n"
        "        return pd.DataFrame()\n"
        "    def build_lookup(self, spec, direction, df):\n"
        "        return df\n"
        "def register(api):\n"
        "    api.register_platform({\n"
        "        'key': 'PLUGPAY', 'priority': 50, 'join_col': '商户订单号',\n"
        "        'handler': 'plugpay',\n"
        "        'directions': {'collection': {'prefixes': ['plugpay收款']},\n"
        "                       'payout': {'prefixes': ['plugpay兑换']}},\n"
        "    }, handler=H())\n"
    )
    _BAD = "raise RuntimeError('插件故意报错')\n"

    def test_plugin_registers_and_bad_plugin_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            plugins = base / "plugins"
            plugins.mkdir(parents=True, exist_ok=True)
            (plugins / "good.py").write_text(self._GOOD, encoding="utf-8")
            (plugins / "bad.py").write_text(self._BAD, encoding="utf-8")
            specs = load_platform_registry("6", platforms_dir=base)

        keys = _keys(specs)
        # 好插件注册成功
        self.assertIn("PLUGPAY", keys)
        # 自定义 handler 已注册
        self.assertIsNotNone(get_handler("plugpay"))
        # 坏插件被跳过，内置平台不受影响
        self.assertIn("BETCAT", keys)
        self.assertIn("CASHNEWPAY", keys)
        self.assertIn("GOLDENPAY", keys)

    def test_txt_sample_not_loaded(self):
        """.py.txt 样例默认不加载（避免样例被当真平台）。"""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            plugins = base / "plugins"
            plugins.mkdir(parents=True, exist_ok=True)
            (plugins / "example.py.txt").write_text(self._GOOD, encoding="utf-8")
            specs = load_platform_registry("6", platforms_dir=base)
        self.assertNotIn("PLUGPAY", _keys(specs))


class DirectionAndLeakTests(unittest.TestCase):
    """方向列覆盖 & 内部列不泄漏。"""

    def test_direction_columns_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("6", platforms_dir=Path(tmp))
        gp = next(s for s in specs if s.key == "GOLDENPAY")
        col_c = gp.cols_for("collection")
        col_p = gp.cols_for("payout")
        # 收/付平台单号列与金额列名不同，由 directions.<dir>.columns 覆盖
        self.assertEqual(col_c["platform_no"], GOLDENPAY_COLLECTION_PLATFORM_NO_SRC_6)
        self.assertEqual(col_c["amount"], GOLDENPAY_COLLECTION_AMOUNT_SRC_6)
        self.assertEqual(col_p["platform_no"], GOLDENPAY_PAYOUT_PLATFORM_NO_SRC_6)
        self.assertEqual(col_p["amount"], GOLDENPAY_PAYOUT_AMOUNT_SRC_6)
        # 顶层公共列（手续费/状态）两方向一致
        self.assertEqual(col_c["fee"], col_p["fee"])

    def test_canonical_cols_not_leaked(self):
        admin = pd.DataFrame([{ADMIN_COLLECTION_JOIN_COL_6: "MORDER-1", "金额": "10"}])
        betcat_raw = pd.DataFrame([{
            BETCAT_JOIN_COL_6: "MORDER-1",
            BETCAT_PLATFORM_NO_COL_6: "B1",
            BETCAT_AMOUNT_COL_6: "10",
            BETCAT_STATUS_COL_6: "支付成功",
            BETCAT_FEE_COL_6: "0.1",
            BETCAT_PAY_TIME_COL_6: "2026-06-30T23:50:00-03:00",
        }])
        result = enrich_admin_6(admin, build_betcat_lookup_6(betcat_raw), None, None)
        self.assertEqual(list(result.columns), list(admin.columns) + OUTPUT_NEW_COLS_6)
        for internal in CANON.values():
            for col in result.columns:
                self.assertNotIn(internal, str(col))


if __name__ == "__main__":
    unittest.main()
