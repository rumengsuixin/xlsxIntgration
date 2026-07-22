"""代号5(代付订单对账)平台外置化测试。

覆盖:内置回退、repo JSON 防漂移、新增声明式平台参与匹配、每平台各自 admin 关联键、
EPIN 机构门槛、插件失败隔离、scan 识别外部平台前缀。
"""

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.bank_integration.platform_loader import get_platforms_dir, load_platform_registry
from src.bank_integration.platform_engine import enrich_admin_columnar
from src.bank_integration.platform_handlers_5 import SCHEMA_5
from src.bank_integration.app5 import enrich_admin_5, scan_source_files_5
from src.bank_integration.config5 import (
    ADMIN_JOIN_COL_5,
    ADMIN_ORG_COL_5,
    ADMIN_TP_ORDER_COL_5,
    MATCH_STATUS_COL_5,
    PLATFORM_AMOUNT_COL_5,
    PLATFORM_CURRENCY_COL_5,
    PLATFORM_ORDER_NO_COL_5,
    PLATFORM_STATUS_COL_5,
)


def _keys(specs):
    return [s.key for s in specs]


class BuiltinFallbackTests(unittest.TestCase):

    def test_missing_platforms_dir_falls_back_to_builtins(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("5", platforms_dir=Path(tmp) / "不存在")
        self.assertEqual(_keys(specs), ["IBFYPAY", "SUPERPAY", "WANGGUYPAY", "PHONECARD", "EPIN"])
        self.assertEqual([s.priority for s in specs], [10, 20, 30, 40, 50])

    def test_repo_json_matches_builtin_specs(self):
        """repo 内 platforms/5/*.json 深合并后必须与内置声明逐字段一致(防漂移)。"""
        specs_repo = load_platform_registry("5", platforms_dir=get_platforms_dir())
        with tempfile.TemporaryDirectory() as tmp:
            specs_builtin = load_platform_registry("5", platforms_dir=Path(tmp))
        self.assertEqual(specs_repo, specs_builtin)

    def test_admin_join_col_and_currency_per_platform(self):
        """IBFYPAY 用第三方订单号、SUPERPAY 用订单号;币种分别为默认/列。"""
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("5", platforms_dir=Path(tmp))
        ibf = next(s for s in specs if s.key == "IBFYPAY")
        sp = next(s for s in specs if s.key == "SUPERPAY")
        self.assertEqual(ibf.admin_join_col, ADMIN_TP_ORDER_COL_5)
        self.assertEqual(sp.admin_join_col, ADMIN_JOIN_COL_5)
        self.assertEqual(ibf.currency_default, "TRY")
        self.assertEqual(sp.currency_col, "币种")


class NewPlatformTests(unittest.TestCase):

    def _write(self, tmp, name, obj):
        d = Path(tmp) / "5"
        d.mkdir(parents=True, exist_ok=True)
        (d / name).write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")

    def test_new_declarative_platform_participates_in_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write(tmp, "testpay.json", {
                "key": "TESTPAY", "priority": 60, "handler": "generic5",
                "join_col": "商户订单号", "admin_join_col": ADMIN_JOIN_COL_5,
                "currency_default": "USD", "fee_mode": "column", "arrive_mode": "column",
                "org_source": "admin",
                "status_map": {"成功": "成功"},
                "columns": {
                    "platform_no": "平台单号", "amount": "金额", "fee": "手续费",
                    "arrive_amount": "实收", "status": "状态", "finish_time": "完成时间",
                },
                "directions": {"payout": {"prefixes": ["testpay-"]}},
            })
            specs = load_platform_registry("5", platforms_dir=Path(tmp))
        self.assertEqual(_keys(specs)[-1], "TESTPAY")

        admin = pd.DataFrame([{ADMIN_JOIN_COL_5: "ORD-1", ADMIN_ORG_COL_5: "TESTPAY", ADMIN_TP_ORDER_COL_5: ""}])
        lk = pd.DataFrame(
            [{"平台单号": "T-1", "金额": "100", "手续费": "2", "实收": "98",
              "状态": "成功", "完成时间": "2026-04-01"}],
            index=pd.Index(["ORD-1"], name="商户订单号"),
        )
        result = enrich_admin_columnar(admin, {"TESTPAY": lk}, specs, SCHEMA_5)
        row = result.iloc[0]
        self.assertEqual(row[MATCH_STATUS_COL_5], "是")
        self.assertEqual(row[PLATFORM_ORDER_NO_COL_5], "T-1")
        self.assertEqual(row[PLATFORM_AMOUNT_COL_5], "100")
        self.assertEqual(row[PLATFORM_CURRENCY_COL_5], "USD")
        self.assertEqual(row[PLATFORM_STATUS_COL_5], "成功")

    def test_scan_recognizes_external_platform_prefix(self):
        with tempfile.TemporaryDirectory() as ptmp, tempfile.TemporaryDirectory() as itmp:
            self._write(ptmp, "testpay.json", {
                "key": "TESTPAY", "priority": 60, "handler": "generic5",
                "join_col": "商户订单号", "admin_join_col": ADMIN_JOIN_COL_5,
                "directions": {"payout": {"prefixes": ["testpay-"]}},
            })
            specs = load_platform_registry("5", platforms_dir=Path(ptmp))
            (Path(itmp) / "testpay-202604.xlsx").touch()
            (Path(itmp) / "admin-x.xls").touch()
            files = scan_source_files_5(Path(itmp), specs)
        self.assertEqual([p.name for p in files["TESTPAY"]], ["testpay-202604.xlsx"])
        self.assertEqual([p.name for p in files["admin"]], ["admin-x.xls"])

    def test_disabled_builtin_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write(tmp, "epin.json", {"key": "EPIN", "enabled": False})
            specs = load_platform_registry("5", platforms_dir=Path(tmp))
        self.assertNotIn("EPIN", _keys(specs))


class EpinFilterTests(unittest.TestCase):

    def test_epin_admin_match_filter_requires_numeric_org(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("5", platforms_dir=Path(tmp))
        # 只保留 EPIN,构造两行 admin:机构数字(命中) / 机构非数字(不命中)
        epin_specs = [s for s in specs if s.key == "EPIN"]
        admin = pd.DataFrame([
            {ADMIN_JOIN_COL_5: "A1", ADMIN_TP_ORDER_COL_5: "PIN-1", ADMIN_ORG_COL_5: "12345"},
            {ADMIN_JOIN_COL_5: "A2", ADMIN_TP_ORDER_COL_5: "PIN-2", ADMIN_ORG_COL_5: "非数字"},
        ])
        epin_lk = pd.DataFrame(
            [
                {"订单ID": "O-1", "单价(USD)": "5", "订单状态": "Başarılı", "确认时间": "2026-04-01", "产品": "150 TL"},
                {"订单ID": "O-2", "单价(USD)": "5", "订单状态": "Başarılı", "确认时间": "2026-04-01", "产品": "150 TL"},
            ],
            index=pd.Index(["PIN-1", "PIN-2"], name="Pin码"),
        )
        result = enrich_admin_columnar(admin, {"EPIN": epin_lk}, epin_specs, SCHEMA_5)
        keyed = {r[ADMIN_JOIN_COL_5]: r for _, r in result.iterrows() if r[ADMIN_JOIN_COL_5]}
        self.assertEqual(keyed["A1"][MATCH_STATUS_COL_5], "是")     # 机构纯数字 → 命中
        self.assertEqual(keyed["A1"][PLATFORM_ORDER_NO_COL_5], "O-1")
        self.assertEqual(keyed["A2"][MATCH_STATUS_COL_5], "否")     # 机构非数字 → 不命中


class PluginIsolationTests(unittest.TestCase):

    _BAD = "raise RuntimeError('插件故意报错')\n"

    def test_bad_plugin_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            plugins = base / "plugins"
            plugins.mkdir(parents=True, exist_ok=True)
            (plugins / "bad.py").write_text(self._BAD, encoding="utf-8")
            specs = load_platform_registry("5", platforms_dir=base)
        # 坏插件被跳过,内置 5 平台不受影响
        self.assertEqual(_keys(specs), ["IBFYPAY", "SUPERPAY", "WANGGUYPAY", "PHONECARD", "EPIN"])


if __name__ == "__main__":
    unittest.main()
