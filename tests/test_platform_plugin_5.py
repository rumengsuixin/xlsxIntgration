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
from src.bank_integration.platform_engine import (
    aggregate_by_keys,
    derive_series,
    enrich_admin_columnar,
    reconcile_aggregate,
    resolve_handler,
)
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
        self.assertEqual(_keys(specs), ["IBFYPAY", "SUPERPAY", "WANGGUYPAY", "PHONECARD", "EPIN", "BINANCE"])
        self.assertEqual([s.priority for s in specs], [10, 20, 30, 40, 50, 60])

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
        # 坏插件被跳过,内置平台不受影响
        self.assertEqual(_keys(specs), ["IBFYPAY", "SUPERPAY", "WANGGUYPAY", "PHONECARD", "EPIN", "BINANCE"])


class DeriveSeriesTests(unittest.TestCase):
    """聚合派生原语:去前缀 / 正则取值 / 缺列返回空串。"""

    def test_strip_prefix_and_regex(self):
        df = pd.DataFrame({
            "其他": ["BIN-1001", "BIN-1002", "OTHER"],
            "奖品名称": ["USDT 3.0", "USDT 12", "无面额"],
        })
        ids = list(derive_series(df, "其他", strip_prefix="BIN-"))
        self.assertEqual(ids, ["1001", "1002", "OTHER"])
        amts = list(derive_series(df, "奖品名称", regex=r"USDT\s*([\d.]+)"))
        self.assertEqual(amts, ["3.0", "12", ""])          # 末行无匹配 → 空串
        missing = list(derive_series(df, "不存在列"))
        self.assertEqual(missing, ["", "", ""])            # 缺列不报错


class ReconcileAggregateTests(unittest.TestCase):
    """四状态判定 + 三种日期口径(exact / period / t1_window)。"""

    def _admin(self):
        # (id, date, amt):三个收款ID,同一 admin 业务日 07-01
        return aggregate_by_keys(
            ["1001", "1002", "1003"],
            ["2026-07-01", "2026-07-01", "2026-07-01"],
            ["3.0", "4.0", "2.0"],
        )

    def _platform_next_day(self):
        # 平台在次日 07-02 打款:1001 金额相等 / 1002 金额不符 / 1005 admin 无
        return aggregate_by_keys(
            ["1001", "1002", "1005"],
            ["2026-07-02", "2026-07-02", "2026-07-02"],
            ["3.0", "5.0", "7.0"],
        )

    def _by_key(self, df):
        return {(r["date"], r["id"]): r for _, r in df.iterrows()}

    def test_t1_window_absorbs_next_day_payout(self):
        out = reconcile_aggregate(self._admin(), self._platform_next_day(),
                                  date_match_mode="t1_window")
        k = self._by_key(out)
        self.assertEqual(k[("2026-07-01", "1001")]["status"], "一致")
        self.assertEqual(k[("2026-07-01", "1002")]["status"], "金额不符")
        self.assertAlmostEqual(k[("2026-07-01", "1002")]["diff"], 1.0)   # 平台5 - admin4
        self.assertEqual(k[("2026-07-01", "1003")]["status"], "平台缺失")
        self.assertEqual(k[("2026-07-02", "1005")]["status"], "平台多余")
        self.assertEqual(len(out), 4)

    def test_exact_does_not_cross_day(self):
        # 严格按天:平台 07-02 与 admin 07-01 全部错位 → 3 缺失 + 3 多余,无一致
        out = reconcile_aggregate(self._admin(), self._platform_next_day(),
                                  date_match_mode="exact")
        statuses = list(out["status"])
        self.assertEqual(statuses.count("平台缺失"), 3)
        self.assertEqual(statuses.count("平台多余"), 3)
        self.assertNotIn("一致", statuses)

    def test_period_ignores_date(self):
        # 整期:忽略日期只按 id 汇总
        out = reconcile_aggregate(self._admin(), self._platform_next_day(),
                                  date_match_mode="period")
        k = {r["id"]: r for _, r in out.iterrows()}
        self.assertTrue(all(r["date"] == "" for _, r in out.iterrows()))
        self.assertEqual(k["1001"]["status"], "一致")
        self.assertEqual(k["1002"]["status"], "金额不符")
        self.assertEqual(k["1003"]["status"], "平台缺失")
        self.assertEqual(k["1005"]["status"], "平台多余")

    def test_tolerance_marks_small_diff_consistent(self):
        admin = aggregate_by_keys(["1001"], ["2026-07-01"], ["3.00"])
        plat = aggregate_by_keys(["1001"], ["2026-07-01"], ["3.005"])
        out = reconcile_aggregate(admin, plat, date_match_mode="exact", tolerance=0.01)
        self.assertEqual(out.iloc[0]["status"], "一致")


class BinanceAggregateHandlerTests(unittest.TestCase):
    """BINANCE 走注册表 + aggregate_recon handler 端到端(原始模板 header=1 + 文件名取日)。"""

    def _spec(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = load_platform_registry("5", platforms_dir=Path(tmp))
        return next(s for s in specs if s.key == "BINANCE")

    def test_spec_is_aggregate(self):
        spec = self._spec()
        self.assertEqual(spec.handler, "aggregate_recon")
        self.assertEqual(spec.recon_mode, "aggregate")
        self.assertEqual(spec.recon["date_match_mode"], "t1_window")
        self.assertEqual(spec.recon["output_sheet"], "Binance-USDT对账")
        self.assertEqual(spec.recon["platform"]["header_row"], 1)

    def _write_platform(self, path):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Binance Pay Payout Template"
        ws.append(["批量代付模板(标题行)"])                       # 第1行大标题,header=1 跳过
        ws.append(["Account Type (Required)",
                   "Recipient's Account information (Required)",
                   "Crypto Currency (Required)",
                   "Amount (Required)"])                            # 第2行=列头
        ws.append(["Binance ID", "1001", "USDT", "3.0"])
        ws.append(["Binance ID", "1002", "USDT", "5.0"])
        ws.append(["Binance ID", "1005", "USDT", "7.0"])
        wb.save(path)

    def test_build_reconciliation_end_to_end(self):
        spec = self._spec()
        admin = pd.DataFrame([
            {"其他": "BIN-1001", "状态": "已完成", "奖品名称": "USDT 3.0", "日期": "2026-07-01"},
            {"其他": "BIN-1002", "状态": "已完成", "奖品名称": "USDT 4.0", "日期": "2026-07-01"},
            {"其他": "BIN-1003", "状态": "已完成", "奖品名称": "USDT 2.0", "日期": "2026-07-01"},
            {"其他": "BIN-1004", "状态": "已取消", "奖品名称": "USDT 9.0", "日期": "2026-07-01"},
        ])
        with tempfile.TemporaryDirectory() as tmp:
            # 文件名带打款日 07-02(admin 07-01 的次日),验证 T+1 归属
            fp = Path(tmp) / "USDT奖品发放信息2026-07-02.xlsx"
            self._write_platform(fp)
            sheet_name, out = resolve_handler(spec).build_reconciliation(spec, admin, [fp])
        self.assertEqual(sheet_name, "Binance-USDT对账")
        self.assertEqual(list(out.columns), [
            "日期", "收款ID", "admin应付USDT", "admin笔数",
            "平台实付USDT", "平台笔数", "差额", "对账状态",
        ])
        k = {(r["日期"], r["收款ID"]): r for _, r in out.iterrows()}
        self.assertEqual(k[("2026-07-01", "1001")]["对账状态"], "一致")        # 3.0=3.0
        self.assertEqual(k[("2026-07-01", "1002")]["对账状态"], "金额不符")     # 5 vs 4
        self.assertAlmostEqual(k[("2026-07-01", "1002")]["差额"], 1.0)
        self.assertEqual(k[("2026-07-01", "1003")]["对账状态"], "平台缺失")
        self.assertEqual(k[("2026-07-02", "1005")]["对账状态"], "平台多余")
        # 已取消的 1004 不计入应付,故不应出现
        self.assertNotIn(("2026-07-01", "1004"), k)


if __name__ == "__main__":
    unittest.main()
