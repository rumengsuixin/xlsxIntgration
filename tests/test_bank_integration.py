# -*- coding: utf-8 -*-
import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "整合.py"


def load_merge_module():
    spec = importlib.util.spec_from_file_location("bank_merge", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BankIntegrationSampleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_merge_module()

    def test_scan_source_files_only_recognizes_prefixed_samples(self):
        sources = self.mod.scan_source_files(str(ROOT))
        names = sorted(path.name for path in (Path(item["filepath"]) for item in sources))

        self.assertEqual(
            names,
            [
                "A-中信银行.xlsx",
                "A-招商银行.xlsx",
                "B-建设银行.xls",
                "B-浦发银行.xls",
                "C-工商银行.xlsx",
            ],
        )

    def test_read_samples_and_extract_latest_balance_by_date(self):
        cases = {
            "A-中信银行.xlsx": ("中信银行", 1, "交易日期", "账户余额", "2026-03-21", 4402.82),
            "A-招商银行.xlsx": ("招商银行", 2, "交易日", "余额", "2026-03-21", 467.96),
            "B-建设银行.xls": ("建设银行", 2, "交易时间", "余额", "2026-03-21", 28.05),
            "B-浦发银行.xls": ("浦发银行", 45, "交易日期", "余额", "2026-03-31", 6919.8),
            "C-工商银行.xlsx": ("工商银行", 4, "交易时间", "余额", "2026-03-27", 1065.78),
        }

        for filename, (bank_name, rows, date_col, balance_col, exp_date, exp_balance) in cases.items():
            with self.subTest(filename=filename):
                df = self.mod.read_bank_file(str(ROOT / filename), bank_name)
                balance_date, balance = self.mod.get_last_balance(df, bank_name)

                self.assertEqual(len(df), rows)
                self.assertIn(date_col, df.columns)
                self.assertIn(balance_col, df.columns)
                self.assertEqual(balance_date, exp_date)
                self.assertAlmostEqual(balance, exp_balance, places=2)

    def test_summary_template_is_required(self):
        self.assertFalse(hasattr(self.mod, "create_summary_file"))


if __name__ == "__main__":
    unittest.main()
