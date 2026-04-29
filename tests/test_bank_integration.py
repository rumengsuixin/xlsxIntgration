# -*- coding: utf-8 -*-
import unittest
from pathlib import Path

from src.bank_integration.balances import get_last_balance
from src.bank_integration.readers import read_bank_file
from src.bank_integration.scanner import scan_source_files


ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input"


class BankIntegrationSampleTests(unittest.TestCase):
    def test_scan_source_files_only_recognizes_prefixed_samples(self):
        sources = scan_source_files(INPUT_DIR)
        names = sorted(path.name for path in (Path(item["filepath"]) for item in sources))

        self.assertEqual(
            names,
            [
                "A-中信银行.xlsx",
                "B-招商银行.xlsx",
                "C-建设银行.xls",
                "D-浦发银行.xls",
                "E-工商银行.xlsx",
                "F-中国银行.csv",
                "G-农业银行.xls",
            ],
        )

    def test_read_samples_and_extract_latest_balance_by_date(self):
        cases = {
            "A-中信银行.xlsx": ("中信银行", "交易日期", "账户余额"),
            "B-招商银行.xlsx": ("招商银行", "交易日", "余额"),
            "C-建设银行.xls": ("建设银行", "交易时间", "余额"),
            "D-浦发银行.xls": ("浦发银行", "交易日期", "余额"),
            "E-工商银行.xlsx": ("工商银行", "交易时间", "余额"),
            "F-中国银行.csv": ("中国银行", "交易日期", "交易后余额"),
            "G-农业银行.xls": ("农业银行", "交易时间", "账户余额"),
        }

        for filename, (bank_name, date_col, balance_col) in cases.items():
            with self.subTest(filename=filename):
                df = read_bank_file(str(INPUT_DIR / filename), bank_name)
                balance_date, balance = get_last_balance(df, bank_name)

                self.assertEqual(len(df), 12)
                self.assertIn(date_col, df.columns)
                self.assertIn(balance_col, df.columns)
                self.assertEqual(balance_date, "2026-12-28")
                self.assertAlmostEqual(balance, 11481.40, places=2)

    def test_summary_template_is_required(self):
        import src.bank_integration.workbook as workbook

        self.assertFalse(hasattr(workbook, "create_summary_file"))


if __name__ == "__main__":
    unittest.main()
