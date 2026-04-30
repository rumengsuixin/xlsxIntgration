# -*- coding: utf-8 -*-
import unittest
import tempfile
from pathlib import Path

from src.bank_integration.balances import get_last_balance, get_monthly_balances
from src.bank_integration.config2 import (
    BANK_BALANCE_COL_2,
    BANK_DATE_COL_2,
    BANK_READ_CONFIG_2,
)
from src.bank_integration.readers import read_bank_file
from src.bank_integration.scanner import scan_source_files, scan_source_files_2


ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input" / "1"
INPUT_DIR_2 = ROOT / "data" / "input" / "2"
HUAMEI_PDF = ROOT / "华美银行电子对账单-2025.02.pdf"


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

    def test_mode2_east_asia_has_no_balance_column(self):
        cases = [
            "B-东亚银行-HKD.csv",
            "B-东亚银行-USD.csv",
        ]

        for filename in cases:
            with self.subTest(filename=filename):
                df = read_bank_file(
                    str(INPUT_DIR_2 / filename),
                    "东亚银行",
                    bank_read_config=BANK_READ_CONFIG_2,
                    bank_date_col=BANK_DATE_COL_2,
                )
                monthly = get_monthly_balances(
                    df,
                    "东亚银行",
                    balance_col_map=BANK_BALANCE_COL_2,
                    date_col_map=BANK_DATE_COL_2,
                )

                self.assertFalse(df.empty)
                self.assertEqual(monthly, [])

    def test_mode2_ocbc_extracts_monthly_balance(self):
        cases = {
            "D-华侨银行-HKD.csv": ("2026-04-13", 1716.58),
            "D-华侨银行-USD.csv": ("2026-04-24", 96049.90),
        }

        for filename, (expected_date, expected_balance) in cases.items():
            with self.subTest(filename=filename):
                df = read_bank_file(
                    str(INPUT_DIR_2 / filename),
                    "华侨银行",
                    bank_read_config=BANK_READ_CONFIG_2,
                    bank_date_col=BANK_DATE_COL_2,
                )
                monthly = get_monthly_balances(
                    df,
                    "华侨银行",
                    balance_col_map=BANK_BALANCE_COL_2,
                    date_col_map=BANK_DATE_COL_2,
                )

                self.assertIn("余额", df.columns)
                self.assertEqual(list(df.columns).count("交易日期"), 1)
                self.assertEqual(monthly[0][0], expected_date)
                self.assertAlmostEqual(monthly[0][1], expected_balance, places=2)

    def test_mode2_scanner_accepts_only_huamei_pdf(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "A-华美银行-USD.pdf").touch()
            (tmp_path / "B-汇丰银行-USD.pdf").touch()
            (tmp_path / "C-华侨银行-HKD.csv").touch()

            sources = scan_source_files_2(tmp_path)
            names = sorted(Path(item["filepath"]).name for item in sources)

            self.assertEqual(names, ["A-华美银行-USD.pdf", "C-华侨银行-HKD.csv"])

    @unittest.skipIf(not HUAMEI_PDF.exists(), "华美银行 PDF 测试文件不存在")
    def test_mode2_huamei_pdf_extracts_statement_month_last_daily_balance(self):
        df = read_bank_file(
            str(HUAMEI_PDF),
            "华美银行",
            bank_read_config=BANK_READ_CONFIG_2,
            bank_date_col=BANK_DATE_COL_2,
        )
        monthly = get_monthly_balances(
            df,
            "华美银行",
            balance_col_map=BANK_BALANCE_COL_2,
            date_col_map=BANK_DATE_COL_2,
        )

        self.assertEqual(list(df.columns), ["Date", "Amount"])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["Date"], "2025-02-04")
        self.assertAlmostEqual(df.iloc[0]["Amount"], 3297.03, places=2)
        self.assertEqual(monthly[0][0], "2025-02-04")
        self.assertAlmostEqual(monthly[0][1], 3297.03, places=2)


if __name__ == "__main__":
    unittest.main()
