# -*- coding: utf-8 -*-
import unittest
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from src.bank_integration.balances import (
    get_last_balance,
    get_monthly_balances,
    update_balance_sheet,
    update_balance_sheet_2,
)
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
        bank_names = sorted(item["bank_name"] for item in sources)

        self.assertEqual(len(names), 7)
        self.assertIn("智星-中信银行.xlsx", names)
        self.assertEqual(
            bank_names,
            ["中信银行", "中国银行", "农业银行", "工商银行", "建设银行", "招商银行", "浦发银行"],
        )

    def test_read_samples_and_extract_latest_balance_by_date(self):
        cases = {
            "中信银行": ("交易日期", "账户余额"),
            "招商银行": ("交易日", "余额"),
            "建设银行": ("交易时间", "余额"),
            "浦发银行": ("交易日期", "余额"),
            "工商银行": ("交易时间", "余额"),
            "中国银行": ("交易日期", "交易后余额"),
            "农业银行": ("交易时间", "账户余额"),
        }
        sources = {item["bank_name"]: item["filepath"] for item in scan_source_files(INPUT_DIR)}

        for bank_name, (date_col, balance_col) in cases.items():
            with self.subTest(bank_name=bank_name):
                df = read_bank_file(sources[bank_name], bank_name)
                balance_date, balance = get_last_balance(df, bank_name)

                self.assertEqual(len(df), 12)
                self.assertIn(date_col, df.columns)
                self.assertIn(balance_col, df.columns)
                self.assertEqual(balance_date, "2026-12-28")
                self.assertAlmostEqual(balance, 11481.40, places=2)

    def test_summary_template_is_required(self):
        import src.bank_integration.workbook as workbook

        self.assertFalse(hasattr(workbook, "create_summary_file"))

    def test_scan_source_files_accepts_chinese_company_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "A-中信银行.xlsx").touch()
            (tmp_path / "瑞泽商务-中信银行.xlsx").touch()
            (tmp_path / "瑞泽商务-未知银行.xlsx").touch()
            (tmp_path / "中信银行.xlsx").touch()

            sources = scan_source_files(tmp_path)
            by_name = {Path(item["filepath"]).name: item for item in sources}

            self.assertEqual(set(by_name), {"A-中信银行.xlsx", "瑞泽商务-中信银行.xlsx"})
            self.assertEqual(by_name["A-中信银行.xlsx"]["company"], "A")
            self.assertEqual(by_name["瑞泽商务-中信银行.xlsx"]["company"], "瑞泽商务")
            self.assertEqual(by_name["瑞泽商务-中信银行.xlsx"]["bank_name"], "中信银行")

    def test_update_balance_sheet_uses_existing_company_header(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "银行余额"
        ws.append(["日期", "类别", None, "合计", "A", "B"])
        ws.append(["2026-12-31", "银行存款", "中信银行", "=SUM(E2:F2)", None, None])

        update_balance_sheet(ws, "中信银行", "A", "2026-12-28", 123.45)

        self.assertEqual(ws.cell(row=2, column=5).value, 123.45)
        self.assertEqual(ws.cell(row=1, column=7).value, None)
        self.assertEqual(ws.cell(row=2, column=4).value, "=SUM(E2:F2)")

    def test_update_balance_sheet_appends_missing_company_header_and_formula(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "银行余额"
        ws.append(["日期", "类别", None, "合计", "A", "B"])
        ws.append(["2026-12-31", "银行存款", "中信银行", "=SUM(E2:F2)", None, None])
        ws.append([None, None, "招商银行", "=SUM(E3:F3)", None, None])

        update_balance_sheet(ws, "中信银行", "瑞泽商务", "2026-12-28", 456.78)

        self.assertEqual(ws.cell(row=1, column=7).value, "瑞泽商务")
        self.assertEqual(ws.cell(row=2, column=7).value, 456.78)
        self.assertEqual(ws.cell(row=2, column=4).value, "=SUM(E2:G2)")
        self.assertEqual(ws.cell(row=3, column=4).value, "=SUM(E3:G3)")

    def test_boc_date_range_fills_months_before_first_transaction(self):
        df = pd.DataFrame(
            [
                {"交易日期": "20260320", "交易金额": "+0.81", "交易后余额": "6,475.08"},
                {"交易日期": "20260326", "交易金额": "-400.00", "交易后余额": "6,075.08"},
            ]
        )
        df.attrs["statement_date_range"] = (date(2026, 1, 1), date(2026, 3, 31))

        monthly = get_monthly_balances(df, "中国银行")

        self.assertEqual(monthly[0][0], "2026-01-31")
        self.assertAlmostEqual(monthly[0][1], 6474.27, places=2)
        self.assertEqual(monthly[1][0], "2026-02-28")
        self.assertAlmostEqual(monthly[1][1], 6474.27, places=2)
        self.assertEqual(monthly[2][0], "2026-03-26")
        self.assertAlmostEqual(monthly[2][1], 6075.08, places=2)

    def test_cmb_debit_first_transaction_backfills_prior_months(self):
        df = pd.DataFrame(
            [
                {
                    "交易日": "2026-03-03",
                    "借方金额": "100.00",
                    "贷方金额": "",
                    "余额": "900.00",
                }
            ]
        )
        df.attrs["statement_date_range"] = (date(2026, 1, 1), date(2026, 3, 31))

        monthly = get_monthly_balances(df, "招商银行")

        self.assertEqual(monthly, [("2026-01-31", 1000.0), ("2026-02-28", 1000.0), ("2026-03-03", 900.0)])

    def test_citic_credit_first_transaction_backfills_prior_months(self):
        df = pd.DataFrame(
            [
                {
                    "交易日期": "2026-04-10",
                    "借方发生额": "",
                    "贷方发生额": "50.00",
                    "账户余额": "1,050.00",
                }
            ]
        )
        df.attrs["statement_date_range"] = (date(2026, 2, 1), date(2026, 4, 30))

        monthly = get_monthly_balances(df, "中信银行")

        self.assertEqual(monthly, [("2026-02-28", 1000.0), ("2026-03-31", 1000.0), ("2026-04-10", 1050.0)])

    def test_missing_middle_month_uses_previous_transaction_balance(self):
        df = pd.DataFrame(
            [
                {"交易日": "2026-01-15", "借方金额": "", "贷方金额": "100.00", "余额": "1,000.00"},
                {"交易日": "2026-03-10", "借方金额": "", "贷方金额": "200.00", "余额": "1,200.00"},
            ]
        )
        df.attrs["statement_date_range"] = (date(2026, 1, 1), date(2026, 3, 31))

        monthly = get_monthly_balances(df, "招商银行")

        self.assertEqual(monthly, [("2026-01-15", 1000.0), ("2026-02-28", 1000.0), ("2026-03-10", 1200.0)])

    def test_missing_month_fill_requires_mode1_default_maps(self):
        df = pd.DataFrame(
            [{"交易日": "2026-03-03", "借方金额": "100.00", "贷方金额": "", "余额": "900.00"}]
        )
        df.attrs["statement_date_range"] = (date(2026, 1, 1), date(2026, 3, 31))

        monthly = get_monthly_balances(
            df,
            "招商银行",
            balance_col_map=BANK_BALANCE_COL_2,
            date_col_map=BANK_DATE_COL_2,
        )

        self.assertEqual(monthly, [("2026-03-03", 900.0)])

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
            (tmp_path / "瑞泽商务-华侨银行-HKD.csv").touch()
            (tmp_path / "B-汇丰银行-USD.pdf").touch()
            (tmp_path / "C-华侨银行-HKD.csv").touch()
            (tmp_path / "瑞泽商务-未知银行-HKD.csv").touch()

            sources = scan_source_files_2(tmp_path)
            by_name = {Path(item["filepath"]).name: item for item in sources}

            self.assertEqual(
                set(by_name),
                {"A-华美银行-USD.pdf", "C-华侨银行-HKD.csv", "瑞泽商务-华侨银行-HKD.csv"},
            )
            self.assertEqual(by_name["A-华美银行-USD.pdf"]["company"], "A")
            self.assertEqual(by_name["瑞泽商务-华侨银行-HKD.csv"]["company"], "瑞泽商务")
            self.assertEqual(by_name["瑞泽商务-华侨银行-HKD.csv"]["bank_name"], "华侨银行")
            self.assertEqual(by_name["瑞泽商务-华侨银行-HKD.csv"]["currency"], "HKD")

    def test_update_balance_sheet_2_uses_existing_company_header(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "MIG银行余额（20261231）"
        ws.append(["日期", None, None, "折合人民币", "合计", "币种", "A", "B"])
        ws.append(["2026-04-30", None, "华侨银行", None, "=SUM(G2:H2)", "HKD", None, None])
        ws.append([None, None, None, None, "=SUM(G3:H3)", "USD", None, None])

        update_balance_sheet_2(ws, "华侨银行", "HKD", "A", "2026-04-13", 123.45)

        self.assertEqual(ws.cell(row=2, column=7).value, 123.45)
        self.assertEqual(ws.cell(row=1, column=9).value, None)
        self.assertEqual(ws.cell(row=2, column=5).value, "=SUM(G2:H2)")
        self.assertIn("MATCH(F2", ws.cell(row=2, column=4).value)

    def test_update_balance_sheet_2_appends_missing_company_header_and_formula(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "MIG银行余额（20261231）"
        ws.append(["日期", None, None, "折合人民币", "合计", "币种", "A", "B"])
        ws.append(["2026-04-30", None, "华侨银行", None, "=SUM(G2:H2)", "HKD", None, None])
        ws.append([None, None, None, None, "=SUM(G3:H3)", "USD", None, None])

        update_balance_sheet_2(ws, "华侨银行", "USD", "瑞泽商务", "2026-04-24", 456.78)

        self.assertEqual(ws.cell(row=1, column=5).value, "合计")
        self.assertEqual(ws.cell(row=1, column=9).value, "瑞泽商务")
        self.assertEqual(ws.cell(row=3, column=9).value, 456.78)
        self.assertEqual(ws.cell(row=2, column=5).value, "=SUM(G2:I2)")
        self.assertEqual(ws.cell(row=3, column=5).value, "=SUM(G3:I3)")
        self.assertIn("MATCH(F3", ws.cell(row=3, column=4).value)

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
