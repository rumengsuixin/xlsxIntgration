# -*- coding: utf-8 -*-
"""Convert a text-based PDF statement to an Excel workbook."""

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable, List, Optional

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bank_integration.pdf_daily_balance import (  # noqa: E402
    extract_daily_balance_rows,
    extract_pdf_text,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output"


def clean_cell(value):
    """Normalize PDF cell text while preserving empty cells."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def safe_sheet_name(name: str, used: set) -> str:
    """Return an Excel-safe, unique worksheet name."""
    base = re.sub(r"[:\\/?*\[\]]", "_", name).strip()[:31] or "Sheet"
    candidate = base
    index = 1
    while candidate in used:
        suffix = f"_{index}"
        candidate = f"{base[:31 - len(suffix)]}{suffix}"
        index += 1
    used.add(candidate)
    return candidate


def write_rows(wb: Workbook, sheet_name: str, rows: Iterable[List[Optional[str]]], used: set) -> None:
    ws = wb.create_sheet(safe_sheet_name(sheet_name, used))
    for row in rows:
        ws.append([clean_cell(cell) for cell in row])


def extract_text_rows(page) -> List[List[Optional[str]]]:
    """Fallback extraction: group words by their vertical position."""
    words = page.extract_words(
        x_tolerance=2,
        y_tolerance=3,
        keep_blank_chars=False,
        use_text_flow=True,
    )
    if not words:
        text = page.extract_text() or ""
        return [[line] for line in text.splitlines() if line.strip()]

    grouped = []
    current_top = None
    current_words = []
    for word in sorted(words, key=lambda w: (round(float(w["top"]), 1), float(w["x0"]))):
        top = float(word["top"])
        if current_top is None or abs(top - current_top) <= 3:
            current_words.append(word)
            if current_top is None:
                current_top = top
        else:
            grouped.append(current_words)
            current_words = [word]
            current_top = top
    if current_words:
        grouped.append(current_words)

    rows = []
    for line_words in grouped:
        line_words = sorted(line_words, key=lambda w: float(w["x0"]))
        rows.append([" ".join(w["text"] for w in line_words)])
    return rows


def convert_pdf_to_excel(pdf_path: Path, output_path: Path) -> int:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 文件不存在: {pdf_path}")
    if pdf_path.suffix.lower() != ".pdf":
        raise ValueError(f"输入文件不是 PDF: {pdf_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    default_sheet = wb.active
    wb.remove(default_sheet)
    used_sheet_names = set()
    sheet_count = 0

    full_text = extract_pdf_text(pdf_path)
    daily_balance_pairs = extract_daily_balance_rows(full_text)
    if daily_balance_pairs:
        write_rows(wb, "Daily_Balances", [["Date", "Amount"], *daily_balance_pairs], used_sheet_names)
        sheet_count = 1
    else:
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables() or []
                non_empty_tables = [
                    table
                    for table in tables
                    if table and any(any(clean_cell(cell) for cell in row) for row in table)
                ]

                if non_empty_tables:
                    for table_index, table in enumerate(non_empty_tables, start=1):
                        write_rows(
                            wb,
                            f"Page{page_index}_Table{table_index}",
                            table,
                            used_sheet_names,
                        )
                        sheet_count += 1
                else:
                    rows = extract_text_rows(page)
                    if rows:
                        write_rows(wb, f"Page{page_index}_Text", rows, used_sheet_names)
                        sheet_count += 1

    if sheet_count == 0:
        write_rows(wb, "No_Content", [["未从 PDF 中提取到文本或表格"]], used_sheet_names)

    wb.save(output_path)
    return sheet_count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="将文本型 PDF 对账单转换为 Excel 文件。")
    parser.add_argument("pdf", help="PDF 文件路径")
    parser.add_argument(
        "-o",
        "--output",
        help="输出 Excel 路径。默认写入 data/output/{PDF文件名}.xlsx",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    pdf_path = Path(args.pdf)
    if not pdf_path.is_absolute():
        pdf_path = (Path.cwd() / pdf_path).resolve()

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR / f"{pdf_path.stem}.xlsx"
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()

    sheet_count = convert_pdf_to_excel(pdf_path, output_path)
    print(f"已生成: {output_path}")
    print(f"工作表数量: {sheet_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
