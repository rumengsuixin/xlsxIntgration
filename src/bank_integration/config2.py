"""代号2（海外银行）路径与读取配置。"""

from .config import DATA_DIR, OUTPUT_DIR, TEMPLATE_DIR

SUMMARY_FILE_2 = "银行汇总.xlsx"
TEMPLATE_PATH_2 = TEMPLATE_DIR / "2" / SUMMARY_FILE_2
OUTPUT_PATH_2 = OUTPUT_DIR / SUMMARY_FILE_2
INPUT_DIR_2 = DATA_DIR / "input" / "2"

# 余额工作表名前缀（实际名称含年份，运行时动态匹配）
BALANCE_SHEET_PREFIX_2 = "MIG银行余额"

# 受保护工作表（不可覆盖）
PROTECTED_SHEETS_2 = {"汇率"}

# 每月块行数（含所有银行+币种行，无合计行）
BALANCE_BLOCK_SIZE_2 = 16

# 公司余额起始列（G列=7）和最大列（U列=21，对应公司O）
COMPANY_COL_START_2 = 7
COMPANY_COL_END_2 = 21  # inclusive

# 银行名称缩写
BANK_ABBR_2 = {
    "汇丰银行": "汇丰",
    "东亚银行": "东亚",
    "华侨银行": "华侨",
    "渣打银行空中云汇": "渣打",
    "华美银行": "华美",
    "大华银行（UOB)": "大华",
    "联昌国际银行（CIMB）": "联昌",
    "招商银行": "招行",
    "工商银行": "工行",
}

# 各银行读取配置
# encoding: 强制编码（覆盖默认的 utf-8-sig/gbk/utf-8 尝试顺序）
# col_map: 读取后对列重命名 {旧名: 新名}（支持 "Unnamed: N" 这类无名列）
# row_filter_col / row_filter_prefix: 只保留指定列以该前缀开头的行
# row_filter_col / row_filter_val: 只保留指定列（去空白后）等于该值的行
# strip_col_suffix_char: 列名中含该字符时截断（去除括号内容等）
BANK_READ_CONFIG_2 = {
    "汇丰银行": {
        "header": 1,
        "engine": None,
        "is_csv": True,
        "strip_col_suffix_char": "(",
    },
    "东亚银行": {
        "header": 0,
        "engine": None,
        "is_csv": True,
        "row_filter_col": "日期及时间",
        "row_filter_prefix": "总结余",
    },
    "华侨银行": {
        "header": 0,
        "engine": None,
        "is_csv": True,
        "encoding": "gbk",
        "col_map": {"Unnamed: 7": "交易日期"},
    },
    "渣打银行空中云汇": {
        "header": 0,
        "engine": "openpyxl",
        "is_csv": False,
    },
    "华美银行": {
        "header": 0,
        "engine": None,
        "is_csv": False,
        "is_pdf": True,
    },
    "大华银行（UOB)": {
        "header": 3,
        "engine": "openpyxl",
        "is_csv": False,
        "row_filter_col": "D1",
        "row_filter_val": "D2",
    },
    "联昌国际银行（CIMB）": {
        "header": 5,
        "engine": "openpyxl",
        "is_csv": False,
    },
    "招商银行": {
        "header": 12,
        "engine": "openpyxl",
        "is_csv": False,
    },
    "工商银行": {
        "header": 1,
        "engine": "openpyxl",
        "is_csv": False,
    },
}

# 各银行余额列名（空字符串表示该银行无余额列，跳过余额提取）
BANK_BALANCE_COL_2 = {
    "汇丰银行": "账面结余",
    "东亚银行": "",
    "华侨银行": "余额",
    "渣打银行空中云汇": "Account Balance",
    "华美银行": "Amount",
    "大华银行（UOB)": "Ledger Balance",
    "联昌国际银行（CIMB）": "Balance",
    "招商银行": "余额",
    "工商银行": "余额",
}

# 各银行日期列名
BANK_DATE_COL_2 = {
    "汇丰银行": "日期",
    "东亚银行": "日期及时间",
    "华侨银行": "交易日期",
    "渣打银行空中云汇": "Time",
    "华美银行": "Date",
    "大华银行（UOB)": "Value Date",
    "联昌国际银行（CIMB）": "Transaction Date",
    "招商银行": "交易日",
    "工商银行": "交易时间",
}

# 农业银行类似的行过滤（此处无，但为兼容读取器接口）
BANK_DATE_FILTER_2: dict = {}
