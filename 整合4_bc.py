"""代号4 子功能入口：BC 平台（betcatpay）代收订单抓取。"""
import sys
from src.bank_integration import app4_bc

if __name__ == "__main__":
    sys.exit(app4_bc.main())
