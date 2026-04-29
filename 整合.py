# -*- coding: utf-8 -*-
"""Compatibility entry point for the bank integration workflow."""

import sys

from src.bank_integration.app import main


if __name__ == "__main__":
    sys.exit(main())
