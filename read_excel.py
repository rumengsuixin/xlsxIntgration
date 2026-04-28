# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, 'E:\python3130\Lib\site-packages')

try:
    import openpyxl
    print("openpyxl imported successfully")
except:
    print("openpyxl not found, trying alternative")

# Try with pandas
try:
    import pandas as pd
    print("pandas available")
except:
    print("pandas not available")

