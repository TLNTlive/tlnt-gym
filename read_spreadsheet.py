import pandas as pd
import sys

# Read the Excel file
xls = pd.ExcelFile('TLNT_Gymdesk_Final_Package_v3 (1).xlsx')

print(f"Total sheets in workbook: {len(xls.sheet_names)}")
print(f"Sheet names: {xls.sheet_names}\n")
print("="*100)

# Read and display each sheet
for sheet_name in xls.sheet_names:
    print(f"\n\n### SHEET: {sheet_name}")
    print("="*100)
    
    df = pd.read_excel(xls, sheet_name)
    
    print(f"\nDimensions: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"\nColumn names: {list(df.columns)}\n")
    
    # Set pandas display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print(df.to_string(index=False))
    print("\n" + "="*100)
