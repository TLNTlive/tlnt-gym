import pandas as pd
import json

xls = pd.ExcelFile('TLNT_Gymdesk_Final_Package_v3 (1).xlsx')

all_data = {}

for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name, dtype=str)
    df = df.fillna('')
    all_data[sheet_name] = {
        "columns": list(df.columns),
        "row_count": len(df),
        "rows": df.to_dict(orient='records')
    }

with open('spreadsheet_data.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False, default=str)

print("Done. Sheets extracted:")
for sheet, data in all_data.items():
    print(f"  - '{sheet}': {data['row_count']} rows, {len(data['columns'])} columns")
    print(f"    Columns: {data['columns']}")
