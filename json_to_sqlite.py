import json
import sqlite3
import re

def sanitize_col(name):
    """Convert column name to a safe SQLite identifier."""
    name = str(name).strip()
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if not name or name[0].isdigit():
        name = 'col_' + name
    return name

def json_to_sqlite(json_path='spreadsheet_data.json', db_path='gym_data.db'):
    print(f"Loading {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for sheet_name, sheet_data in all_data.items():
        rows = sheet_data.get('rows', [])
        if not rows:
            print(f"  Skipping '{sheet_name}' (no rows)")
            continue

        # Build safe table name
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', sheet_name).strip('_')
        table_name = re.sub(r'_+', '_', table_name)

        # Build column names from first row keys
        raw_cols = list(rows[0].keys())
        safe_cols = [sanitize_col(c) for c in raw_cols]

        # Handle duplicate column names
        seen = {}
        deduped_cols = []
        for col in safe_cols:
            if col in seen:
                seen[col] += 1
                deduped_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                deduped_cols.append(col)

        # Drop and recreate the table
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        col_defs = ', '.join(f'"{c}" TEXT' for c in deduped_cols)
        cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

        # Insert rows
        placeholders = ', '.join('?' for _ in deduped_cols)
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        for row in rows:
            values = [str(row.get(raw, '')) for raw in raw_cols]
            cur.execute(insert_sql, values)

        conn.commit()
        print(f"  Created table '{table_name}': {len(rows)} rows, {len(deduped_cols)} columns")

    conn.close()
    print(f"\nDone. Database saved to: {db_path}")
    print("You can now query it with: sqlite3 gym_data.db")

if __name__ == '__main__':
    json_to_sqlite()
