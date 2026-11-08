import json
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from paper_pipeline import get_paths
_, db = get_paths()
if not db.is_file():
    raise SystemExit('No papers database yet. Run the DAG first.')
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
try:
    rows = [dict(row) for row in conn.execute('SELECT * FROM papers ORDER BY filename')]
finally:
    conn.close()
for row in rows:
    row['keywords'] = json.loads(row['keywords'])
print(json.dumps(rows, indent=2, ensure_ascii=False))
