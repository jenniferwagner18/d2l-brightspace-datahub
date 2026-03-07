import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

# -----------------------
# Setup
# -----------------------
BASE_DIR = Path(__file__).parent
DB_FILE = BASE_DIR / "brightspace.duckdb"
OUTPUT_FOLDER = BASE_DIR / "Row_Counts"
OUTPUT_FOLDER.mkdir(exist_ok=True)

# Timestamp for filename
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# Connect to DuckDB
con = duckdb.connect(str(DB_FILE))
con.execute("PRAGMA threads=8")  # speed up parallel processing

# -----------------------
# Get all tables
# -----------------------
tables = con.execute("SHOW TABLES").fetchall()
tables = [t[0] for t in tables]
print(f"Found {len(tables)} tables:\n{tables}\n")

# -----------------------
# Count rows per table
# -----------------------
summary = []
for table in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    summary.append({"Table": table, "Row_Count": count})
    print(f"{table}: {count} rows")

# -----------------------
# Save summary to CSV with timestamp
# -----------------------
df = pd.DataFrame(summary)
summary_csv_path = OUTPUT_FOLDER / f"row_counts_{ts}.csv"
df.to_csv(summary_csv_path, index=False)
print(f"\nRow counts saved to {summary_csv_path}")