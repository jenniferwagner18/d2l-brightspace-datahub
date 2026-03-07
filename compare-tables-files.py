import duckdb
import pandas as pd
from pathlib import Path
import zipfile
import tempfile
from datetime import datetime
import re

# -----------------------
# Paths
# -----------------------
BASE_DIR = Path(__file__).parent
EDW_FOLDER = BASE_DIR / "EDW_Export"
OUTPUT_FOLDER = BASE_DIR / "DHUBvsEDW"
OUTPUT_FOLDER.mkdir(exist_ok=True)

DB_FILE = BASE_DIR / "brightspace.duckdb"

# Timestamp for filenames
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# Connect to DuckDB
con = duckdb.connect(str(DB_FILE))
con.execute("PRAGMA threads=8")

# -----------------------
# Helpers
# -----------------------
def is_valid_csv(name):
    return name.lower().endswith(".csv") and not name.startswith(".") and not name.startswith("._")

def clean_table_name(name: str) -> str:
    """Normalize names to match DuckDB table names."""
    name = name.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name.strip("_")

def count_rows_in_csv(zip_path):
    """Extract first CSV from ZIP and count rows accurately."""
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_files = [f for f in z.namelist() if is_valid_csv(f)]
        if not csv_files:
            return None, None
        csv_file = csv_files[0]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_csv_path = Path(tmpdir) / Path(csv_file).name
            with z.open(csv_file) as source, open(tmp_csv_path, 'wb') as dest:
                dest.write(source.read())
            # Correct row counting
            count = sum(len(chunk) for chunk in pd.read_csv(tmp_csv_path, chunksize=100_000, dtype=str))
    return csv_file, count

# -----------------------
# Compare EDW ZIPs to DHUB tables
# -----------------------
summary = []

# Get list of DuckDB tables
dhub_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

for zip_path in EDW_FOLDER.glob("*.zip"):
    # Count rows in the first CSV inside the ZIP
    csv_name, edw_count = count_rows_in_csv(zip_path)
    if csv_name is None:
        print(f"No CSV in {zip_path.name}, skipping")
        continue

    # Use ZIP filename stem to match DuckDB table
    table_name = clean_table_name(zip_path.stem)

    if table_name not in dhub_tables:
        print(f"{zip_path.name} exists in EDW but no matching DHUB table")
        dhub_count = None
        diff_created = False
    else:
        dhub_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        diff_created = False
        if edw_count != dhub_count:
            print(f"{zip_path.name}: EDW={edw_count}, DHUB={dhub_count} → writing differences")
            # Stream differences to CSV using a temporary table
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_csv_path = Path(tmpdir) / csv_name
                with zipfile.ZipFile(zip_path, 'r') as z:
                    z.extract(csv_name, tmpdir)
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE edw_tmp AS
                    SELECT * FROM read_csv_auto('{tmp_csv_path}', sample_size=-1, union_by_name=true, ignore_errors=true)
                """)
                output_path = OUTPUT_FOLDER / f"{zip_path.stem}-diff_{ts}.csv"
                con.execute(f"""
                    COPY (SELECT * FROM edw_tmp EXCEPT SELECT * FROM {table_name}
                          UNION ALL
                          SELECT * FROM {table_name} EXCEPT SELECT * FROM edw_tmp) 
                    TO '{output_path}' (HEADER)
                """)
                diff_created = output_path.exists() and output_path.stat().st_size > 0
                con.execute("DROP TABLE edw_tmp")

    summary.append({
        "File": zip_path.name,
        "Rows_EDW": edw_count,
        "Rows_DHUB": dhub_count,
        "Difference": None if dhub_count is None else edw_count - dhub_count,
        "Diff_File_Created": diff_created
    })

# -----------------------
# Save summary with timestamp
# -----------------------
summary_df = pd.DataFrame(summary)
for col in ["Rows_EDW", "Rows_DHUB", "Difference"]:
    summary_df[col] = summary_df[col].astype("Int64")

summary_csv_path = OUTPUT_FOLDER / f"compare_summary_{ts}.csv"
summary_df.to_csv(summary_csv_path, index=False)

print("\n=== Summary ===")
print(summary_df)
print(f"\nSummary saved to {summary_csv_path}")