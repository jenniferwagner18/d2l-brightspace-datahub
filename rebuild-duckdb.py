import duckdb
import pathlib
import hashlib
import pickle
import zipfile
import shutil
import tempfile
import re
import pandas as pd
from datetime import datetime

# -------------------------
# Paths relative to the script
# -------------------------
BASE_DIR = pathlib.Path(__file__).parent
DATA_FOLDER = BASE_DIR / "DataHub_Export"
DB_FILE = BASE_DIR / "brightspace.duckdb"
HASH_FILE = BASE_DIR / "zip_hashes.pkl"
ROW_COUNT_FILE = BASE_DIR / "table_row_counts.pkl"
OUTPUT_FOLDER = BASE_DIR / "Row_Counts"
OUTPUT_FOLDER.mkdir(exist_ok=True)

# -------------------------
# Compute MD5 hash of file contents
# -------------------------
def file_md5(path: pathlib.Path, block_size=65536):
    hash_md5 = hashlib.md5()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            hash_md5.update(block)
    return hash_md5.hexdigest()

# -------------------------
# Compute hash for all ZIPs
# -------------------------
def compute_zip_hash(folder: pathlib.Path):
    zip_files = sorted(folder.glob("*.zip"))
    hash_dict = {}
    for z in zip_files:
        hash_dict[z.name] = file_md5(z)
    return hash_dict

# -------------------------
# Load previous hash
# -------------------------
current_hash = compute_zip_hash(DATA_FOLDER)
try:
    with open(HASH_FILE, "rb") as f:
        old_hash = pickle.load(f)
except FileNotFoundError:
    old_hash = {}

# -------------------------
# Connect to DuckDB
# -------------------------
con = duckdb.connect(DB_FILE)
con.execute("PRAGMA threads=8")  # optional speed-up

# -------------------------
# Load previous row counts or initialize
# -------------------------
try:
    with open(ROW_COUNT_FILE, "rb") as f:
        row_counts = pickle.load(f)
except FileNotFoundError:
    print("Row count file not found. Initializing from existing tables...")
    row_counts = {}
    for table in con.execute("SHOW TABLES").fetchall():
        table_name = table[0]
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        row_counts[table_name] = count
    with open(ROW_COUNT_FILE, "wb") as f:
        pickle.dump(row_counts, f)

# -------------------------
# Detect new or changed ZIPs
# -------------------------
to_load = []
skipped = []
for name, hash_val in current_hash.items():
    table_name = re.sub(r'[^a-z0-9]+', '_', pathlib.Path(name).stem.lower()).strip("_")
    if name not in old_hash or old_hash[name] != hash_val:
        to_load.append(name)
    else:
        skipped.append(name)

# -------------------------
# Detect deleted ZIPs
# -------------------------
existing_zip_names = set(current_hash.keys())
previous_zip_names = set(old_hash.keys())

deleted_zips = previous_zip_names - existing_zip_names
deleted_tables = []

for zip_name in deleted_zips:
    table_name = re.sub(r'[^a-z0-9]+', '_', pathlib.Path(zip_name).stem.lower()).strip("_")
    print(f"ZIP deleted: {zip_name} → Dropping table {table_name}")
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    deleted_tables.append(table_name)
    # Remove from row_counts and hash tracking
    row_counts.pop(table_name, None)
    old_hash.pop(zip_name, None)

if deleted_tables:
    print(f"Dropped tables ({len(deleted_tables)}): {', '.join(deleted_tables)}")

# -------------------------
# Load new/updated ZIPs
# -------------------------
updated_tables = []
if not to_load:
    print("No new or updated ZIP files detected. Skipping rebuild.")
else:
    print(f"{len(to_load)} new/updated ZIP(s) detected. Loading into DuckDB...")
    for zip_name in to_load:
        zip_path = DATA_FOLDER / zip_name
        table_name = re.sub(r'[^a-z0-9]+', '_', zip_path.stem.lower()).strip("_")
        print(f"\nLoading {zip_name} → table {table_name}")

        old_count = row_counts.get(table_name, 0)

        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                print(f"⚠️ No CSV found in {zip_name}, skipping")
                continue
            csv_file = csv_files[0]

            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_csv_path = pathlib.Path(tmpdir) / pathlib.Path(csv_file).name
                with z.open(csv_file) as source, open(tmp_csv_path, 'wb') as dest:
                    shutil.copyfileobj(source, dest)

                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT *
                    FROM read_csv_auto('{tmp_csv_path}', sample_size=-1, union_by_name=true, ignore_errors=true)
                """)

        new_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        row_counts[table_name] = new_count
        updated_tables.append(table_name)
        print(f"Rows: {old_count} → {new_count} (change: {new_count - old_count})")

    # -------------------------
    # Update hash and row count files
    # -------------------------
    old_hash.update({name: current_hash[name] for name in to_load})
    with open(HASH_FILE, "wb") as f:
        pickle.dump(old_hash, f)
    with open(ROW_COUNT_FILE, "wb") as f:
        pickle.dump(row_counts, f)

    print("\nIncremental DuckDB update complete!")
    print(f"Updated tables ({len(updated_tables)}): {', '.join(updated_tables)}")
    print(f"Skipped tables ({len(skipped)}): {', '.join([re.sub(r'[^a-z0-9]+','_',name.lower()).strip('_') for name in skipped])}")

# -------------------------
# Optimized row count summary
# Only recount updated tables; reuse old counts for others
# -------------------------
tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
summary = []
print(f"\nCounting rows for {len(tables)} tables...\n")
for table in tables:
    if table in updated_tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        row_counts[table] = count
    else:
        count = row_counts.get(table, 0)
    summary.append({"Table": table, "Row_Count": count})
    print(f"{table}: {count} rows")

# -------------------------
# Optional: save summary to CSV
# Comment this block if you only want console output
# -------------------------
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
df = pd.DataFrame(summary)
summary_csv_path = OUTPUT_FOLDER / f"row_counts_{ts}.csv"
df.to_csv(summary_csv_path, index=False)
print(f"\nRow counts saved to {summary_csv_path}")

# -------------------------
# Save updated row counts for next run
# -------------------------
with open(ROW_COUNT_FILE, "wb") as f:
    pickle.dump(row_counts, f)
