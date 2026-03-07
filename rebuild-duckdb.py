import duckdb
import pathlib
import hashlib
import pickle
import zipfile
import shutil
import tempfile
import re

# -------------------------
# Paths relative to the script
# -------------------------
BASE_DIR = pathlib.Path(__file__).parent
DATA_FOLDER = BASE_DIR / "DataHub_Export"
DB_FILE = BASE_DIR / "brightspace.duckdb"
HASH_FILE = BASE_DIR / "zip_hashes.pkl"
ROW_COUNT_FILE = BASE_DIR / "table_row_counts.pkl"

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
    folder = pathlib.Path(folder)
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

# -------------------------
# Load previous row counts or initialize from existing tables
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
# Load new/updated ZIPs
# -------------------------
if not to_load:
    print("No new or updated ZIP files detected. Skipping rebuild.")
else:
    print(f"{len(to_load)} new/updated ZIP(s) detected. Loading into DuckDB...")
    updated_tables = []

    for zip_name in to_load:
        zip_path = DATA_FOLDER / zip_name
        table_name = re.sub(r'[^a-z0-9]+', '_', zip_path.stem.lower()).strip("_")
        print(f"\nLoading {zip_name} → table {table_name}")

        # old row count
        old_count = row_counts.get(table_name, 0)

        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                print(f"⚠️ No CSV found in {zip_name}, skipping")
                continue
            csv_file = csv_files[0]

            # Extract to temporary file
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_csv_path = pathlib.Path(tmpdir) / pathlib.Path(csv_file).name
                with z.open(csv_file) as source, open(tmp_csv_path, 'wb') as dest:
                    shutil.copyfileobj(source, dest)

                # Load into DuckDB
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT *
                    FROM read_csv_auto('{tmp_csv_path}', sample_size=-1, union_by_name=true, ignore_errors=true)
                """)

        # new row count
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

    # -------------------------
    # Summary log
    # -------------------------
    print("\nIncremental DuckDB update complete!")
    print(f"Updated tables ({len(updated_tables)}): {', '.join(updated_tables)}")
    print(f"Skipped tables ({len(skipped)}): {', '.join([re.sub(r'[^a-z0-9]+','_',name.lower()).strip('_') for name in skipped])}")