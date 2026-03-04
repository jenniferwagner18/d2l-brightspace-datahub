# Compare existing dataset .zip files in two folders (EDW and DHUB) and output differences in third folder.
# Before running script, install packages using: pip3 install pandas sqlalchemy chardet

import os
import pandas as pd
from sqlalchemy import create_engine, text
import zipfile
import chardet

# -----------------------
# Folder setup
# -----------------------
folder_a = "EDW"
folder_b = "DHUB"
folder_c = "Differences"
os.makedirs(folder_c, exist_ok=True)

# -----------------------
# Database setup
# -----------------------
db_path = os.path.join(folder_c, "diffs.db")
engine = create_engine(f"sqlite:///{db_path}")

# -----------------------
# Helper: detect file encoding
# -----------------------
def detect_encoding(path, nbytes=100_000):
    with open(path, 'rb') as f:
        raw = f.read(nbytes)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'

# -----------------------
# Helper: sanitize table names
# -----------------------
def sanitize_table_name(prefix, filename):
    base = os.path.splitext(filename)[0].lstrip(".").replace(" ", "_").replace("-", "_")
    base = "".join(c if c.isalnum() or c == "_" else "_" for c in base)
    return f"{prefix}_{base}"

# -----------------------
# Helper: filter valid CSVs (skip hidden and dot-underscore files)
# -----------------------
def is_valid_csv(filename):
    return filename.lower().endswith(".csv") and not filename.startswith(".") and not filename.startswith("._")

# -----------------------
# Unzip CSVs into folder root
# -----------------------
def unzip_csv_files(folder):
    for filename in os.listdir(folder):
        if filename.lower().endswith(".zip"):
            zip_path = os.path.join(folder, filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.lower().endswith(".csv"):
                        zip_ref.extract(member, folder)
                        src = os.path.join(folder, member)
                        dest = os.path.join(folder, os.path.basename(member))
                        if src != dest:
                            os.rename(src, dest)
                        print(f"Extracted {os.path.basename(member)} from {filename}")

unzip_csv_files(folder_a)
unzip_csv_files(folder_b)

# -----------------------
# Load CSV to SQLite with index
# -----------------------
def load_csv_to_sql_with_index(folder, table_prefix):
    for filename in os.listdir(folder):
        if not is_valid_csv(filename):
            continue
        path = os.path.join(folder, filename)
        table_name = sanitize_table_name(table_prefix, filename)
        
        encoding = detect_encoding(path)
        print(f"Loading {filename} with encoding {encoding}")
        
        chunksize = 100_000
        for chunk in pd.read_csv(path, dtype=str, chunksize=chunksize, encoding=encoding):
            # Strip BOM from first column if present
            if chunk.columns[0].startswith('\ufeff'):
                chunk.rename(columns={chunk.columns[0]: chunk.columns[0].replace('\ufeff','')}, inplace=True)
            chunk.fillna("", inplace=True)
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"Loaded {filename} into table {table_name}")
        
        # Create composite index on all columns
        with engine.connect() as conn:
            cols = pd.read_sql_query(f'PRAGMA table_info("{table_name}");', conn)
            col_names = [f'"{c}"' for c in cols['name']]
            if col_names:
                idx_name = f"idx_{table_name}"
                col_list = ", ".join(col_names)
                conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({col_list})'))
                print(f"Index created on {table_name} ({col_list})")

# Load both folders
load_csv_to_sql_with_index(folder_a, "EDW")
load_csv_to_sql_with_index(folder_b, "DHUB")

# -----------------------
# Stream differences to CSV
# -----------------------
def save_diff_sql_to_csv(table_a, table_b, output_path):
    chunksize = 100_000
    first_chunk = True
    queries = [
        f"SELECT * FROM \"{table_a}\" EXCEPT SELECT * FROM \"{table_b}\"",
        f"SELECT * FROM \"{table_b}\" EXCEPT SELECT * FROM \"{table_a}\""
    ]
    for query in queries:
        for chunk in pd.read_sql_query(query, engine, chunksize=chunksize):
            chunk.to_csv(output_path, mode='a', index=False, header=first_chunk)
            first_chunk = False

# -----------------------
# Per-file differences & summary
# -----------------------
summary = []

files_a = [f for f in os.listdir(folder_a) if is_valid_csv(f)]
files_b = [f for f in os.listdir(folder_b) if is_valid_csv(f)]
files_b_set = set(files_b)

for filename in files_a:
    table_a = sanitize_table_name("EDW", filename)
    table_b = sanitize_table_name("DHUB", filename)
    
    if filename not in files_b_set:
        count_a = pd.read_sql_query(f'SELECT COUNT(*) AS cnt FROM "{table_a}"', engine)["cnt"][0]
        summary.append({
            "File": filename,
            "Rows_EDW": count_a,
            "Rows_DHUB": None,
            "Difference": None,
            "Diff_File_Created": False
        })
        print(f"{filename} exists in EDW but not in DHUB")
        continue
    
    count_a = pd.read_sql_query(f'SELECT COUNT(*) AS cnt FROM "{table_a}"', engine)["cnt"][0]
    count_b = pd.read_sql_query(f'SELECT COUNT(*) AS cnt FROM "{table_b}"', engine)["cnt"][0]
    
    print(f"\n{filename}")
    print(f"  EDW rows: {count_a:,}")
    print(f"  DHUB rows: {count_b:,}")
    
    diff_created = False
    if count_a != count_b:
        print("  Counts differ — computing differences...")
        output_path = os.path.join(folder_c, filename.replace(".csv", "-diff.csv"))
        save_diff_sql_to_csv(table_a, table_b, output_path)
        diff_created = os.path.exists(output_path) and os.path.getsize(output_path) > 0
        if diff_created:
            print(f"  Differences written to {output_path}")
        else:
            print("  Counts differ but no row-level differences found")
    else:
        print("  Row counts match — skipping diff")
    
    summary.append({
        "File": filename,
        "Rows_EDW": count_a,
        "Rows_DHUB": count_b,
        "Difference": count_a - count_b,
        "Diff_File_Created": diff_created
    })
    
    # Drop tables to free memory
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_a}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_b}"'))

# Handle DHUB-only files
processed_files = set(f["File"] for f in summary)
for filename in files_b:
    if filename in processed_files:
        continue
    table_b = sanitize_table_name("DHUB", filename)
    count_b = pd.read_sql_query(f'SELECT COUNT(*) AS cnt FROM "{table_b}"', engine)["cnt"][0]
    print(f"{filename} exists in DHUB but not in EDW")
    summary.append({
        "File": filename,
        "Rows_EDW": None,
        "Rows_DHUB": count_b,
        "Difference": None,
        "Diff_File_Created": False
    })
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_b}"'))

# -----------------------
# Save summary
# -----------------------
summary_df = pd.DataFrame(summary)
for col in ["Rows_EDW", "Rows_DHUB", "Difference"]:
    summary_df[col] = summary_df[col].astype("Int64")
summary_csv_path = os.path.join(folder_c, "summary.csv")
summary_df.to_csv(summary_csv_path, index=False)

print("\n=== Summary ===")
print(summary_df)
print(f"\nSummary saved to {summary_csv_path}")
