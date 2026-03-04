# Unzips dataset files to CSV and counts rows; outputs new CSV file of row counts
# Before running script, install packages using: pip3 install pandas sqlalchemy chardet

import os
import pandas as pd
import zipfile
import chardet

# -----------------------
# Folder setup
# -----------------------
folder = "DHUB"

# -----------------------
# Helper: detect file encoding
# -----------------------
def detect_encoding(path, nbytes=100_000):
    with open(path, 'rb') as f:
        raw = f.read(nbytes)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'

# -----------------------
# Helper: filter valid CSVs
# -----------------------
def is_valid_csv(filename):
    return filename.lower().endswith(".csv") and not filename.startswith(".") and not filename.startswith("._")

# -----------------------
# Unzip CSVs into the same folder (skip if CSV exists)
# -----------------------
def unzip_csv_files(folder):
    for filename in os.listdir(folder):
        if filename.lower().endswith(".zip"):
            zip_path = os.path.join(folder, filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.lower().endswith(".csv"):
                        dest = os.path.join(folder, os.path.basename(member))
                        if os.path.exists(dest):
                            print(f"Skipping {os.path.basename(member)} (already exists)")
                            continue
                        zip_ref.extract(member, folder)
                        src = os.path.join(folder, member)
                        if src != dest:
                            os.rename(src, dest)
                        print(f"Extracted {os.path.basename(member)} from {filename}")

# -----------------------
# Count rows in each CSV (fast, chunked)
# -----------------------
def count_csv_rows(folder):
    summary = []
    for filename in os.listdir(folder):
        if not is_valid_csv(filename):
            continue
        path = os.path.join(folder, filename)
        encoding = detect_encoding(path)
        print(f"Counting rows in {filename} (encoding: {encoding})")
        
        row_count = 0
        try:
            for chunk in pd.read_csv(path, dtype=str, encoding=encoding, chunksize=100_000):
                row_count += len(chunk)
        except Exception as e:
            print(f"  Error reading {filename}: {e}")
            row_count = None
        
        summary.append({"File": filename, "Row_Count": row_count})
    return summary

# -----------------------
# Run
# -----------------------
unzip_csv_files(folder)
summary = count_csv_rows(folder)

# Save summary in the current working directory
summary_df = pd.DataFrame(summary)
summary_csv_path = os.path.join(os.getcwd(), "zip-row-counts.csv")
summary_df.to_csv(summary_csv_path, index=False)

print("\n=== Summary ===")
print(summary_df)
print(f"\nSummary saved to {summary_csv_path}")
