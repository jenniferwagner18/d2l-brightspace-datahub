from pathlib import Path
import pandas as pd
import zipfile
from datetime import datetime
from bs4 import BeautifulSoup
import re

# -----------------------
# Optional CSV extraction
# -----------------------
SAVE_EXTRACTED_CSVS = False  # Set True to save extracted CSVs from data set ZIPs to your computer (summary and diff files will always be saved)

# -----------------------
# Paths
# -----------------------
BASE_DIR = Path(__file__).parent
folder_a = BASE_DIR / "EDW_Export"
folder_b = BASE_DIR / "DHUB_Export"
folder_c = BASE_DIR / "EDW_DHUB_Diffs"
folder_c.mkdir(exist_ok=True)

# Timestamp for filenames
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# -----------------------
# Helpers
# -----------------------
def get_csv_name(zip_path):
    """Return the first CSV filename inside a ZIP."""
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if name.lower().endswith(".csv"):
                return name
    return None

def read_csv_from_zip(zip_path):
    """Read first CSV from ZIP into memory as DataFrame."""
    csv_name = get_csv_name(zip_path)
    if csv_name is None:
        return None
    with zipfile.ZipFile(zip_path) as z:
        with z.open(csv_name) as f:
            return pd.read_csv(f, dtype=str).fillna("")

def extract_csv(zip_path, target_folder):
    """Optionally extract CSV to disk, always return DataFrame."""
    df = read_csv_from_zip(zip_path)
    if SAVE_EXTRACTED_CSVS:
        csv_name = get_csv_name(zip_path)
        with zipfile.ZipFile(zip_path) as z:
            z.extract(csv_name, target_folder)
        csv_path = target_folder / Path(csv_name).name
        return df, csv_path
    else:
        return df, None  # CSV not saved to disk

def count_rows(df):
    return len(df) if df is not None else None

def clean_html(text):
    if pd.isna(text) or text == "":
        return ""
    soup = BeautifulSoup(str(text), "html.parser")
    return " ".join(soup.get_text().split()).lower()

def detect_html_columns(df, sample_rows=10):
    html_pattern = re.compile(r"<\s*(p|ul|li|strong|em|span|div|br|a)[ >]", re.IGNORECASE)
    detected = []
    for col in df.columns:
        sample = df[col].dropna().head(sample_rows).astype(str)
        if any(html_pattern.search(val) for val in sample):
            detected.append(col)
    return detected

def normalize_df(df):
    if df is None:
        return None
    df = df.fillna("")
    html_cols = detect_html_columns(df)
    for col in df.columns:
        if df[col].dtype == object:
            if col in html_cols:
                df[col] = df[col].apply(clean_html)
            else:
                df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip().str.lower()
        else:
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df

# -----------------------
# Main loop
# -----------------------
summary = []
csvs_saved = 0
csvs_memory_only = 0

for zip_a in folder_a.glob("*.zip"):

    zip_b = folder_b / zip_a.name
    diff_created = False

    # Extract CSVs (optionally saved) and read into memory
    df_a, csv_a_path = extract_csv(zip_a, folder_a)
    df_b, csv_b_path = extract_csv(zip_b, folder_b) if zip_b.exists() else (None, None)

    count_a = count_rows(df_a)
    count_b = count_rows(df_b)

    # Print info about CSV storage
    if SAVE_EXTRACTED_CSVS:
        print(f"{zip_a.name}: CSV extracted to disk at {csv_a_path}")
        csvs_saved += 1
        if zip_b.exists():
            print(f"{zip_b.name}: CSV extracted to disk at {csv_b_path}")
            csvs_saved += 1
    else:
        print(f"{zip_a.name}: CSV read from ZIP into memory (not saved)")
        csvs_memory_only += 1
        if zip_b.exists():
            print(f"{zip_b.name}: CSV read from ZIP into memory (not saved)")
            csvs_memory_only += 1

    if not zip_b.exists():
        print(f"{zip_a.name} exists in EDW but not in DHUB")
        summary.append({
            "File": zip_a.name,
            "Rows_EDW": count_a if count_a is not None else pd.NA,
            "Rows_DHUB": pd.NA,
            "Difference": pd.NA,
            "Diff_File": False
        })
        continue

    print(f"\n{zip_a.name}")
    print(f"  EDW rows: {count_a if count_a is not None else 'N/A'}")
    print(f"  DHUB rows: {count_b if count_b is not None else 'N/A'}")

    if df_a is not None and df_b is not None and count_a != count_b:

        print("  Row counts differ — computing diff...")

        df_a_norm = normalize_df(df_a)
        df_b_norm = normalize_df(df_b)

        df_b_norm = df_b_norm[df_a_norm.columns]

        df_a_norm["source"] = "from_EDW"
        df_b_norm["source"] = "from_DHUB"

        combined = pd.concat([df_a_norm, df_b_norm], ignore_index=True)
        subset_cols = [c for c in combined.columns if c != "source"]
        combined = combined.sort_values(by=subset_cols, ignore_index=True)

        diff = combined.drop_duplicates(subset=subset_cols, keep=False)
        if not diff.empty:
            diff_created = True
            output_path = folder_c / f"{zip_a.stem}-diff{ts}.csv"
            diff.to_csv(output_path, index=False)
            print(f"  → Differences written to {output_path}")

    else:
        if count_a == count_b:
            print("  Row counts match — skipping diff")

    summary.append({
        "File": zip_a.name,
        "Rows_EDW": count_a if count_a is not None else pd.NA,
        "Rows_DHUB": count_b if count_b is not None else pd.NA,
        "Difference": (count_a - count_b) if (count_a is not None and count_b is not None) else pd.NA,
        "Diff_File": diff_created
    })

# -----------------------
# Save summary CSV
# -----------------------
summary_df = pd.DataFrame(summary)
for col in ["Rows_EDW", "Rows_DHUB", "Difference"]:
    if col not in summary_df.columns:
        summary_df[col] = pd.NA
summary_df[["Rows_EDW", "Rows_DHUB", "Difference"]] = summary_df[["Rows_EDW", "Rows_DHUB", "Difference"]].astype("Int64")

summary_path = folder_c / f"summary-differences-{ts}.csv"
summary_df.to_csv(summary_path, index=False)

# -----------------------
# Final summary prints
# -----------------------
print("\n=== Summary ===")
print(summary_df)
print(f"\nSummary saved to {summary_path}")
print(f"\nCSV extraction stats:")
print(f"  CSVs saved to disk: {csvs_saved}")
print(f"  CSVs read in memory only: {csvs_memory_only}")