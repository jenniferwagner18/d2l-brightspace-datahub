from pathlib import Path
import pandas as pd
import zipfile
from datetime import datetime
from bs4 import BeautifulSoup
import re

# -----------------------
# Paths
# -----------------------
BASE_DIR = Path(__file__).parent

folder_a = BASE_DIR / "EDW_Export"
folder_b = BASE_DIR / "DHUB_Export"
folder_c = BASE_DIR / "EDW_DHUB_Diffs"
folder_c.mkdir(exist_ok=True)

# Create timestamp to append to filenames
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

def extract_csv(zip_path, target_folder):
    """Extract first CSV from ZIP to target folder."""
    csv_name = get_csv_name(zip_path)
    if csv_name is None:
        return None
    with zipfile.ZipFile(zip_path) as z:
        z.extract(csv_name, target_folder)
    return target_folder / Path(csv_name).name

def count_rows(csv_path):
    rows = 0
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=100_000):
        rows += len(chunk)
    return rows

def read_csv_for_diff(csv_path):
    return pd.read_csv(csv_path, dtype=str).fillna("")

def clean_html(text):
    """Convert HTML to normalized plain text for comparison."""
    if pd.isna(text) or text == "":
        return ""
    soup = BeautifulSoup(str(text), "html.parser")
    return " ".join(soup.get_text().split()).lower()

def detect_html_columns(df, sample_rows=10):
    """Automatically detect columns likely containing HTML."""
    html_pattern = re.compile(r"<\s*(p|ul|li|strong|em|span|div|br|a)[ >]", re.IGNORECASE)
    detected = []
    for col in df.columns:
        # Take a sample of values to speed up detection
        sample = df[col].dropna().head(sample_rows).astype(str)
        if any(html_pattern.search(val) for val in sample):
            detected.append(col)
    return detected

def normalize_df(df):
    """Normalize dataframe for comparison:
       - collapse whitespace
       - lowercase
       - numeric conversion
       - clean detected HTML columns
    """
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
# Main comparison loop
# -----------------------
summary = []

for zip_a in folder_a.glob("*.zip"):

    zip_b = folder_b / zip_a.name
    diff_created = False
    count_a = None
    count_b = None

    # Extract CSVs
    csv_a_path = extract_csv(zip_a, folder_a)
    if csv_a_path:
        count_a = count_rows(csv_a_path)

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

    csv_b_path = extract_csv(zip_b, folder_b)
    if csv_b_path:
        count_b = count_rows(csv_b_path)

    print(f"\n{zip_a.name}")
    print(f"  EDW rows: {count_a if count_a is not None else 'N/A'}")
    print(f"  DHUB rows: {count_b if count_b is not None else 'N/A'}")

    if csv_a_path and csv_b_path and count_a != count_b:

        print("  Row counts differ — computing diff...")

        df_a = normalize_df(read_csv_for_diff(csv_a_path))
        df_b = normalize_df(read_csv_for_diff(csv_b_path))

        # Reorder DHUB columns to match EDW
        df_b = df_b[df_a.columns]

        df_a["source"] = "from_EDW"
        df_b["source"] = "from_DHUB"

        combined = pd.concat([df_a, df_b], ignore_index=True)
        subset_cols = [c for c in combined.columns if c != "source"]
        combined = combined.sort_values(by=subset_cols, ignore_index=True)

        diff = combined.drop_duplicates(subset=subset_cols, keep=False)
        if not diff.empty:
            output_path = folder_c / f"{zip_a.stem}-diff{ts}.csv"
            diff.to_csv(output_path, index=False)
            diff_created = True
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
# Save summary safely
# -----------------------
summary_df = pd.DataFrame(summary)

for col in ["Rows_EDW", "Rows_DHUB", "Difference"]:
    if col not in summary_df.columns:
        summary_df[col] = pd.NA

summary_df[["Rows_EDW", "Rows_DHUB", "Difference"]] = summary_df[["Rows_EDW", "Rows_DHUB", "Difference"]].astype("Int64")

summary_path = folder_c / f"summary-differences-{ts}.csv"
summary_df.to_csv(summary_path, index=False)

print("\n=== Summary ===")
print(summary_df)
print(f"\nSummary saved to {summary_path}")