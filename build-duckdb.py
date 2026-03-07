import duckdb
from pathlib import Path
import zipfile
import tempfile
import shutil
import re

# Base folder for this project
BASE_DIR = Path(__file__).parent

# Folder containing Data Hub ZIP exports
DATA_FOLDER = BASE_DIR / "DataHub_Export"

# DuckDB database file
DB_FILE = BASE_DIR / "brightspace.duckdb"

# Connect to DuckDB
con = duckdb.connect(str(DB_FILE))
con.execute("PRAGMA threads=8")  # optional: speed up parallel reading

print("Creating DuckDB database...")

# Function to clean table names
def clean_table_name(name):
    name = name.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name.strip("_")

# Loop over all ZIP files
zip_files = list(DATA_FOLDER.glob("*.zip"))

for zip_path in zip_files:

    table_name = clean_table_name(zip_path.stem)
    print(f"Loading {table_name}")

    with zipfile.ZipFile(zip_path, 'r') as z:

        # Find CSV files in the ZIP
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]

        if not csv_files:
            print(f"⚠️ No CSV found inside {zip_path}")
            continue

        csv_file = csv_files[0]  # take the first CSV

        # Extract CSV to a temporary file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_csv_path = Path(tmpdir) / Path(csv_file).name
            with z.open(csv_file) as source, open(tmp_csv_path, 'wb') as dest:
                shutil.copyfileobj(source, dest)

            # Read the temporary CSV into DuckDB
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_csv_auto('{tmp_csv_path}',
                                   sample_size=-1,
                                   union_by_name=true,
                                   ignore_errors=true)
            """)

print("Database build complete.")