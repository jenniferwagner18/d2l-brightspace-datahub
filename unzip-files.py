import zipfile
from pathlib import Path

# Base folder where the script is located
BASE_DIR = Path(__file__).parent

# Folder containing ZIP files (relative to the script)
DATA_FOLDER = BASE_DIR / "DataHub_Export"

# Folder to extract CSVs into (relative to the script)
EXTRACT_FOLDER = BASE_DIR / "DataHub_Unzipped"
EXTRACT_FOLDER.mkdir(parents=True, exist_ok=True)

# Loop over all ZIP files
for zip_path in DATA_FOLDER.glob("*.zip"):
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(EXTRACT_FOLDER)
        print(f"Extracted {zip_path.name} to {EXTRACT_FOLDER}")

print("All ZIP files have been unzipped!")