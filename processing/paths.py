from pathlib import Path

# Base directories (adjust these for your deployment)
AIRFLOW_HOME = Path(__file__).parent.parent.parent.parent
DATA_DIR = AIRFLOW_HOME / "data"

# Data paths
PATHS = {
    "bronze_raw": str(DATA_DIR / "bronze/breweries/raw.json"),
    "silver": str(DATA_DIR / "silver/breweries"),
    "gold": str(DATA_DIR / "gold/breweries/aggregations")
}