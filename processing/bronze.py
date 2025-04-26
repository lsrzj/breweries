import requests
import json
from pathlib import Path
from paths import PATHS  # Reuse centralized paths

def fetch_breweries(output_path: str = PATHS["bronze_raw"]) -> str:
    """
    Fetches raw brewery data from API
    Returns path where data was saved
    """
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(response.json(), f)
    
    return output_path  # Airflow auto-pushes this to XCom