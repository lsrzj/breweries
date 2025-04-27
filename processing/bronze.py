import requests
import json
from pathlib import Path
from paths import PATHS  # Reuse centralized paths


"""This function fetches brewery data from the Open Brewery DB API, saves it to a specified
location, and returns the path to the saved data.
It handles pagination to ensure all data is retrieved, and it creates the necessary directories
if they do not exist. To avoid downloading the whole database from the API, it allows
the user to specify the initial page, the number of items per page and the maximum number of 
pages to fetch.It also handles errors by raising an exception if the API request fails, so 
Airflow will retry
    Args:
        output_path (str): Path to save the raw data.
        items_per_page (int): Number of items to fetch per page.
        max_pages_to_fetch (int): Maximum number of pages to fetch.
    Returns:
        str: Path where data was saved.
    """


def fetch_breweries(output_path: str = PATHS["bronze_raw"], page=1, items_per_page=200, max_pages_to_fetch=50) -> str:
    url = "https://api.openbrewerydb.org/v1/breweries"
    breweries = []
    Path(output_path).mkdir(parents=True, exist_ok=True)
    while True and page <= max_pages_to_fetch:
        # Fetch data from API
        response = requests.get(
            f"{url}?page={page}&per_page={items_per_page}", timeout=30)
        response.raise_for_status()
        if len(response.json()) == 0:
            # No more data to fetch
            break
        breweries.extend(response.json())
        with open(f"{output_path}/raw_page={page}_items={items_per_page}.json", 'w') as f:
            json.dump(breweries, f)
        
        breweries = []
        page += 1

    return f"{output_path}/*.json"  # Airflow auto-pushes this to XCom


if __name__ == "__main__":
    fetch_breweries()
