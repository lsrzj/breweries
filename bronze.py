import json
import requests
from airflow.exceptions import AirflowException


def fetch_brewery_data(**kwargs):
    url = "https://api.openbrewerydb.org/v1/breweries"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises exception for 4XX/5XX errors
        
        data = response.json()
        
        if not isinstance(data, list):
            raise AirflowException("Unexpected API response format")
            
        # Push data to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='bronze_data', value=data)
        
        # Save raw data to file (bronze layer)
        with open('data/bronze/breweries_raw.json', 'w') as f:
            json.dump(data, f)
            
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"API request failed: {str(e)}")
    except (json.JSONDecodeError, ValueError) as e:
        raise AirflowException(f"Failed to parse JSON response: {str(e)}")