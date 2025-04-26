from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import sys
sys.path.append(f"{Path(__file__).parent}/processing")
from bronze import fetch_breweries
from paths import PATHS


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('brewery_pipeline',
         schedule='@daily',
         default_args=default_args) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_breweries,
        op_kwargs={'output_path': PATHS["bronze_raw"]}
    )

    silver_task = SparkSubmitOperator(
        task_id='process_silver',
        application='silver.py',
        application_args=[PATHS["bronze_raw"], PATHS["silver"]],
        conf={
            "spark.master": "local[*]"  # Overrides any other setting
        },
        dag=dag
    )

    gold_task = SparkSubmitOperator(
        task_id='process_gold',
        application='gold.py',
        application_args=[PATHS["silver"], PATHS["gold"]],
        conf={
            "spark.master": "local[*]"  # Overrides any other setting
        },
        dag=dag
    )

    fetch_task >> silver_task >> gold_task
