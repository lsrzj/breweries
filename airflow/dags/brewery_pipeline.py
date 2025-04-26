from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.paths import PATHS

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('brewery_pipeline',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_raw_data',
        python_callable='pipelines.bronze.fetch_breweries',
        op_kwargs={'output_path': PATHS["bronze_raw"]}
    )

    silver_task = SparkSubmitOperator(
        task_id='process_silver',
        application='scripts/breweries/silver.py',
        application_args=[PATHS["bronze_raw"], PATHS["silver"]],
    )

    gold_task = SparkSubmitOperator(
        task_id='process_gold',
        application='scripts/breweries/gold.py',
        application_args=[PATHS["silver"], PATHS["gold"]],
    )

    fetch_task >> silver_task >> gold_task
