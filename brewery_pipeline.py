import os
import sys
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
sys.path.append(os.getcwd())
import bronze
import silver
import gold
import data_quality


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 23),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'brewery_data_pipeline_spark',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    bronze_task = PythonOperator(
        task_id='fetch_bronze_data',
        python_callable=bronze.fetch_bronze_data,
        dag=dag,
    )

    silver_task = PythonOperator(
        task_id='process_silver_data',
        python_callable=silver.process_silver_data,
        dag=dag,
    )

    gold_task = PythonOperator(
        task_id='create_gold_views',
        python_callable=gold.create_gold_views,
        dag=dag,
    )

    quality_check = PythonOperator(
        task_id='validate_data_quality',
        python_callable=data_quality.validate_data_quality,
        dag=dag,
    )

    # Set task dependencies
    bronze_task >> silver_task >> quality_check >> gold_task
