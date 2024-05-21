from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators \
     .spark_submit import SparkSubmitOperator

from kafka_stream import stream_data
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 4, 13, 00),
}


with DAG('user_data_stream',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
            task_id='stream_data_from_api',
            python_callable=stream_data,
            dag=dag)

    processing_task = SparkSubmitOperator(
            task_id='process_data',
            application='/opt/airflow/dags/spark_processing.py',
            dag=dag)

    streaming_task
