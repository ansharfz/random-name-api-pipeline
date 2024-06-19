from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kafka_stream import stream_data
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 18, 13, 00),
}


"""
This DAG defines a daily scheduled task that streams data from an API using
the `stream_data` function.

The `streaming_task` is a PythonOperator that executes the `stream_data`
function. The DAG is named 'user_data_stream'.
"""
with DAG('user_data_stream',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
            task_id='stream_data_from_api',
            python_callable=stream_data,
            dag=dag)

    streaming_task
