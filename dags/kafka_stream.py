from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import json
import requests

default_args = {
    'owner': 'airscholar',
    'start_date' : datetime(2024, 3, 19, 13,00)
}

def stream_data():
    res = requests.get('https://randomuser.me/api/')
    print(res.json())

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )
stream_data()