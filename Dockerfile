FROM apache/airflow:2.9.0-python3.11

USER root
RUN sudo apt-get update && sudo apt-get install -y gcc libpq-dev

USER airflow
ADD requirements.txt .
RUN pip install -r requirements.txt








