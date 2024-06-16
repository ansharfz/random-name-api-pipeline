FROM apache/airflow:2.9.0-python3.11
COPY requirements.txt /

USER root
RUN sudo apt-get update && sudo apt-get install -y gcc libpq-dev \
openjdk-17-jre-headless

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip3 install "apache-airflow==2.9.0" apache-airflow-providers-apache-spark==4.7.1
RUN pip3 install --no-cache-dir -r /requirements.txt







