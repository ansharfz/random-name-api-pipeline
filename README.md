# Random Name API Pipeline

This is an Apache Airflow pipeline that fetches random user data from the RandomUser.me API, processes it using Apache Spark, and stores the processed data in Apache Cassandra.

## Overview
The pipeline consists of the following components:

1. Kafka Producer: Fetches random user data from the RandomUser.me API and produces messages to a Kafka topic.
2. Kafka Consumer: Consumes messages from the Kafka topic.
3. Spark: Processes the consumed messages using Apache Spark and prepares the data for storage.
4. Cassandra Storage: Stores the processed data in an Apache Cassandra database.

## Requirements
- Docker

## Setup

1. Clone the repository:

    ```
    git clone https://github.com/yourusername/random-name-api-pipeline.git
    ```

2. Navigate to the project directory:

    ```
    cd random-name-api-pipeline
    ```

3. Build and start the Docker containers:
    
    ```
    docker compose up --build
    ```
This will start all the required services (Airflow, Kafka, Spark, Cassandra, etc.).

4. Access the Airflow web UI at http://localhost:8080 and trigger the user_data_stream DAG or you can wait manually to process.

## Pipeline Details
The pipeline is defined in the dags/dags.py file. It consists of two tasks:

stream_data_from_api: Fetches random user data from the RandomUser.me API and produces messages to a Kafka topic.
process_data: Consumes messages from the Kafka topic, processes the data using Apache Spark, and stores the processed data in Apache Cassandra.

The Spark processing logic is defined in the spark-jobs/spark_processing.py file.