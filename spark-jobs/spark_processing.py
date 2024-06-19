from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import from_json, col
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:\
                            %(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    """
    Create a new Spark session with the specified configuration.

    Returns
    -------
    spark : pyspark.sql.SparkSession
        A new Spark session instance.

    Raises
    ------
    Exception
        If there is an error creating the Spark session.
    """

    try:
        spark = SparkSession \
            .builder \
            .appName('SparkStructuredStreaming') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .config("spark.cassandra.connection.port", 9042) \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")

    return spark


def create_initial_dataframe(spark_session):
    """
    Create an initial DataFrame from a Kafka stream.

    This function creates a new DataFrame by reading from a Kafka stream.
    It subscribes to the "user_data" topic, sets the starting offsets to
    "earliest", and loads the data into a DataFrame.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        A Spark session instance.

    Returns
    -------
    df : pyspark.sql.DataFrame
        A DataFrame containing the data from the Kafka stream.

    Raises
    ------
    Exception
        If there is an error creating the initial DataFrame.
    """

    try:
        df = spark_session \
             .readStream \
             .format("kafka") \
             .option("kafka.bootstrap.servers", "kafka:9092") \
             .option("subscribe", "user_data") \
             .option("delimeter", ",") \
             .option("startingOffsets", "earliest") \
             .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created \
                        due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Create a final DataFrame from the initial DataFrame.

    This function takes an initial DataFrame and creates a final DataFrame by
    applying a schema to the data. The schema defines the structure of the
    DataFrame, including the data types of the columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The initial DataFrame to be transformed.
    spark_session : pyspark.sql.SparkSession
        A Spark session instance.

    Returns
    -------
    pyspark.sql.DataFrame
        The final DataFrame with the applied schema.
    """
    schema = StructType([
            StructField("full_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("location", StringType(), False),
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("postcode", IntegerType(), False),
            StructField("latitude", FloatType(), False),
            StructField("longitude", FloatType(), False),
            StructField("email", StringType(), False)
        ])

    df = df.selectExpr("CAST(value AS STRING)") \
           .select(from_json(col("value"), schema).alias("data")) \
           .select("data.*")
    return df


def start_streaming(df):
    """
    Starts a Spark Streaming job that writes data to a Cassandra table.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame to write to Cassandra.

    Returns
    -------
    pyspark.sql.streaming.StreamingQuery
        The Spark Streaming query object.
    """

    logging.info("Streaming started")
    my_query = (df.writeStream
                  .format("org.apache.spark.sql.cassandra")
                  .outputMode("append")
                  .option("checkpointLocation", "/opt/spark/checkpoint")
                  .options(table="random_names", keyspace="spark_streaming")
                  .option("kafka.request.timeout.ms", "30000")
                  .option("kafka.retry.backoff.ms", "500")
                  .option("kafka.session.timeout.ms", "60000")
                  .start())

    return my_query.awaitTermination()


def write_streaming_data():
    """
    Combines all other functions to create a Spark Streaming job.

    This function creates a Spark session,
    generates an initial DataFrame, transforms it into a final DataFrame,
    and starts the streaming process.
    """
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)


if __name__ == '__main__':
    write_streaming_data()
