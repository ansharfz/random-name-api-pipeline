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
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)


if __name__ == '__main__':
    write_streaming_data()
