from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)

import os

# Configuration for using Kafka in Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Kafka connection parameters
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],  # Kafka broker address
    "username": "admin",  # Kafka username
    "password": "VawEzo1ikLtrA8Ug8THa",  # Kafka password
    "security_protocol": "SASL_PLAINTEXT",  # Kafka security protocol
    "sasl_mechanism": "PLAIN",  # SASL mechanism for authentication
}

# Creating a Spark session for data processing
spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

# Defining the schema for incoming JSON data
schema = StructType(
    [
        StructField("sport", StringType(), True),  # Sport type
        StructField("medal", StringType(), True),  # Medal type (e.g., gold, silver)
        StructField("sex", StringType(), True),  # Athlete's sex
        StructField("noc_country", StringType(), True),  # National Olympic Committee country code
        StructField("avg_height", StringType(), True),  # Athlete's average height
        StructField("avg_weight", StringType(), True),  # Athlete's average weight
        StructField("timestamp", StringType(), True),  # Timestamp of the data
    ]
)

# Reading streaming data from Kafka
kafka_streaming_df = (
    spark.readStream.format("kafka")  # Use Kafka as the source format
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])  # Kafka server address
    .option("kafka.security.protocol", "SASL_PLAINTEXT")  # Kafka security protocol
    .option("kafka.sasl.mechanism", "PLAIN")  # SASL authentication mechanism
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )  # JAAS configuration for SASL authentication
    .option("subscribe", "andrew_motko_enriched_athlete_avg")  # Kafka topic to subscribe to
    .option("startingOffsets", "earliest")  # Start reading from the earliest offset
    .option("maxOffsetsPerTrigger", "50")  # Limit the number of offsets per trigger (for performance)
    .option("failOnDataLoss", "false")  # Don't fail if data is lost
    .load()  # Load the stream data
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))  # Remove escape characters
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))  # Remove surrounding quotes
    .selectExpr("CAST(value AS STRING)")  # Cast the Kafka message value to a string
    .select(from_json(col("value"), schema).alias("data"))  # Parse the JSON message using the defined schema
    .select("data.*")  # Select all fields from the parsed JSON data
)

# Output the results of the streaming data to the console
kafka_streaming_df.writeStream.trigger(availableNow=True).outputMode("append").format(
    "console"  # Output the data to the console
).option("truncate", "false").start().awaitTermination()  # Don't truncate the output and start the streaming job
