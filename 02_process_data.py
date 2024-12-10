from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os

# Setting up the environment for working with Kafka through PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Kafka connection configuration
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],  # Kafka broker address
    "username": "admin",  # Kafka username
    "password": "VawEzo1ikLtrA8Ug8THa",  # Kafka password
    "security_protocol": "SASL_PLAINTEXT",  # Security protocol for Kafka connection
    "sasl_mechanism": "PLAIN",  # SASL mechanism for authentication
    "sasl_jaas_config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="admin" password="VawEzo1ikLtrA8Ug8THa";'  # JAAS configuration for SASL
    ),
}

# MySQL connection configuration
jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",  # MySQL URL
    "user": "neo_data_admin",  # MySQL username
    "password": "Proyahaxuqithab9oplp",  # MySQL password
    "driver": "com.mysql.cj.jdbc.Driver",  # MySQL JDBC driver
}

# Initialize a Spark session
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")  # Set checkpoint location for streaming jobs
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")  # Force delete temporary checkpoint locations
    .appName("JDBCToKafka")  # Application name
    .master("local[*]")  # Use all available local cores
    .getOrCreate()
)

print("Spark version:", spark.version)  # Print the Spark version

# Define the schema for Kafka messages
schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),  # Athlete ID
        StructField("sport", StringType(), True),  # Sport name
        StructField("medal", StringType(), True),  # Medal type (gold, silver, etc.)
        StructField("timestamp", StringType(), True),  # Timestamp when the event occurred
    ]
)

# Read data from MySQL using JDBC
jdbc_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="athlete_event_results",  # Table to read from MySQL
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        partitionColumn="result_id",  # Column for partitioning data
        lowerBound=1,  # Lower bound for partitioning
        upperBound=1000000,  # Upper bound for partitioning
        numPartitions="10",  # Number of partitions for parallel reading
    )
    .load()
)

# Send data to Kafka
jdbc_df.selectExpr(
    "CAST(result_id AS STRING) AS key",  # Set result_id as key
    "to_json(struct(*)) AS value",  # Convert the whole row into a JSON string
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]  # Kafka broker
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]  # Security protocol
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]  # SASL mechanism
).option(
    "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]  # SASL JAAS configuration
).option(
    "topic", "andrew_motko_athlete_event_results"  # Kafka topic to publish to
).save()

# Read data from Kafka stream
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", "andrew_motko_athlete_event_results")  # Subscribe to the Kafka topic
    .option("startingOffsets", "earliest")  # Start reading from the earliest message
    .option("maxOffsetsPerTrigger", "5")  # Limit the number of offsets to read per trigger
    .option("failOnDataLoss", "false")  # Do not fail if data is lost
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))  # Remove escape characters
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))  # Remove quotes
    .selectExpr("CAST(value AS STRING)")  # Convert the Kafka message to string
    .select(from_json(col("value"), schema).alias("data"))  # Parse the JSON data
    .select("data.athlete_id", "data.sport", "data.medal")  # Extract relevant fields
)

# Read athlete biography data from MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="athlete_bio",  # Table for athlete biography data
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        partitionColumn="athlete_id",  # Column to partition the data by
        lowerBound=1,  # Lower bound for partitioning
        upperBound=1000000,  # Upper bound for partitioning
        numPartitions="10",  # Number of partitions for parallel reading
    )
    .load()
)

# Filter athlete bio data to ensure valid height and weight
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())  # Ensure height is not null
    & (col("weight").isNotNull())  # Ensure weight is not null
    & (col("height").cast("double").isNotNull())  # Ensure height is a valid number
    & (col("weight").cast("double").isNotNull())  # Ensure weight is a valid number
)

# Join Kafka stream data with athlete bio data
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

# Aggregate data by sport and medal, calculating average height and weight
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),  # Calculate average height
    avg("weight").alias("avg_weight"),  # Calculate average weight
    current_timestamp().alias("timestamp"),  # Add timestamp for when the aggregation occurred
)

# Function to write results to Kafka and MySQL
def foreach_batch_function(df, epoch_id):
    # Write the aggregated results to Kafka
    df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
    ).option(
        "topic", "andrew_motko_enriched_athlete_avg"  # Kafka topic for enriched data
    ).save()

    # Write the aggregated results to MySQL
    df.write.format("jdbc").options(
        url="jdbc:mysql://217.61.57.46:3306/neo_data",  # MySQL URL
        driver=jdbc_config["driver"],
        dbtable="andrew_motko_enriched_athlete_avg",  # MySQL table to write to
        user=jdbc_config["user"],
        password=jdbc_config["password"],
    ).mode("append").save()

# Start streaming job and process each batch using the foreachBatch function
aggregated_df.writeStream.outputMode("complete").foreachBatch(
    foreach_batch_function
).option("checkpointLocation", "/path/to/checkpoint/dir").start().awaitTermination()
