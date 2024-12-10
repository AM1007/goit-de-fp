from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

# Create a Spark session with the name "BronzeToSilver".
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

def clean_text(df):
    # Iterate through each column in the DataFrame.
    for column in df.columns:
        # If the column data type is StringType, process it.
        if df.schema[column].dataType == StringType():
            # Trim whitespace and convert text to lowercase.
            df = df.withColumn(column, trim(lower(col(column))))
    return df

# List of tables to be processed (bronze tables).
tables = ["athlete_bio", "athlete_event_results"]

# Iterate through each table for cleaning and saving to the silver layer.
for table in tables:
    # Read parquet files from the bronze layer for the current table.
    df = spark.read.parquet(f"/tmp/bronze/{table}")

    # Clean text using the `clean_text` function.
    df = clean_text(df)
    # Remove duplicates from the DataFrame.
    df = df.dropDuplicates()

    # Create a path for saving the processed data to the silver layer.
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)  # Ensure the folder exists.

    # Write the processed data to a parquet file in the silver layer with "overwrite" mode.
    df.write.mode("overwrite").parquet(output_path)

    # Print a message indicating that the processed data was successfully saved.
    print(f"Data saved to {output_path}")

    # Re-read the saved data for verification and display its content.
    df = spark.read.parquet(output_path)
    df.show(truncate=False)  # Display the content of the DataFrame without truncating text.

# Stop the Spark session.
spark.stop()
