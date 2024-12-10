from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Create a Spark session for data processing
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Load tables from the silver layer
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

# Rename columns to avoid ambiguity when joining
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Join the tables on the "athlete_id" column
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Calculate average values for each group
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

# Create a directory to save the results in the gold layer
output_path = "/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists before saving data

# Save the processed data in parquet format
aggregated_df.write.mode("overwrite").parquet(output_path)

# Print a message indicating successful saving
print(f"Data saved to {output_path}")

# Re-read the parquet file to verify the data
df = spark.read.parquet(output_path)
df.show(truncate=False)  # Display the content of the DataFrame without truncating rows

# Stop the Spark session
spark.stop()
