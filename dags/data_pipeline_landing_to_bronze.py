from pyspark.sql import SparkSession
import os
import requests


def download_data(file):
    # Base URL for downloading the data.
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + file + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url, timeout=10)  # Send a GET request to download the file.

    if response.status_code == 200:
        # Save the downloaded file locally as a CSV file.
        with open(file + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {file}")
    else:
        # Exit the program if the download fails and display the status code.
        exit(f"Failed to download the file. Status code: {response.status_code}")


# Create a Spark session for data processing.
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

# List of tables to process.
tables = ["athlete_bio", "athlete_event_results"]

# Process each table.
for table in tables:
    # Construct the local path for saving the CSV file.
    local_path = f"{table}.csv"
    # Download the file from the server.
    download_data(table)

    # Read the CSV file into a Spark DataFrame.
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Create a directory for saving parquet files.
    output_path = f"/tmp/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists.
    # Write the DataFrame in parquet format with "overwrite" mode.
    df.write.mode("overwrite").parquet(output_path)

    # Print a message indicating successful saving.
    print(f"Data saved to {output_path}")

    # Re-read the parquet file to verify the data.
    df = spark.read.parquet(output_path)
    df.show(truncate=False)  # Display the content of the DataFrame without truncating rows.

# Stop the Spark session.
spark.stop()
