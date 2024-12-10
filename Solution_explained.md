# Data Engineering Course Final Project Solution

## Part 1: Building an End-to-End Streaming Pipeline

**This screenshot shows the result of inserting data into the enriched_athlete_avg table.**

![SQL_Database](./screenshots/SQL_Database.png)

\***\*The screenshot confirms the data was recorded in the Kafka topic 'andrew_motko_enriched_athlete_avg'.**

![kafka_topic_record](./screenshots/kafka-topic_record.png)

## Part 2: Building an End-to-End Batch Data Lake

**The result of displaying the saved data in the bronze/athlete_event_results table**

![landing_to_bronze_DAG](./screenshots/landing_to_bronze_DAG.jpg)

**The result of displaying the saved data in the silver/athlete_event_results table**

![bronze_to_silver_DAG](./screenshots/bronze_to_silver_DAG.jpg)

**The result of displaying the saved data in the gold/avg_stats table**

![silver_to_gold_DAG](./screenshots/silver_to_gold_DAG.jpg)

**This screenshot shows a DAG with three successfully executed batches.**

![successful_batches](./screenshots/successful_batches.jpg)
