from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Define the base path to the project (specified in docker-compose.yaml)
BASE_PATH = os.getenv('BASE_PATH', '/opt/airflow/dags')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Set the start date for the DAG to one day ago
}

# Initialize the DAG
dag = DAG(
    'andrew-DAG',  # DAG ID
    default_args=default_args,  # Default arguments to be used by all tasks in the DAG
    description='ETL pipeline for changing medals in the Data Lake.',  # Description of the DAG's purpose
    schedule_interval=None,  # No schedule, so it runs manually or triggered by another process
    tags=['andrew_motko'],  # Tags for the DAG
)

# Task for running landing_to_bronze.py
# This task will run the script that loads data into the "bronze" layer of the data lake
landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',  # Unique task ID
    bash_command=f'python {BASE_PATH}/landing_to_bronze.py',  # Command to run the script
    dag=dag,  # Associate this task with the DAG
)

# Task for running bronze_to_silver.py
# This task will run the script that processes data from the "bronze" layer to the "silver" layer
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'python {BASE_PATH}/bronze_to_silver.py',
    dag=dag,
)

# Task for running silver_to_gold.py
# This task will run the script that processes data from the "silver" layer to the "gold" layer
silver_to_gold = BashOperator(
    task_id='silver_to_gold',  
    bash_command=f'python {BASE_PATH}/silver_to_gold.py',  # Command to run the script
    dag=dag,
)

# Define the sequence of task execution
# The tasks will execute in the following order: landing_to_bronze -> bronze_to_silver -> silver_to_gold
landing_to_bronze >> bronze_to_silver >> silver_to_gold
