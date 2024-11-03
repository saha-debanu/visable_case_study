from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    'start_date': datetime(2024, 11, 1),
    'schedule_interval': '@monthly',
}

# Define a function for the extract task that pushes data to XComs
def extract_data_task(ti):
    extracted_data = extract_data()
    # Push the extracted data to XComs for the transform task
    ti.xcom_push(key='extracted_data', value=extracted_data)

# Define a function for the transform task that retrieves from and pushes to XComs
def transform_data_task(ti):
    # Retrieve extracted data from XComs
    extracted_data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    # Transform the data
    transformed_data = transform_data(extracted_data)
    # Push transformed data to XComs for the load task
    ti.xcom_push(key='transformed_data', value=transformed_data)

# Define a function for the load task that retrieves transformed data from XComs
def load_data_task(ti):
    # Retrieve transformed data from XComs
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    # Load data into the database
    load_data(transformed_data)

with DAG('etl_pipeline', default_args=default_args, catchup=False) as dag:
    
    # Task to extract data from CSV files
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_task
    )
    
    # Task to transform data, including deduplication
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_task
    )
    
    # Task to load data into PostgreSQL with UPSERT logic
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_task
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
