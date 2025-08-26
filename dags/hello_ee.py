from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator # type: ignore
import ee

# Define global variables
PROJECT_ID = "<your-project-id>"  # Replace with your GCP project ID

# DAG definition
with DAG(dag_id='hello_ee',
    schedule_interval='0 * * * *',  # Hourly
    start_date=datetime(2025, 8, 26),
    catchup=False,
    tags=['gcp', 'earthengine', 'test'],
) as dag:
    start = DummyOperator(task_id='start')

    @task
    def ee_task():
        ee.Authenticate()
        ee.Initialize(project=PROJECT_ID)
        print(f"Earth Engine initialized successfully for project: {PROJECT_ID}")

        print(ee.String('Hello from EE!').getInfo())

    end = DummyOperator(task_id='end')

    start >> ee_task() >> end
