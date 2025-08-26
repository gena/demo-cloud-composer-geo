from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
import ee

# DAG definition
with DAG(dag_id='hello_ee', description='A workflow which calls Earth Engine', 
         start_date=datetime(2025,8,27), catchup=False, tags=['gcp', 'earthengine', 'test']) as dag:
    
    @task
    def hello_ee():
        # Initialize Earth Engine inside the task
        ee.Initialize(project='dgena-demo3')

        # Call some Earth Engine code
        print(ee.String("Hello from Earth Engine!").getInfo())

    start = DummyOperator(task_id='start')
    
    end = DummyOperator(task_id='end')

    # define task dependencies
    start >> hello_ee() >> end

