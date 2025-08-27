import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

GCP_PROJECT_ID = '<your-project-id>'
GCP_LOCATION = "US"
BIGQUERY_DATASET = 'airflow_test'
BIGQUERY_TABLE = 'airflow_task_logs'

with DAG(
    dag_id="bigquery_task_instance_logger",
    start_date=pendulum.datetime(2025, 8, 21, tz="UTC"),
    catchup=False,
    schedule=None,  # This DAG is meant to be triggered manually or by other DAGs
    tags=["bigquery", "example"],
    doc_md="""
    ### BigQuery Task Instance Logger DAG

    This DAG demonstrates how to insert metadata about a task instance
    into a BigQuery table using the `BigQueryInsertJobOperator`.
    """,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    # Task to create the BigQuery dataset if it doesn't exist
    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        dataset_id=BIGQUERY_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        if_exists='ignore',
    )

    # Task to create the BigQuery table if it doesn't exist
    create_bigquery_table = BigQueryCreateTableOperator(
        task_id="create_bigquery_table",
        dataset_id=BIGQUERY_DATASET,
        table_id=BIGQUERY_TABLE,
        project_id=GCP_PROJECT_ID,
        # Define the schema of the table
        table_resource={
            "schema" : {
                "fields": [
                    {"name": "dag_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "task_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "run_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "logical_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "execution_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "try_number", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "log_url", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "inserted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                ]
            }
        },
        if_exists='ignore',
    )

    # The SQL query to insert a new record into the BigQuery table.
    # It uses Airflow's context variables, which are accessible via Jinja templating.
    # These variables are automatically filled in by Airflow at runtime.
    INSERT_ROWS_QUERY = f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` (
            dag_id,
            task_id,
            run_id,
            logical_date,
            execution_date,
            try_number,
            log_url,
            inserted_at
        )
        VALUES
        (
            '{{{{ dag.dag_id }}}}',          -- The ID of the DAG
            '{{{{ task.task_id }}}}',        -- The ID of the task
            '{{{{ run_id }}}}',              -- The unique ID of the DAG run
            '{{{{ data_interval_start }}}}', -- The logical start date of the run (TIMESTAMP)
            '{{{{ ts }}}}',                  -- The execution date as a timestamp string
            '{{{{ ti.try_number }}}}',       -- The try number for this task instance
            '{{{{ ti.log_url }}}}',          -- The URL to the task instance's logs
            CURRENT_TIMESTAMP()              -- The current timestamp when the record is inserted
        );
    """

    insert_task_info_into_bq = BigQueryInsertJobOperator(
        task_id="insert_task_info_into_bq",
        # Configuration for the BigQuery job
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,  # Important to use Standard SQL
            }
        }
    )

    # insert_task_info_into_bq = BigQueryCheckOperator(
    #     task_id="insert_task_info_into_bq",
    #     sql=f"SELECT 1",
    #     use_legacy_sql=False,
    # )

    # get_dataset_tables = BigQueryGetDatasetTablesOperator(
    #     task_id="get_dataset_tables", dataset_id=BIGQUERY_DATASET
    # )

    list_buckets = GCSListObjectsOperator(task_id="list_buckets", 
                                          bucket="us-central1-ee-workflows-9679eded-bucket")
    
    
    end = EmptyOperator(
        task_id='end',
    )

    def print_buckets():
        print('hello')
        # for object_name in objects:
        #     print(object_name)

    start >> create_bigquery_dataset >> create_bigquery_table >> insert_task_info_into_bq >> end
    
    start >> list_buckets >> end
    
    

    
    
