from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.utils.dates import days_ago
import json
import ee
import pandas as pd
from datetime import datetime, timedelta
import io

# Define global variables
PROJECT_ID = "<your-project-id>"  # Replace with your GCP project ID
GCS_BUCKET_PATH = "<your-gcs-bucket>"  # Replace with your GCS bucket name
BQ_DATASET_NAME = "ee_gfs_workflow"  # Replace with your BigQuery dataset name
BQ_TABLE_NAME = "temperature_stats"  # Replace with your BigQuery table name

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='gfs_temperature_workflow',
    default_args=default_args,
    description='A workflow to process GeoJSON files, extract temperature data from GEE, and load stats into BigQuery',
    schedule_interval='0 * * * *',  # Hourly
    start_date=days_ago(2),
    catchup=False,
    tags=['gcp', 'earthengine', 'bigquery'],
) as dag:


    @task
    def list_geojson_files(bucket_name):
        """Lists GeoJSON files in the specified GCS bucket."""
        hook = GCSHook(gcp_conn_id='google_cloud_default')  # Use your GCS connection ID
        files = hook.list(bucket_name=bucket_name)
        geojson_files = [f"gs://{bucket_name}/{file}" for file in files if file.endswith('.geojson')]  # prepends gs://
        if not geojson_files:
            print("No GeoJSON files found in the bucket.")
            return []  # Return an empty list
        return geojson_files

    @task
    def process_geojson_file(geojson_file_path):
        """
        Reads a GeoJSON file, extracts temperature data from GEE,
        and writes the time series to a CSV file in GCS.
        """

        ee.Authenticate()
        ee.Initialize(project=PROJECT_ID)
        print(f"Earth Engine initialized successfully for project: {PROJECT_ID}")

        bucket_name = geojson_file_path.split('/')[2]
        file_name = geojson_file_path.split('/')[-1]
        hook = GCSHook(gcp_conn_id='google_cloud_default')

        file_object = hook.download(bucket_name=bucket_name, object_name=file_name)
        geojson_str = file_object.decode('utf-8')
        geojson = json.loads(geojson_str)

        # Extract the geometry from the GeoJSON (assumes a single feature)
        geometry = geojson['features'][0]['geometry']
        polygon_id = geojson['features'][0].get('properties', {}).get('id', 'unknown_id')  # example of how to get an ID
        ee_geometry = ee.Geometry(geometry)

        # Earth Engine data processing
        def get_temperature_timeseries(geometry, start_date, end_date):
            gfs = ee.ImageCollection("NOAA/GFS0P25").filter(ee.Filter.date(start_date, end_date))

            gfs = gfs.select('temperature_2m_above_ground').map(lambda i: i.set({ 'system:time_start': i.get('forecast_time')}))

            def reduce_region_mean(image):
                reducer = ee.Reducer.mean()
                reduced = image.reduceRegion(
                    reducer=reducer,
                    geometry=geometry,
                    scale=1000,
                    maxPixels=1e9
                )
                temperature = reduced.get('temperature_2m_above_ground')
                timestamp = image.date().millis()
                return ee.Feature(None, {'temperature': temperature, 'timestamp': timestamp})

            feature_collection = gfs.map(reduce_region_mean).filter(ee.Filter.notNull(['temperature']))
            return feature_collection.getInfo()

        # Query data
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=12)

        formatted_end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')
        formatted_start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')

        print(f"start_date = {formatted_start_date}")
        print(f"end_date = {formatted_end_date}")

        time_series_data = get_temperature_timeseries(ee_geometry, formatted_start_date, formatted_end_date)

        df = pd.DataFrame([{
            'timestamp': feature['properties']['timestamp'],
            'temperature_celsius': feature['properties']['temperature']
        } for feature in time_series_data['features']])

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Write the time series to a CSV file in GCS
        output_file_name = file_name.replace('.geojson', '-temperature.csv')
        output_file_path = f"gs://{bucket_name}/{output_file_name}"

        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)

        # Upload the CSV string to GCS
        hook.upload(bucket_name=bucket_name, object_name=output_file_name, data=csv_string.encode('utf-8'),
                    mime_type='text/csv')

        return output_file_path  # Return the path to the generated CSV file

    @task
    def calculate_stats_and_load_bq(csv_file_path, **context):
        """
        Reads a temperature time series CSV file from GCS,
        calculates statistics, and loads them into BigQuery.
        """
        if csv_file_path is None:
            print("Skipping calculate_stats_and_load_bq because the input file path is None.")
            return  # Skip processing if the file path is None

        bucket_name = csv_file_path.split('/')[2]
        file_name = csv_file_path.split('/')[-1]

        hook = GCSHook(gcp_conn_id='google_cloud_default')

        file_object = hook.download(bucket_name=bucket_name, object_name=file_name)
        csv_str = file_object.decode('utf-8')

        df = pd.read_csv(io.StringIO(csv_str))  # Read CSV from string
        temp_mean = df['temperature_celsius'].mean()
        temp_min = df['temperature_celsius'].min()
        temp_max = df['temperature_celsius'].max()

        # Extract polygon_id from the filename (assuming the format 'polygon_id-temperature.csv')
        polygon_id = file_name.replace('-temperature.csv', '').replace('.csv', '')

        current_time = datetime.utcnow().isoformat()

        # Prepare the data for BigQuery

        sql = f"""
            INSERT INTO `{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}` (polygon_id, current_time, temp_mean, temp_min, temp_max)
            VALUES ('{polygon_id}', '{current_time}', {temp_mean}, {temp_min}, {temp_max})
            """
        
        # Load data into BigQuery using BigQueryInsertJobOperator
        insert_job = BigQueryInsertJobOperator(
            task_id=f'bq_insert_{polygon_id}',
            configuration={
                'query': {
                    'query': sql,
                    'useLegacySql': False
                }
            }
        )

        insert_job.execute(context=context)

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    file_list = list_geojson_files(bucket_name=GCS_BUCKET_PATH)

    process_tasks = process_geojson_file.expand(geojson_file_path=file_list) # create multiple process tasks

    # Ensure that calculate_stats_and_load_bq only runs if process_geojson_file returns a valid file path
    bq_tasks = calculate_stats_and_load_bq.expand(csv_file_path=process_tasks)

    start >> file_list
    file_list >> process_tasks
    process_tasks >> bq_tasks
    bq_tasks >> end