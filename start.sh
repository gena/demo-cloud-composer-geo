export AIRFLOW_HOME=`pwd`/airflow
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://'

echo "Starting airflow in the following directory: ${AIRFLOW_HOME} ..."

airflow standalone
