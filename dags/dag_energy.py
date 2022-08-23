from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocSubmitJobOperator, ClusterGenerator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator
from airflow.contrib.sensors.gcs_sensor import GCSObjectsWithPrefixExistenceSensor, GCSObjectExistenceSensor, GoogleCloudStorageObjectSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from energy_etl import process_energy

EMAIL  = ['sandtwice5@gmail.com']
OWNER  = 'Gnine'
PROJECT_ID = Variable.get("project")
REGION='us-central1'
BUCKET_NAME_ENERGY = Variable.get("BUCKET_ENERGY") # energy-46c2377c-2322-11ed-afb6-5a6f18944df6
OBJECT = "energy" + "_" + datetime.now().strftime("%Y-%m")
#STAGING_BUCKET = Variable.get("STAGING_BUCKET") + '/Energy/' # staging-46c2377c-2322-11ed-afb6-5a6f18944df6

def process_objects(**kwargs):
    ti = kwargs['ti']
    objects = ti.xcom_pull(task_ids='wait_for_objects', key='return_value')
    for element in objects:
        process_energy(element)


default_args = {
    'owner': OWNER,               
    'depends_on_past': False,         
    'start_date':days_ago(2), # datetime.datetime(2022, 8, 20),
    'email':EMAIL,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)  # Time between retries
}

with DAG("process ETL",
         default_args = default_args,
         catchup = False,
         description='ETL process automatized airflow',
         schedule_interval="@once",
        ) as dag:

        start_pipeline = DummyOperator(task_id="start_pipeline")

        wait_for_data = GoogleCloudStorageObjectSensor(
            task_id = f"wait_for_data",
            bucket = BUCKET_NAME_ENERGY,
            object = OBJECT
        )

        process_objects_task = PythonOperator(
        task_id='process_objects',
        python_callable=process_objects,
        provide_context=True
        )

        end_pipeline = DummyOperator(task_id="end_pipeline")

        start_pipeline >> process_objects_task >> end_pipeline
