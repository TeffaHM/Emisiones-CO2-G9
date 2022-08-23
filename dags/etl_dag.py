from airflow import DAG
import datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocSubmitJobOperator, ClusterGenerator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import date, trigger_rule

EMAIL  = ['sandtwice5@gmail.com']
OWNER  = 'Gnine'
CLUSTER_NAME="spark-cluster-{{ ds_nodash }}"
REGION='us-central1'
PROJECT_ID = Variable.get("project")
BUCKET_NAME = 'Data_Lake'
PYSPARK_URI_1='gs://Data_Lake/jobs_pyspark/etl_spark_energy.py'
PYSPARK_URI_2='gs://Data_Lake/jobs_pyspark/etl_spark_energy_source.py'
PYSPARK_URI_3='gs://Data_Lake/jobs_pyspark/etl_spark_industries.py'
PYSPARK_URI_4='gs://Data_Lake/jobs_pyspark/load_datawarehouse.py'
DATASET_NAME = "analytics_dwh_onu"

PYSPARK_JOB_1 = {   "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_1},
}
 
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_2},
}

PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_3},
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-1",
    worker_machine_type="n1-standard-1",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=BUCKET_NAME,
).make()

default_args = {
    'owner': OWNER,               
    'depends_on_past': False,         
    'start_date':days_ago(2), # datetime.datetime(2022, 8, 20),
    'email':EMAIL,
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
}

with DAG("pipeline_etl",
         default_args = default_args,
         catchup = False,
         description='ETL process automatized airflow',
         schedule_interval="@once",
        ) as dag:

        start_pipeline = DummyOperator(task_id="start_pipeline")

        create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id = DATASET_NAME)

        create_cluster = DataprocCreateClusterOperator(
                    task_id="create_cluster",
                    project_id= PROJECT_ID,
                    cluster_config=CLUSTER_CONFIG,
                    region=REGION,
                    cluster_name=CLUSTER_NAME,
        )

        submit_job_energy = DataprocSubmitJobOperator(
                    task_id="extract_transform_energy", 
                    job=PYSPARK_JOB_1, 
                    region=REGION, 
                    project_id=PROJECT_ID
        )

        submit_job_source = DataprocSubmitJobOperator(
                    task_id="extract_transform_source", 
                    job=PYSPARK_JOB_2, 
                    region=REGION, 
                    project_id=PROJECT_ID
        )

        #submit_job_industries = DataprocSubmitJobOperator(
        #           task_id="extract_transform_industries", 
        #           job=PYSPARK_JOB_3, 
        #           region=REGION, 
        #           project_id=PROJECT_ID
        #)

        t_join = DummyOperator(task_id='t_join', dag=dag, trigger_rule='all_success')

        submit_job_bigquery = DataprocSubmitJobOperator(
            task_id="load_datawarehouse",
            project_id=PROJECT_ID,
            location=REGION,
            job= {
                'reference': {'project_id': '{{ project }}',
                              'job_id': '{{task.task_id}}_{{ds_nodash}}_2446afcc_a'},
                'placement': {'cluster_name': CLUSTER_NAME},
                'labels': {'airflow-version': 'v2-1-0'},
                'pyspark_job': {
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',
                'gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar'],
                'main_python_file_uri': PYSPARK_URI_4
                                }   
                },
            gcp_conn_id='google_cloud_default'          
        )

        delete_cluster = DataprocDeleteClusterOperator(
                    task_id="delete_cluster", 
                    project_id=PROJECT_ID, 
                    cluster_name=CLUSTER_NAME, 
                    region=REGION,
                    trigger_rule="all_done"
        )


        finish_pipeline = DummyOperator(task_id="finish_pipeline")


        start_pipeline >> create_dataset >> create_cluster

        create_cluster >> submit_job_energy >> t_join
        create_cluster >> submit_job_source >> t_join

        t_join >> submit_job_bigquery >> delete_cluster >> finish_pipeline