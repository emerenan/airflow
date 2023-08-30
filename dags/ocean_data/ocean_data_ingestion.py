import os
import re
import csv
import json
from io import StringIO
from datetime import date
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from package.utils.monitoring.slack_hook import slack_fail_alerts

DAG_ID = "ocean_data_ingestion"
OCEAN_SETS = Variable.get("ocean_api_secret", deserialize_json=True)
API_TOKEN = OCEAN_SETS[
    'api_token']  # THIS WILL DEPRECATED SINCE WE WILL MOVE THIS PROCESS TO API FRAMEWORK (DATAENG-1049)
GCS_BUCKET = Variable.get("ocean-data-gcs-bucket")
BQ_GCP_CONN_ID = Variable.get("bq_data_lake_connection")
S3_BUCKET = Variable.get("s3_ocean_ingestion_bucket_name")
AWS_S3_CONN_ID = Variable.get("s3_assume_role_connection")
SQL_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/ingestion_sql/"


def valid_strings(s: str) -> str:
    return re.sub("[^0-9a-zA-Z_]+", "_", s)


def get_bq_deltas(**context):
    bq = BigQueryHook(gcp_conn_id=BQ_GCP_CONN_ID, use_legacy_sql=False, )
    bq.get_client()
    conn = bq.get_conn()
    sql = f"SELECT COALESCE(max(Date),DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)) FROM " \
          f"`{dwh_project}.{dwh_dataset}.{dwh_table_name}`"
    cursor = conn.cursor()
    cursor.execute(sql)
    start_date = (date.fromisoformat(cursor.fetchone()[0])).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    context['ti'].xcom_push(key='start_date', value=start_date)
    context['ti'].xcom_push(key='end_date', value=end_date)


def extract_api_data(API_TOKEN: str, start_date: str, end_date: str):
    api_cmd = "curl --location --request POST 'https://omdigitaldataapi.oceanmediainc.com:444/api/DCPTradeDesk" \
              "/Get' --header \"ClientKey\":\"{0}\" --header 'Content-Type: application/json' --data '{{" \
              "\"StartDate\": \"{1}\",\"EndDate\": \"{2}\"}}'".format(API_TOKEN, start_date, end_date)
    data_str = os.popen(api_cmd).read()
    data_dict = json.loads(data_str)['data']
    buffer = StringIO()

    # Convert JSON to CSV
    col_names = data_dict[0].keys()
    csv_writer = csv.DictWriter(buffer, col_names)
    csv_writer.writerows(data_dict)
    context = get_current_context()
    context['ti'].xcom_push(key='dct_ocean_data', value=buffer.getvalue())


def sync_to_data_lake(order_data: str, start_date: str, end_date: str):
    dt_start_date = datetime.strptime(start_date, "%Y-%m-%d").strftime('%Y%m%d')
    dt_end_date = datetime.strptime(end_date, "%Y-%m-%d").strftime('%Y%m%d')
    partition_date = f'ocean-ingestion/{dt_start_date}-{dt_end_date}'
    s3_hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    s3_hook.load_string(order_data, f'{partition_date}/ocean_data.csv', S3_BUCKET,
                        replace=True)
    context = get_current_context()
    context['ti'].xcom_push(key='files_path', value=partition_date)


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 1 * * *",
    start_date=datetime(2023, 2, 10),
    catchup=False,
    default_args={
        "retries": 0,
    },
    on_failure_callback=slack_fail_alerts,
    tags=['ocean']
) as dag:
    dl_dataset = "data_landing"
    dl_table_name = "media_campaign_data"
    dl_project = Variable.get("bq_data_lake_project")

    dwh_dataset = "viator_ocean"
    dwh_table_name = "media_campaign_data"
    dwh_project = Variable.get("bq_data_warehouse_project")

    check_create_dwh_bq_table = BigQueryInsertJobOperator(
        task_id="create_dwh_table",
        gcp_conn_id=BQ_GCP_CONN_ID,
        configuration={
            "query": {
                "query": open(f"{SQL_PATH}create_table.sql", 'r').read()
                .replace('{project}', dwh_project)
                .replace('{dataset}', dwh_dataset)
                .replace('{table_name}', dwh_table_name),
                "useLegacySql": False
            }
        },
        project_id=dl_project
    )

    get_bq_date_range = PythonOperator(
        task_id='get_bq_date_range',
        python_callable=get_bq_deltas,
        provide_context=True
    )

    extract_api_data = PythonOperator(
        task_id="extract_api_data",
        python_callable=extract_api_data,
        op_kwargs={
            "API_TOKEN": API_TOKEN,
            "start_date": "{{ti.xcom_pull(key='start_date')}}",
            "end_date": "{{ti.xcom_pull(key='end_date')}}"
        }
    )

    sync_to_dl = PythonOperator(
        task_id="sync_to_datalake",
        python_callable=sync_to_data_lake,
        op_kwargs={
            "order_data": "{{ti.xcom_pull(key='dct_ocean_data')}}",
            "start_date": "{{ti.xcom_pull(key='start_date')}}",
            "end_date": "{{ti.xcom_pull(key='end_date')}}"
        }
    )

    move_to_gcs = S3ToGCSOperator(
        task_id="transport_s3_to_gcs",
        bucket=S3_BUCKET,
        prefix="{{ti.xcom_pull(key='files_path')}}",
        aws_conn_id=AWS_S3_CONN_ID,
        gcp_conn_id=BQ_GCP_CONN_ID,
        dest_gcs="gs://{}/".format(GCS_BUCKET),
        replace=True
    )
    check_create_dl_bq_table = BigQueryInsertJobOperator(
        task_id="create_dl_table",
        gcp_conn_id=BQ_GCP_CONN_ID,
        configuration={
            "query": {
                "query": open(f"{SQL_PATH}create_table.sql", 'r').read()
                .replace('{project}', dl_project)
                .replace('{dataset}', dl_dataset)
                .replace('{table_name}', dl_table_name),
                "useLegacySql": False
            },
            "labels": {
                "dag_id": DAG_ID,
                "task_id": "create_dl_table"
            }
        },
        project_id=dl_project
    )
    build_bq_table = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["{{ti.xcom_pull(key='files_path')}}" + '/*.csv'],
        destination_project_dataset_table=f"{dl_project}.{dl_dataset}.{dl_table_name}",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=BQ_GCP_CONN_ID,
        source_format='CSV'
    )

    delete_gcs_objects = GCSDeleteObjectsOperator(
        task_id="delete_gcs_objects",
        bucket_name=GCS_BUCKET,
        prefix="{{ti.xcom_pull(key='files_path')}}",
        gcp_conn_id=BQ_GCP_CONN_ID
    )

    move_dl_to_dwh = BigQueryInsertJobOperator(
        task_id="copy_data_dl_dwh",
        gcp_conn_id=BQ_GCP_CONN_ID,
        configuration={
            "query": {
                "query": open(f"{SQL_PATH}move_to_dwh.sql", 'r').read()
                .replace('{dl_project}', dl_project)
                .replace('{dl_dataset}', dl_dataset)
                .replace('{dl_table_name}', dl_table_name)
                .replace('{dwh_project}', dwh_project)
                .replace('{dwh_dataset}', dwh_dataset)
                .replace('{dwh_table_name}', dwh_table_name)
                .replace('{partition_date}', date.today().isoformat()),
                "useLegacySql": False
            },
            "labels" : {
                "dag_id" : DAG_ID,
                "task_id": "copy_data_dl_dwh"
            }
        },
        project_id=dl_project
    )

    check_create_dwh_bq_table >> get_bq_date_range >> extract_api_data >> sync_to_dl >> move_to_gcs
    move_to_gcs >> check_create_dl_bq_table >> build_bq_table >> move_dl_to_dwh
    build_bq_table >> delete_gcs_objects
