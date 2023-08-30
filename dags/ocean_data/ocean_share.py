import os
import glob

import pendulum
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import task, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from airflow.utils.edgemodifier import Label

from package.utils.monitoring.slack_hook import slack_fail_alerts

TODAY = date.today()
DAG_NAME = "ocean_data_share"
DEFAULT_TZ = pendulum.timezone("UTC")
RESPECTIVE_WEEK = (date.today().isocalendar()[1] - 1)

AWS_CONN_ID = Variable.get("s3_assume_role_connection")
BQ_GCP_CONN_ID = Variable.get("bq_data_lake_connection")
SQL_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/sql/"
DDL_SQL_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/create_tables/"
SHARE_SQL_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/share_sql/"

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "email": ["VI_ANALYTICS_ENGINEERING@tripadvisor.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}


def check_backfill_status(processing_week: str):
    """
        Check if the processing week is equal to the previous week from the current week's Monday
    """
    if int(processing_week) == (date.today() - timedelta(days=date.today().weekday())).isocalendar()[1] - 1:
        return "end_flow"
    else:
        return "backfill_data"


@task(multiple_outputs=True)
def get_week_ranges(project: str, dataset: str, table_name: str):
    bq = BigQueryHook(gcp_conn_id=BQ_GCP_CONN_ID, use_legacy_sql=False, )
    bq.get_client()
    conn = bq.get_conn()
    sql = open(f"{SHARE_SQL_PATH}backfill_check.sql", 'r').read() \
        .replace("{project}", project) \
        .replace("{dataset}", dataset) \
        .replace("{table_name}", table_name)

    cursor = conn.cursor()
    cursor.execute(sql)
    year, week_number, week_start, week_end = cursor.fetchone()
    return {"year": year,
            "week_number": week_number,
            "week_start": week_start,
            "week_end": week_end}


# Beginning testing
with DAG(
        dag_id=DAG_NAME,
        default_args=default_args,
        schedule_interval="30 1 * * 1",
        start_date=datetime(2022, 12, 19, tzinfo=DEFAULT_TZ),
        on_failure_callback=slack_fail_alerts,
        catchup=False,
        tags=["Ocean"]) as dag:
    staging_dataset = "staging"
    base_project = Variable.get("bq_data_lake_project")
    s3_ocean_bucket = Variable.get("ocean-data-s3-bucket")
    gcs_ocean_bucket = Variable.get('ocean-data-gcs-bucket')
    create_tables = []
    get_week_range = []
    run_queries = []
    export_gcs = []
    transfer_to_s3 = []
    # Week number for backfill check purposes
    min_week = -1
    for filename in glob.glob(f"{SQL_PATH}*.sql"):
        base_name = filename.split('/')[-1].split('.')[0]
        base_uri = f"s3://{s3_ocean_bucket}/{base_name}/week_{RESPECTIVE_WEEK}/*"
        table_name = f"ocean_{base_name}"
        destination_table = "{}.{}.{}".format(base_project, staging_dataset, table_name)
        create_tables.append(
            BigQueryInsertJobOperator(
                task_id=f"create_{base_name}_table",
                gcp_conn_id=BQ_GCP_CONN_ID,
                configuration={
                    "query": {
                        "query": open(f"{DDL_SQL_PATH}{base_name}.sql", 'r').read()
                        .replace('{project}', base_project)
                        .replace('{dataset}', staging_dataset)
                        .replace('{table_name}', table_name),
                        "useLegacySql": False
                    },
                },
                project_id=base_project
            )
        )

        keys = get_week_ranges(project=base_project,
                               dataset=staging_dataset,
                               table_name=table_name)

        year_key = str(keys["year"])
        week_number_key = str(keys["week_number"])
        week_start_key = str(keys["week_start"])
        week_end_key = str(keys["week_end"])

        if min_week == -1:
            min_week = week_number_key
        else:
            if week_number_key < min_week:
                min_week = week_number_key

        run_queries.append(
            BigQueryExecuteQueryOperator(
                task_id=f'update_{base_name}',
                sql=open(filename, 'r').read()
                .replace('{year}', year_key)
                .replace('{week_number}', week_number_key)
                .replace('{min-event-date}', week_start_key)
                .replace('{max-event-date}', week_end_key),
                gcp_conn_id=BQ_GCP_CONN_ID,
                use_legacy_sql=False,
                write_disposition="WRITE_APPEND",
                dag=dag,
                destination_dataset_table=destination_table
            )
        )

        # Export query result to Cloud Storage.
        export_gcs.append(
            BigQueryInsertJobOperator(
                task_id=f'{base_name}_to_gcs',
                gcp_conn_id=BQ_GCP_CONN_ID,
                configuration={
                    "query": {
                        "query": open(f"{SHARE_SQL_PATH}export_to_gcs.sql", 'r').read()
                        .replace('{project}', base_project)
                        .replace('{dataset}', staging_dataset)
                        .replace('{table_name}', table_name)
                        .replace('{year}', year_key)
                        .replace('{week_number}', week_number_key)
                        .replace("{destination_cloud_storage_uris}", f"gs://{gcs_ocean_bucket}/ocean_data/{base_name}/"
                                                                     f"{year_key}/week_{week_number_key}/part-*.csv"),
                        "useLegacySql": False
                    },
                },
                project_id=base_project
            )
        )

        transfer_to_s3.append(
            GCSToS3Operator(
                task_id=f"{base_name}_gcs_to_s3",
                gcp_conn_id=BQ_GCP_CONN_ID,
                dest_aws_conn_id=AWS_CONN_ID,
                bucket=gcs_ocean_bucket,
                prefix=f"ocean_data/{base_name}/{year_key}/week_{week_number_key}/",
                dest_s3_key=f"s3://{s3_ocean_bucket}",
                delimiter='.csv',
                replace=True
            )
        )

        create_tables[-1] >> keys >> run_queries[-1] >> export_gcs[-1] >> transfer_to_s3[-1]
    backfill_check = BranchPythonOperator(
        task_id="check_backfill",
        python_callable=check_backfill_status,
        op_kwargs={
            "processing_week": week_number_key,
        }
    )
    backfill_call = TriggerDagRunOperator(
        task_id="backfill_data",
        trigger_dag_id=DAG_NAME
    )
    end_flow = DummyOperator(task_id="end_flow")
    backfill_check >> Label("Up to Date") >> end_flow
    transfer_to_s3 >> backfill_check >> Label("BackFill Data") >> backfill_call
