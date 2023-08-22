import os
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from package.utils.monitoring.slack_hook import slack_fail_alerts

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]
DEFAULT_TZ = pendulum.timezone("UTC")
CURRENT_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

base_dir = os.path.dirname(os.path.realpath(__file__))

sql_dir = os.path.join(base_dir, 'sql')

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "email": ["VI_ANALYTICS_ENGINEERING@tripadvisor.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    default_args=default_args,
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    start_date=datetime(2023, 1, 19, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    tags=["Appsflyer", "Events"]
)

BQ_CONN_ID = Variable.get("bq_data_lake_connection")

skad_event = 'appsflyer_events'

for event in ['channel', 'geo']:
    
    check_table = BigQueryInsertJobOperator(
        task_id="check_if_table_exists",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": open(f"{sql_dir}/appsflyer_cost_etl.sql", 'r').read()
                .replace('{dwh_project_id}', Variable.get("bq_data_warehouse_project"))
                .replace('{lake_project_id}', Variable.get("bq_data_lake_project"))
                .replace('{event}', event)
                .replace('{dag_id}', dag)
                .replace('{task_id}', "check_if_table_exists"),
                "useLegacySql": False
            }
        }
    )



