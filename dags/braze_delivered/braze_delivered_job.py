import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from utils.airflow_render_jinja  import render_jinja

import pendulum
from package.utils.slack_alerts import slack_fail_alerts


DEFAULT_TZ = pendulum.timezone("UTC")

sql_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 
    'sql'
)

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    default_args=default_args,
    dag_id="braze_delivered_job",
    schedule_interval="0 6 * * *",
    start_date=datetime(2023, 7, 25, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    tags=["Simon"]
)

BQ_GCP_CONN_ID = Variable.get("bq_data_lake_connection")
BQ_DWH_PROJECT = Variable.get("bq_data_warehouse_project")
   
load_braze_simon = BigQueryInsertJobOperator(
    task_id=f"load_braze_data_to_simon",
    gcp_conn_id=BQ_GCP_CONN_ID,
    project_id=BQ_DWH_PROJECT,
    configuration={
        "query": {
            "query": render_jinja(
                path_base=sql_dir,
                sql_path='braze_events.sql',
                sql_params={
                    "dwh_project_id": BQ_DWH_PROJECT,
                    "simon_dataset_id": "viator_simon",
                    "delivered_table": f"braze_delivered_events",
                    "dwh_dataset_id": "viator_braze",
                    "dwh_table": "message_engagement"
                }
            ),
            "useLegacySql": False,
        }
    },
    dag=dag
)

