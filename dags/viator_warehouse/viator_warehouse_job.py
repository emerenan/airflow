import pendulum
import os
import glob

from datetime import datetime, timedelta
from airflow.models import DAG

from package.factory.factory import viator_warehouse_factory
from package.utils.slack_alerts import slack_fail_alerts

#from utils.monitoring.slack_hook import tasks_fail_slack_alert

DEFAULT_TZ = pendulum.timezone("UTC")
DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
SQL_DIR = f"{os.path.abspath(os.path.dirname(__file__))}/sql/"
CONFIG = f"{os.path.abspath(os.path.dirname(__file__))}/config/"

default_args = {
    "owner": "Analytics",
    "depends_on_past": False,
    "email": ["vi-apac-data-eng@tripadvisor.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

# This DAG below do data extraction from S3 Buckets to DWH Landing Dataset
with DAG(
    default_args=default_args,
    dag_id=DAG_NAME,
    schedule_interval="* 6 * * *",
    start_date=datetime(2023, 3, 8, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    tags=["dwh_jobs"],
) as dag:
    
    viator_warehouse_factory(
        dag=dag,
        config_path=CONFIG,
        sql_dir=SQL_DIR,
        job_name=DAG_NAME
    )