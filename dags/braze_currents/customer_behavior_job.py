import os
from datetime import datetime, timedelta

import pendulum

from airflow.models import DAG
from package.utils.slack_alerts import slack_fail_alerts

from braze_currents.factory.factory import braze_job

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]
DEFAULT_TZ = pendulum.timezone("UTC")
CURRENT_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Folder structure inside Airflow
base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, 'sql')
config_path = os.path.join(base_dir, 'config/customer_behavior.yml')
    
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
    schedule_interval="59 * * * *",
    start_date=datetime(2023, 3, 28, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    template_searchpath=[sql_dir, '/usr/local/airflow/dags/new_braze/'],
    catchup=False,
    tags=["Braze Currents"]
)

braze_job(
    dag=dag,
    config_path=config_path,
    braze_event='customer_behavior',
    etl_type='daily',
    etl_filter='date',
    sql_path=sql_dir,
)