import os
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG

from appsflyer.factory.factory import factory
from package.utils.slack_alerts import slack_fail_alerts

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
    tags=["Appsflyer", "Installs"]
)

skad_event = 'appsflyer_installs'

factory(
    dag=dag,
    sql_path_base=sql_dir,
    event=skad_event,
    ref_date=f"{{{{ ds }}}}",
    ref_datetime=CURRENT_DATETIME
)


