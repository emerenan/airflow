import os
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG

from package.utils.slack_alerts import slack_fail_alerts
from appsflyer.factory.factory import backfill_factory

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
    schedule_interval="15 * * * *",
    start_date=datetime(2023, 1, 19, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    tags=["Appsflyer", "Events"]
)

events = ['appsflyer_events', 'appsflyer_installs', 'appsflyer_skad']

for event in events:
    
    backfill_factory(
        dag=dag,
        sql_path_base=sql_dir,
        event=event,
        dt_filter='d-1',
        ref_datetime=CURRENT_DATETIME,
    )




