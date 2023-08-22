import pendulum
import os
import glob

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.decorators import dag

from package.operator.analytics_operators import bigquery_transformation, bigquery_backfill
from package.utils.slack_alerts import slack_fail_alerts

DEFAULT_TZ = pendulum.timezone("UTC")
CURRENT_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
SQL_DIR = os.path.join(BASE_DIR, 'sql')
CONFIG = os.path.join(BASE_DIR, 'config')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["vi-apac-data-eng@tripadvisor.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Dates
day_minus_one=f"{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}}}"
day=f"{{{{ ds_nodash }}}}",
    
    
# This DAG below do data extraction from S3 Buckets to DWH Landing Dataset
with DAG(
    default_args=default_args,
    dag_id=DAG_NAME,
    schedule_interval="30 */3 * * *",  # Override to match your needs
    start_date=datetime(2023, 7, 5, tzinfo=DEFAULT_TZ),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    tags=["nsp_adp"],
) as dag:

    for filename in glob.glob(f"{CONFIG}/*.yaml"):
        
        bigquery_transformation(
            dag=dag,
            dag_type='hourly',
            sql_path_base=SQL_DIR,
            job_name=DAG_NAME,
            dag_config=filename,
            ref_date=f"{{{{ ds }}}}",
            ref_datetime= CURRENT_DATETIME
        )
        
        bigquery_backfill(
            dag=dag,
            dag_type='backfill',
            sql_path_base=SQL_DIR,
            dag_config=filename,
            ref_datetime=CURRENT_DATETIME
        )