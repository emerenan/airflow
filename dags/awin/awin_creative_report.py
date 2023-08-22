import os
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from package.utils.slack_alerts import slack_fail_alerts
from package.operator.analytics_operators import awin_etl

AWIN_AUTH = Variable.get("AWIN_AUTH")
SQL_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/sql/"
dag_name = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "email": ["VI_ANALYTICS_ENGINEERING@tripadvisor.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id=dag_name,
    schedule_interval="30 1 * * *",
    start_date=pendulum.datetime(2023, 5, 12, tz="UTC"),
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    doc_md=open(f"{os.path.abspath(os.path.dirname(__file__))}/docs.md", 'r').read(),
    tags=["botify"]
) as dag:
    
    ref_date = f"{{{{ ds }}}}"
    awin_report = 'creative'
    
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def get_advertisers_list():
        """
            Use the AWIN API to get current advertiser list
            We remove the ones with "CLOSED" tag on account Name
            Ex: "accountName": "TripAdvisor Rentals AU - CLOSED 30.04.2020"
        """
        try:
            resp = requests.get(
                url="https://api.awin.com/accounts?type=advertiser", 
                headers={
                    'Authorization': AWIN_AUTH
                    }).json()
            
            if "error" in resp.keys():
                raise Exception(f"Request state: {resp['error']}\n"
                                f"Request Description {resp['description']}")
            accounts = [x['accountId'] for x in resp['accounts'] if "CLOSED" not in x['accountName']]
        except Exception as e:
            raise e
        return accounts
    
    acc_list = get_advertisers_list()
    
    base_etl = awin_etl(
        dag_name=dag, 
        path_sql=SQL_PATH, 
        report=awin_report,
        account_list=acc_list,
        ref_date=ref_date, 
    )

    
