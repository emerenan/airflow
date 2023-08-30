from datetime import datetime, timedelta
import pendulum
import os
import yaml
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from package.operator.bigquery_legacy_operator import (
    bq_legacy_validations,
    create_yaml_file
)
from package.utils.slack_alerts import slack_fail_alerts

"""
This DAG creates yaml files from Google sheets, that will be used to generate tasks for "process_client_reporting_daily" DAG.
"""
DAILY_BOOKING_GSHEET_URL = "https://docs.google.com/spreadsheets/d/18nKw3XpMiVcJEVROZvo19NETZ_hNSifUJ_DnLfJp31k/edit?usp=sharing"
DAILY_TRAFFIC_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1e1x5DAeIzeui1XVkGBUcHscw4R7KNSTPra0KE90H2D0/edit?usp=sharing"
BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
BASE_PATH = os.path.abspath(os.path.dirname(__file__))

DEFAULT_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": Variable.get("HIGH_PRIORITY_WH_ALERT_EMAIL"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=2),
}

dag = DAG(
    dag_id="update_client_reporting",
    start_date = datetime(2023, 8, 23, tzinfo=DEFAULT_TZ),
    default_args=default_args,
    on_failure_callback=slack_fail_alerts,
    catchup=False,
    schedule_interval="@daily",
)

load_booking_report_config_from_google_sheet = BigQueryCreateExternalTableOperator(
    task_id=f"load_booking_report_config_from_google_sheet",
    bigquery_conn_id=BQ_CONN_ID,
    table_resource={
        "tableReference": {
            "projectId": "avr-warehouse",
            "datasetId": "ATTRDimensions",
            "tableId": "ExperiencesDailyBookingReportPartners",
        },
        "schema": {
            "fields": "Partner:STRING,Email:STRING,Status:STRING,PUID:STRING,Threshold:INTEGER,AccountManager:STRING",
        },
        "externalDataConfiguration": {
            "sourceUris": [DAILY_BOOKING_GSHEET_URL],
            "sourceFormat": "GOOGLE_SHEETS",
            "compression": "NONE",
            "googleSheetsOptions": {"skipLeadingRows": 0},
        },
    },
    pool="gsheet_import",
    retries=1,
    dag=dag
)


load_traffic_report_config_from_google_sheet = BigQueryCreateExternalTableOperator(
    task_id=f"load_traffic_report_config_from_google_sheet",
    bigquery_conn_id=BQ_CONN_ID,
    table_resource={
        "tableReference": {
            "projectId": "avr-warehouse",
            "datasetId": "ATTRDimensions",
            "tableId": "ExperiencesDailyTrafficReportPartners",
        },
        "schema": {
            "fields": "Partner:STRING,Email:STRING,Status:STRING,PUID:STRING,Threshold:INTEGER,AccountManager:STRING",
        },
        "externalDataConfiguration": {
            "sourceUris": [DAILY_TRAFFIC_GSHEET_URL],
            "sourceFormat": "GOOGLE_SHEETS",
            "compression": "NONE",
            "googleSheetsOptions": {"skipLeadingRows": 0},
        },
    },
    pool="gsheet_import",
    retries=1,
    dag=dag
)

# Data validation of imported data
# Emails should be a semi-colon separated list of email addresses.
# status is either active or inactive
# PUID is a comma separated list of integers
booking_gs_data_validation = bq_legacy_validations(
    dag=dag,
    task_group_id="booking_gs_data_validation",
    expected_result_queries={
    "select count(1) qtd from `avr-warehouse.ATTRDimensions.ExperiencesDailyBookingReportPartners` where regexp_contains(Email,'([a-zA-Z0-9_.+-]+)@[a-zA-Z0-9_.+-]+\\.[a-zA-Z0-9_.+-]') is not true and Email is not null and date(_PARTITIONTIME) = <yesterday>;" : "0",
    "select count(1) qtd from `avr-warehouse.ATTRDimensions.ExperiencesDailyBookingReportPartners` where Status != 'active' and Status != 'inactive' and date(_PARTITIONTIME) = <yesterday>;" : "0",
    },
    queries_to_count=["""
    select 
        count(1)
    from (
        select * from `avr-warehouse.ATTRDimensions.ExperiencesDailyBookingReportPartners`
        where (PUID is not null or PUID != "")
        and date(_PARTITIONTIME) = <yesterday>
    )
    having count(split(PUID, ',')) > 0;
    """],
    poke_interval=60 * 15,  # (seconds); checking every 15 minutes
    timeout=1800,  # (seconds); timeout after 30 minutes
)

traffic_gs_data_validation = bq_legacy_validations(
    dag=dag,
    task_group_id="traffic_gs_data_validation",
    expected_result_queries={
    "select count(1) qtd from `avr-warehouse.ATTRDimensions.ExperiencesDailyTrafficReportPartners` where regexp_contains(Email,'([a-zA-Z0-9_.+-]+)@[a-zA-Z0-9_.+-]+\\.[a-zA-Z0-9_.+-]'') is not true and Email is not null and date(_PARTITIONTIME) = <yesterday>;" : "0",
    "select count(1) qtd from `avr-warehouse.ATTRDimensions.ExperiencesDailyTrafficReportPartners` where Status != 'active' and Status != 'inactive' and date(_PARTITIONTIME) = <yesterday>;"  : "0",
    },
    queries_to_count=["""
    select 
        count(1)
    from (
        select * from `avr-warehouse.ATTRDimensions.ExperiencesDailyTrafficReportPartners`
        where (PUID is not null or PUID != "")
        and date(_PARTITIONTIME) = <yesterday>
    )
    having count(split(PUID, ',')) > 0;
    """],
    poke_interval=60 * 15,  # (seconds); checking every 15 minutes
    timeout=1800,  # (seconds); timeout after 30 minutes
)

# Find and register new email addresses to UNP
#register_booking_subscriber_keys = PythonOperator(
#    task_id="register_booking_subscriber_keys",
#    python_callable=register_subscriber_keys,
#    provide_context=False,
#    op_kwargs={
#        "bq_dataset": "ATTRDimensions",
#        "bq_table": "ExperiencesDailyBookingReportPartners",
#    },
#    trigger_rule="all_done",
#    retries=1,
#    dag=dag,
#)
#
#register_traffic_subscriber_keys = PythonOperator(
#    task_id="register_traffic_subscriber_keys",
#    python_callable=register_subscriber_keys,
#    provide_context=False,
#    op_kwargs={
#        "bq_dataset": "ATTRDimensions",
#        "bq_table": "ExperiencesDailyTrafficReportPartners",
#    },
#    trigger_rule="all_done",
#    retries=1,
#    dag=dag,
#)

# Generate yaml file from Google Sheet
generate_booking_yaml = PythonOperator(
    task_id="generate_booking_yaml",
    python_callable=create_yaml_file,
    op_kwargs={
        "report_name": "Experiences Daily Booking Report",
        "arg_count": 1,
        "dir_path": BASE_PATH
    },
    retries=1,
    dag=dag,
)

generate_traffic_yaml = PythonOperator(
    task_id="generate_traffic_yaml",
    python_callable=create_yaml_file,
    op_kwargs={
        "report_name": "Experiences Daily Traffic Report",
        "arg_count": 1,
        "dir_path": BASE_PATH
    },
    retries=1,
    dag=dag,
)

start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)

start >> load_booking_report_config_from_google_sheet >> booking_gs_data_validation >> generate_booking_yaml >> end
start >> load_traffic_report_config_from_google_sheet >> traffic_gs_data_validation >> generate_traffic_yaml >> end
