from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from package.operator.bigquery_legacy_operator import (bq_legacy_validations, quality_check_v2,check_source_query)
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator,
                                                               BigQueryCheckOperator,
                                                               BigQueryUpsertTableOperator)
from package.utils.airflow_render_jinja  import render_jinja
from airflow.operators.python import PythonOperator
from package.operator.analytics_operators import write_labels
from functools import partial
from simon.monitoring_helper import monitor_callback
from airflow.utils.task_group import TaskGroup

#------calculated variables----#
tz_utc = pendulum.timezone("UTC")
base_dir = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(base_dir, 'configs')
sql_dir = os.path.join(base_dir, 'sql')
BQ_DATALAKE_PROJECT=Variable.get("bq_data_lake_project")
BQ_CONN_ID=Variable.get("bq_data_warehouse_connection")
base_project_env = Variable.get('bq_data_warehouse_project')
########################################

########################################
#-----variables---------#
dag_id = "etl_availability_daily_jobs"
legacy_table="avr-warehouse.VISaintData.v2_prodavaildate"
migrated_table=f"{base_project_env}.vi_prod_inventory.v2_prodavaildate"
#########################################
source_bq_project = legacy_table.split(".")[0]
########################################################
destination_bq_dataset = migrated_table.split(".")[1]
destination_bq_table = migrated_table.split(".")[2]
#################################
basic_operator_params = {
"gcp_conn_id": BQ_CONN_ID,
"project_id": base_project_env,
"dataset_id": destination_bq_dataset,
"table_id": destination_bq_table,
"dag_id":  dag_id
}

def cron_to_label(cron_str):
    parts = cron_str.split()

    if len(parts) != 5:
        return "invalid_cron_format"

    minute, hour, day, month, weekday = parts

    if hour == '*' and day == '*' and month == '*' and weekday == '*':
        return f"runs_every_hour_at_{minute}"

    if day == '*' and month == '*' and weekday == '*':
        return f"runs_daily_at_{hour}_{minute}_utc"

    return "unrecognized_cron_pattern"

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 4, tzinfo=tz_utc),
    "email": Variable.get("VI_AF_NO_SLA_ALERT"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=5),
    "on_failure": partial(monitor_callback, basic_operator_params),
}

# This is the DAG definition. This is scheduled to run every hour
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    user_defined_filters={
        "UTC": lambda d: tz_utc.convert(d),
        "UTC_DS": lambda d: d.strftime("%Y-%m-%d"),
        "UTC_NODASH": lambda d: d.strftime("%Y%m%d"),
        "H": lambda d: d.strftime("%H"),
        "NYC_DS_H": lambda d: d.strftime("%Y/%m/%d/%H"),
    },
    schedule_interval="0 9 * * *",
    max_active_runs=1,
    catchup=False,
)


start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)

check_partition = BigQueryCheckOperator( #TODO: IMPROVE 
                        task_id=f'check_{destination_bq_table}_last_modification',
                        use_legacy_sql=False,
                        gcp_conn_id=BQ_CONN_ID,
                        sql=check_source_query(
                            table_ids=["viator-availability-prod.vi_prod_inventory.inventory_availability",
                                       "viator-availability-prod.vi_prod_inventory.inventory_metadata_booking_cutoff",
                                       "viator-availability-prod.vi_prod_inventory.inventory_pricing_history"],
                            target_table_id=migrated_table,
                            condition="any"
                            ),
                        location='US',
                        dag=dag,
                        retry_delay=timedelta(hours=5)
                    )


with TaskGroup(group_id=f"load_{destination_bq_table}", dag=dag) as first_step:
    run_query = BigQueryInsertJobOperator(
        task_id="insert_v2_prodavaildate",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": render_jinja(
                    path_base=sql_dir, 
                    sql_path='v2_prodavaildate.sql',
                ),
                "useLegacySql": False,
                "writeDisposition": 'WRITE_TRUNCATE',
                "createDisposition": 'CREATE_IF_NEEDED',
                "timePartitioning": {
                        "type": "MONTH",
                        "field": "m_partition_traveldate"
                },
                "clustering": {
                "fields": ["travel_date", "prod_tour_grade", "start_time"]
              }
                ,
                "destinationTable": {
                    "projectId": base_project_env,
                    "datasetId": destination_bq_dataset,
                    "tableId": destination_bq_table
                }
                 },
                 },
        project_id=base_project_env,
        dag=dag,
        on_success_callback=partial(monitor_callback, basic_operator_params)
    )

    update_labels =  PythonOperator(
        task_id="update_labels",
        python_callable=write_labels,
        dag=dag,
        op_kwargs={
            'dag': dag, 
            'task_id': "insert_v2_prodavaildate",
            'destination_table':migrated_table
            }
    )
    run_query >> update_labels


    #insert_v2_prodavaildate = BigQueryUpsertTableOperator(
    #        task_id=f"insert_v2_prodavaildate",
    #        gcp_conn_id=BQ_CONN_ID,
    #        project_id=base_project_env,
    #        dataset_id=destination_bq_dataset,
    #        table_resource={
    #            "tableReference": {"tableId": destination_bq_table},
    #            "description": "Migrated table",
    #            "labels": {
    #                "job_name": dag.dag_id.lower(),
    #                "task_name": "insert_v2_prodavaildate",
    #                "schedule_interval": cron_to_label(dag.schedule_interval)
    #            },
    #             "timePartitioning": {
    #                "type": "MONTH",
    #                    "field": "m_partition_traveldate"
    #            },
    #            "clustering": {
    #            "fields": ["travel_date", "prod_tour_grade", "start_time"]
    #            },
    #            "createDisposition": "CREATE_IF_NEEDED",
    #            "writeDisposition": "WRITE_TRUNCATE",
    #        },
    #        dag=dag,
    #        on_success_callback=partial(monitor_callback, basic_operator_params)
    #    )
#
    #insert_v2_prodavaildate

    
basic_operator_params['table_id'] ="v2_prodavaildetail"
with TaskGroup(group_id=f"load_v2_prodavaildetail", dag=dag) as second_step:
    #insert_v2_prodavaildetail = BigQueryUpsertTableOperator(
    #        task_id=f"insert_v2_prodavaildetail",
    #        gcp_conn_id=BQ_CONN_ID,
    #        project_id=base_project_env,
    #        dataset_id=destination_bq_dataset,
    #        table_resource={
    #            "tableReference": {"tableId": "v2_prodavaildetail"},
    #            "description": "Migrated table",
    #            "labels": {
    #                "job_name": dag.dag_id.lower(),
    #                "task_name": "insert_v2_prodavaildate",
    #                "schedule_interval": cron_to_label(dag.schedule_interval)
    #            },
    #            "clustering": {
    #            "fields": ["product_code", "tour_grade_code", "season_from", "season_to"]
    #            },
    #             "createDisposition": "CREATE_IF_NEEDED",
    #            "writeDisposition": "WRITE_TRUNCATE",
    #        },
    #        dag=dag,
    #        on_success_callback=partial(monitor_callback, basic_operator_params)
    #    )

    #insert_v2_prodavaildetail 
    run_query = BigQueryInsertJobOperator(
        task_id="insert_v2_prodavaildetail",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": render_jinja(
                    path_base=sql_dir, 
                    sql_path='v2_prodavaildetail.sql',
                ),
                "useLegacySql": False,
                "writeDisposition": 'WRITE_TRUNCATE',
                "createDisposition": 'CREATE_IF_NEEDED',
                "clustering": {
                "fields": ["product_code", "tour_grade_code", "season_from", "season_to"]
                },
                "destinationTable": {
                    "projectId": base_project_env,
                    "datasetId": destination_bq_dataset,
                    "tableId": "v2_prodavaildetail"
                }
            },
        },
        project_id=base_project_env,
        dag=dag,
        on_success_callback=partial(monitor_callback, basic_operator_params)
    )

    update_labels =  PythonOperator(
        task_id="update_labels",
        python_callable=write_labels,
        dag=dag,
        op_kwargs={
            'dag': dag, 
            'task_id': "insert_v2_prodavaildetail",
            'destination_table':f"{base_project_env}.{destination_bq_dataset}.v2_prodavaildetail"
            }
    )

    run_query >> update_labels

    start_task >> check_partition >> first_step >> second_step >> end_task