#
#   Ingestion of snapshot tables: Nsp; Visitor.
#
import os
import yaml
import logging
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryUpsertTableOperator
)

# VARIABLES
DEFAULT_TZ = pendulum.timezone("UTC")
BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
BQ_PROJECT_ID = Variable.get("bq_data_warehouse_project")
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")

default_args = {
    "owner": "Analytics Engineering Team",
    "depends_on_past": False,
    "email": [], 
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=6),
    "execution_timeout": timedelta(hours=6),
}

"""
  This function is used to load partition tables
"""
def load_partition(file_path,BQ_CONN_ID,dag, start_task, end_task, ds_context_key="ds"):

    partition_date= f"{{{{ ds_nodash }}}}"
    #partition_date=f"{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}}}"
    
    if not os.path.isfile(file_path):
        logging.warning("Cannot find file: " + file_path) 
        start_task >> end_task
        return
    try:
        with open(file_path, "r") as f:
            yload = yaml.safe_load(f)
    except Exception as e:
        logging.error("Failed reading file: " + e)

    bq_project = yload["bqProject"]
    bq_dataset = yload["bqDataset"]

    for key, value in yload["tables"].items(): 
        taskid = key + "_" + ds_context_key
        query = value["query"]
        bq_table = value["bqTable"]
        table_description = value.get("description", "")

        run_query = BigQueryInsertJobOperator(
            task_id= f"{taskid}_run_query",
            gcp_conn_id=BQ_CONN_ID, 
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE", 
                    "priority": "INTERACTIVE",
                    "destinationTable": {
                        "projectId": bq_project,
                        "datasetId": bq_dataset,
                        "tableId":f"{bq_table}${partition_date}"
                    },
                    "timePartitioning": {
                        'type': 'DAY'
                }
            },
        },
              dag=dag
        )

        upsert_table_description = BigQueryUpsertTableOperator(
            task_id=f"{taskid}_upsert_table",
            gcp_conn_id=BQ_CONN_ID,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_resource={
                "tableReference": {"tableId": bq_table},
                "description": table_description,
                "labels": {
                    "job_name": dag.dag_id.lower(),
                    "task_name": taskid.lower(),
                },
            },
            dag=dag,
        )


        start_task >> run_query >> upsert_table_description >> end_task

# DAG definition
with DAG(
    default_args=default_args,
    dag_id="snapshot_ingestion",
    schedule_interval="0 6 * * *",
    start_date=datetime(2023, 7, 7, tzinfo=DEFAULT_TZ),
    catchup=False,
) as dag:
    
# *********************************** SNAPSHOT: Generate visitor partitions ***********************************
    with TaskGroup('generate_visitor_partitions') as generate_visitor: 
        load_partition(f"{CONFIG_DIR}/visitor/visitors.yml",
                        BQ_CONN_ID,
                        dag,
                        EmptyOperator(task_id="load_visitor_start", dag=dag),
                        EmptyOperator(task_id="load_visitor_end", dag=dag )
            
            )
# *********************************** SNAPSHOT: Generate NSP partitions ***********************************
    with TaskGroup('generate_nsp_partitions') as generate_nsp:

        with TaskGroup('load_product') as generate_product: 
            load_partition(f"{CONFIG_DIR}/nsp/prod_product_snapshot.yml",
                            BQ_CONN_ID,
                            dag,
                            EmptyOperator(task_id="load_product_start", dag=dag),
                            EmptyOperator(task_id="load_product_end", dag=dag ) 
            )

        with TaskGroup('load_media') as generate_media: 
            load_partition(f"{CONFIG_DIR}/nsp/prod_media_snapshot.yml",
                            BQ_CONN_ID,
                            dag,
                            EmptyOperator(task_id="load_media_start", dag=dag),
                            EmptyOperator(task_id="load_media_end", dag=dag ,)
            )

        with TaskGroup('load_tag') as generate_media: 
            load_partition(f"{CONFIG_DIR}/nsp/prod_tag_snapshot.yml",
                            BQ_CONN_ID,
                            dag,
                            EmptyOperator(task_id="load_tag_start", dag=dag),
                            EmptyOperator(task_id="load_tag_end", dag=dag ,)
            )

        with TaskGroup('load_supplyperf') as generate_supplyperf: 
            load_partition(f"{CONFIG_DIR}/nsp/prod_supplyperf_snapshot.yml",
                            BQ_CONN_ID,
                            dag,
                            EmptyOperator(task_id="load_supplyperf_start", dag=dag),
                            EmptyOperator(task_id="load_supplyperf_end", dag=dag ,)
            )

    Start_loading_partitions = EmptyOperator(task_id="start_loading_partitions", dag=dag)
    End_loading_partitions = EmptyOperator(task_id="end_loading_partitions", dag=dag)

    Start_loading_partitions >> generate_visitor >> End_loading_partitions
    Start_loading_partitions >> generate_nsp >> End_loading_partitions
    