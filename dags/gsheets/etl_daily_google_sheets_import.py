from datetime import datetime, timedelta
import pendulum
import os
import yaml
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from airflow.providers.google.cloud.operators.bigquery import BigQueryUpdateTableOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.operators.dummy_operator import DummyOperator

BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")

gsheets_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/config/"

DEFAULT_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=3),
}

with DAG(
        default_args=default_args,
        dag_id="etl_daily_google_sheets_import",
        schedule_interval="0 0 * * *",
        start_date=datetime(2023, 4, 23, tzinfo=DEFAULT_TZ),
        catchup=False,
        max_active_runs=1
) as dag:
    gsheets_to_bq_start = DummyOperator(task_id="gsheets_to_bq_start")
    gsheets_to_bq_end = DummyOperator(task_id="gsheets_to_bq_end")
    with TaskGroup("gsheets_process", tooltip="Tasks for processing gsheets data") as gsheets_process:

        yaml_files = [
            f for f in os.listdir(gsheets_PATH) if f.endswith(".yml")
        ]

        for f in yaml_files:
            s = open(os.path.join(gsheets_PATH, f), "r")
            yload = yaml.safe_load(s)
            s.close()
            task_id = f.split(".")[0]
            columns = yload.get('columns')
            destination = yload.get('destination')
            primary_keys = yload.get("primaryKeys")
            allow_dup = yload.get("allowDup")
            url = yload.get("url")
            owner = yload.get("owner")
            description = yload.get("description")

            if allow_dup is None:
                allow_dup = False

            bq_project = Variable.get("bq_data_warehouse_project")
            bq_dataset = "viator_gsheets"
            bq_table = destination.strip()
            external_dataset = "staging"
            external_table = f"{bq_dataset}_{bq_table}_external"

            schema = yload.get("schema")
            # default all column to string type
            schema_fields = []
            if schema is None:
                for name in columns.split(","):
                    col_schema = {"name": name.strip(), "type": "STRING"}
                    schema_fields.append(col_schema)
            else:
                for name in schema.replace("'", "").split(","):
                    col_schema = {"name": name.strip().split(":")[0], "type": name.split(":")[1].strip()}
                    schema_fields.append(col_schema)

            skip_header = yload.get("skipHeader")
            if skip_header is None:
                skip_leading_rows = 1
            elif skip_header.lower() == "false":
                skip_leading_rows = 0
            else:
                skip_leading_rows = 1

            create_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"create_external_{task_id}_table",
                bigquery_conn_id=BQ_CONN_ID,
                table_resource={
                    "tableReference": {
                        "projectId": bq_project,
                        "datasetId": external_dataset,
                        "tableId": external_table,
                    },
                    "schema": {
                        "fields": schema_fields
                    },
                    "externalDataConfiguration": {
                        "sourceUris": [url],
                        "sourceFormat": "GOOGLE_SHEETS",
                        "compression": "NONE",
                        "googleSheetsOptions": {"skipLeadingRows": skip_leading_rows},
                    },
                },
            )
            # apply validations and load data into staging table
            where_condition = columns.replace(",", " is not null OR  ")
            where_condition = f" where {where_condition} is not null "

            if allow_dup:
                query = f"SELECT * FROM {bq_project}.{external_dataset}.{external_table} {where_condition}"
            else:
                query = f"SELECT DISTINCT * FROM {bq_project}.{external_dataset}.{external_table} {where_condition}"

            apply_load_validation_task = BigQueryInsertJobOperator(
                task_id=f"apply_validations_and_load_{task_id}_data_dest",
                gcp_conn_id=BQ_CONN_ID,
                project_id=bq_project,
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,
                        "writeDisposition": "WRITE_TRUNCATE",
                        "create_disposition": "CREATE_IF_NEEDED",
                        'destinationTable': {
                            'projectId': bq_project,
                            'datasetId': bq_dataset,
                            'tableId': bq_table
                        },
                    }
                },
            )

            # set description to that table
            if description is None:
                description = ""
            complete_description = "Airflow automatically populated this table from Google Sheets {} . {}".format(
                url, description
            )

            set_table_description = BigQueryUpdateTableOperator(
                task_id=f"set_description_to_{task_id}_destination_table",
                gcp_conn_id=BQ_CONN_ID,
                project_id=bq_project,
                dataset_id=bq_dataset,
                table_id=bq_table,
                fields=["description"],
                table_resource={
                    "description": complete_description,
                },
            )
            create_external_table_task >> apply_load_validation_task >> set_table_description
    gsheets_to_bq_start >> gsheets_process >> gsheets_to_bq_end
