from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from utils.airflow_render_jinja  import render_jinja
from yaml import safe_load

from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator

from airflow.utils.helpers import chain
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator, 
    BigQueryDeleteTableOperator
)

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

AWS_S3_CONN_ID = Variable.get("s3_assume_role_connection")
ATHENA_S3_BUCKET = Variable.get("s3_athena_unload_bucket")   
ATHENA_DB = Variable.get("athena_braze_database")

BQ_GCP_CONN_ID = Variable.get("bq_data_lake_connection")
BQ_LAKE_PROJECT = Variable.get("bq_data_lake_project")

BQ_DWH_CONN_ID = Variable.get("bq_data_warehouse_connection")
BQ_DWH_PROJECT = Variable.get("bq_data_warehouse_project") 

BQ_OMNI_CONN_ID = Variable.get("bq_omni_connection_name")
BQ_OMNI_CONN_LOCATION = Variable.get("bq_omni_connection_location", "us-east-1")

def athena_extraction(dag, braze_event, etl_type, etl_filter, sql_path):
    
    BUCKET_RESULT = f"athena_unload_result/{ATHENA_DB}/etl_{braze_event}_{etl_type}"
    URI = f"s3://{ATHENA_S3_BUCKET}/{BUCKET_RESULT}"
    
    @task(task_id=f"cleaning_etl_{braze_event}_bucket", dag=dag)
    def delete_objects(aws_conn_id, bucket, prefix):
        
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        def chunks(lst, n):
            for i in range(0, len(lst)):
                yield lst[i:i + n]
                
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        object_keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        if object_keys:
            batches = chunks(object_keys, 1000)
            for batch in batches:
                s3_hook.delete_objects(bucket=bucket, keys=batch)
                
        print('Error!')
                
    with TaskGroup(group_id=f"{braze_event}_extraction", dag=dag) as athena_extraction:
            
        prepare_for_unload_data = delete_objects(
            aws_conn_id=AWS_S3_CONN_ID,
            bucket=ATHENA_S3_BUCKET,
            prefix=BUCKET_RESULT
        )
        
        athena_unload_data = AWSAthenaOperator(
            task_id=f'athena_unload_{braze_event}_data',
            aws_conn_id=AWS_S3_CONN_ID,
            query=f"/athena/unload.sql",
            params={
                'event': braze_event,
                'etl_type': etl_type,
                'filter': etl_filter,
                's3_bucket': ATHENA_S3_BUCKET,
                'prefix_result': BUCKET_RESULT,
            },
            database=ATHENA_DB,
            output_location=f"s3://{ATHENA_S3_BUCKET}/athena_query_result",
            dag=dag
        )
        
        load_data_to_landing = BigQueryInsertJobOperator(
            task_id=f"load_{braze_event}_data_landing",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path, 
                        sql_path='bigquery/load_aws_to_bigquery.sql', 
                        sql_params={
                            'dwh_project_id': BQ_LAKE_PROJECT,
                            'dataset_id': "data_landing_braze",
                            'table_id': braze_event,
                            's3_uri': URI,
                            'gcp_conn': BQ_OMNI_CONN_ID
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )

        prepare_for_unload_data >> athena_unload_data >> load_data_to_landing
        
    return athena_extraction


def bigquery_transformation(dag, braze_event, sql_path, tables, test_name=''):  
    
    DWH_TABLE_ID = f"{braze_event}{test_name}"
    
    with TaskGroup(group_id=f"bigquery_{braze_event}_load", dag=dag) as bq_load:
        
        loading_to_dwh_temp = BigQueryInsertJobOperator(
            task_id=f"loading_to_dwh_{braze_event}_temp_table",
            gcp_conn_id=BQ_GCP_CONN_ID,
            project_id=BQ_LAKE_PROJECT,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path, 
                        sql_path=f'bigquery/create_{braze_event}_temp.sql', 
                        sql_params={
                            "project_id": BQ_LAKE_PROJECT,
                            "dataset_id": "data_landing_braze",
                        },
                    ),
                    "useLegacySql": False,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "destinationTable": {
                        "project_id": BQ_LAKE_PROJECT,
                        "dataset_id": "data_landing_braze",
                        "table_id": f"{braze_event}_temp"
                    },
                }
            },
            trigger_rule=TriggerRule.NONE_SKIPPED,
            dag=dag
        )
    
        merge_data = BigQueryInsertJobOperator(
            task_id=f"merge_{braze_event}_data",
            gcp_conn_id=BQ_DWH_CONN_ID,
            project_id=BQ_DWH_PROJECT,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path, 
                        sql_path='bigquery/merge_tables.sql', 
                        sql_params={
                            "dwh_project_id": BQ_DWH_PROJECT,
                            "dwh_dataset_id": 'viator_braze',
                            "final_table": DWH_TABLE_ID,
                            "lake_project_id": BQ_LAKE_PROJECT,
                            "lake_dataset_id": "data_landing_braze",
                            "temp_table": f"{braze_event}_temp",
                            "partition_field": "event_date",
                            "ref_column": 'id',
                        },
                    ),
                    "useLegacySql": False,
                },
            },
            dag=dag
        )
        
        delete_temp_table = BigQueryDeleteTableOperator(
            task_id=f"delete_{braze_event}_temp_table",
            deletion_dataset_table=f"{BQ_LAKE_PROJECT}.data_landing_braze.{braze_event}_temp",
            gcp_conn_id=BQ_GCP_CONN_ID,
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )
        
        truncate_land_tables = BigQueryInsertJobOperator(
            task_id=f"truncate_{braze_event}_landing_tables",
            gcp_conn_id=BQ_GCP_CONN_ID,
            project_id=BQ_LAKE_PROJECT,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path, 
                        sql_path=f'bigquery/truncate_tables.sql', 
                        sql_params={
                            "project_id": BQ_LAKE_PROJECT,
                            "dataset_id": "data_landing_braze",
                            "tables": tables
                        },
                    ),
                    "useLegacySql": False,
                }
            },
            dag=dag
        )
        
        loading_to_dwh_temp >> merge_data >> [delete_temp_table, truncate_land_tables]
    
    return bq_load

        
def braze_job(dag, config_path, braze_event, etl_type, etl_filter, sql_path, test_name=''):
    
    from pathlib import Path
    
    event_tables = safe_load(open(Path(config_path), "r"))
    
    start_process = DummyOperator(
        task_id='start_process',
        dag=dag  
    )
    
    end_process = DummyOperator(
        task_id='end_process',
        dag=dag  
    )
    
    with TaskGroup(group_id=f"extract_process", dag=dag) as extraction_process:
        
        for table in event_tables[braze_event]:
            
            etl_event = athena_extraction(
                dag=dag,
                braze_event= table,
                etl_type=etl_type,
                etl_filter=etl_filter,
                sql_path=sql_path,
            )
    
    with TaskGroup(group_id=f"transform_process", dag=dag) as transformation_process:
        
        transformation = bigquery_transformation(
            dag=dag,
            braze_event=braze_event, 
            sql_path=sql_path, 
            tables=event_tables[braze_event], 
            test_name=test_name)     
            
    return start_process >> extraction_process >> transformation_process >> end_process


