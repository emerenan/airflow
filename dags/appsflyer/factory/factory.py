from airflow.models import Variable
from airflow.utils.helpers import chain

from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator

from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from utils.airflow_render_jinja  import render_jinja

ATHENA_S3_BUCKET = Variable.get("s3_athena_unload_bucket")
ATHENA_DB = Variable.get("athena_appsflyer_database")
AWS_S3_CONN_ID = Variable.get("s3_assume_role_connection")

BQ_GCP_CONN_ID = Variable.get("bq_data_lake_connection")

BQ_LAKE_PROJECT = Variable.get("bq_data_lake_project")
BQ_DWH_CONN = Variable.get("bq_data_warehouse_connection")
BQ_DWH_PROJECT = Variable.get("bq_data_warehouse_project")

BQ_OMNI_CONN_ID = Variable.get("bq_omni_connection_name")
BQ_OMNI_CONN_LOCATION = Variable.get("bq_omni_connection_location", "us-east-1")

QA_TABLE_ID = 'appsflyers_qa'
  
def athena_extract(dag: str, sql_path_base, event: str, ref_date:str, test_name:str):
        
    if event in ('appsflyer_installs', 'appsflyer_events'):
        athena_db = Variable.get("athena_appsflyer_database")
    else:
        athena_db = Variable.get("athena_appsflyer_skad_database")
    
    BUCKET_RESULT = f"athena_unload_result/{athena_db}/appsflyer_{event}"

    URI = f"""s3://{ATHENA_S3_BUCKET}/{BUCKET_RESULT}"""

    final_table = f"{event}{test_name}"


    with TaskGroup(group_id=f"athena_extract_{event}", dag=dag) as athena_extract:

        start_flow = DummyOperator(task_id="start_flow")
        end_flow = DummyOperator(task_id="end_flow")

        prepare_for_unload_data = S3DeleteObjectsOperator(
            task_id=f"cleaning_extract_{event}_bucket",
            aws_conn_id=AWS_S3_CONN_ID,
            bucket=ATHENA_S3_BUCKET,
            prefix=f"{BUCKET_RESULT}/",
            dag=dag
        )
           
        athena_unload_data = AWSAthenaOperator(
            task_id=f'athena_unload_{event}_data',
            aws_conn_id=AWS_S3_CONN_ID,
            query=render_jinja(
                        path_base=sql_path_base, 
                        sql_path=f'athena/unload_{event}.sql', 
                        sql_params={
                            "s3_bucket": ATHENA_S3_BUCKET,
                            "prefix_result": BUCKET_RESULT,
                            "ref_date": ref_date,
                        }
            ),
            database=athena_db,
            output_location=f"s3://{ATHENA_S3_BUCKET}/athena_query_result",
            dag=dag
        )
        
        load_data_to_landing = BigQueryInsertJobOperator(
            task_id=f"load_{event}_data_to_landing",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/load_aws_to_bigquery.sql', 
                        sql_params={
                            'project_id': BQ_LAKE_PROJECT,
                            'table_id': final_table,
                            's3_uri': URI,
                            'gcp_conn': BQ_OMNI_CONN_ID
                        }
                    ),
                    "useLegacySql": False
                }
            },
            project_id=BQ_LAKE_PROJECT,
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )
            
        start_flow >> prepare_for_unload_data >> athena_unload_data >> load_data_to_landing >> end_flow
    
    return athena_extract


def athena_qa(dag: str, sql_path_base: str ,database:str, event: str, tables: list):
    
    BUCKET_RESULT = f"athena_unload_result/{database}/validation_{event}"
    URI = f"s3://{ATHENA_S3_BUCKET}/{BUCKET_RESULT}"
            
    with TaskGroup(group_id=f"athena_validation_{event}", dag=dag) as athena_validation:
        
        prepare_for_unload_data = S3DeleteObjectsOperator(
            task_id=f"cleaning_extract_{event}_bucket",
            aws_conn_id=AWS_S3_CONN_ID,
            bucket=ATHENA_S3_BUCKET,
            prefix=f"{BUCKET_RESULT}/",
            dag=dag
        )
        
        athena_unload_data = AWSAthenaOperator(
            task_id=f'athena_validation_{event}',
            aws_conn_id=AWS_S3_CONN_ID,
            query="/athena/validation_data.sql",
            params={
                "table_list": tables,
                'event_type': event,
                "s3_bucket": ATHENA_S3_BUCKET,
                "prefix_result": BUCKET_RESULT
            },
            database=database,
            output_location=f"s3://{ATHENA_S3_BUCKET}/athena_query_result",
            dag=dag
        )
        
        load_data_to_landing = BigQueryInsertJobOperator(
            task_id=f"load_{event}_qa_data_to_landing",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/load_aws_to_bigquery.sql', 
                        sql_params={
                            "project_id": BQ_LAKE_PROJECT,
                            "table_id": f'{event}_athena_qa',
                            "s3_uri": URI,
                            "gcp_conn": BQ_OMNI_CONN_ID
                        }
                    ),
                    "useLegacySql": False
                }
            },
            project_id=BQ_LAKE_PROJECT,
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )
        
        load_validation_data = BigQueryInsertJobOperator(
            task_id=f"load_{event}_validation_data",
            gcp_conn_id=BQ_GCP_CONN_ID,
            project_id=BQ_DWH_PROJECT,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/validation_athena.sql',
                        sql_params={
                            "project_id": BQ_LAKE_PROJECT,
                            "table_id": f'{event}_athena_qa'
                        }
                    ),
                    "useLegacySql": False,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "destinationTable": {
                        "project_id": BQ_DWH_PROJECT,
                        "dataset_id": 'logging',
                        "table_id": f'{event}_athena_qa',
                    }
                }
            },
            dag=dag
        )
        
        upsert_qa_table = BigQueryInsertJobOperator(
            task_id=f"upsert_athena_{event}_into_qa_table",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/upsert_qa_table.sql', 
                        sql_params={
                            "table_id": f'{event}_athena_qa'
                        }
                    ),
                    "useLegacySql": False
                }
            },
            project_id=BQ_DWH_PROJECT,
            location='US',
            dag=dag
        )
        
        delete_lake_table = BigQueryDeleteTableOperator(
            task_id=f"delete_landing_athena_{event}_qa_table",
            deletion_dataset_table=f"{BQ_LAKE_PROJECT}.data_landing.viator_appsflyer_{event}_athena_qa",
            gcp_conn_id=BQ_GCP_CONN_ID,
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )
        
        delete_dwh_table = BigQueryDeleteTableOperator(
            task_id=f"delete_dwh_athena_{event}_qa_table",
            deletion_dataset_table=f"{BQ_DWH_PROJECT}.logging.{event}_athena_qa",
            gcp_conn_id=BQ_GCP_CONN_ID,
            location='US',
            dag=dag
        )
        
        chain(
            prepare_for_unload_data, 
            athena_unload_data, 
            load_data_to_landing, 
            load_validation_data,
            upsert_qa_table,
            [delete_lake_table, delete_dwh_table]
            )
        
    return athena_validation


def bigquery_qa(dag: str, sql_path_base: str, event: str, test_name:str):
    
    with TaskGroup(group_id=f"bigquery_dwh_qa_{event}", dag=dag) as bigquery_dwh_qa:
        
        bq_validation_data = BigQueryInsertJobOperator(
            task_id=f"bq_dwh_{event}_qa",
            gcp_conn_id=BQ_GCP_CONN_ID,
            project_id=BQ_LAKE_PROJECT,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/bigquery_qa.sql',
                        sql_params={
                            "table_event": event,
                            "project_id": BQ_DWH_PROJECT,
                            'test': test_name
                        }
                    ),
                    "useLegacySql": False,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "destinationTable": {
                        "project_id": BQ_DWH_PROJECT,
                        "dataset_id": 'logging',
                        "table_id": f"{event}_dwh_qa",
                    }
                }
            },
            dag=dag
        )
        
        upsert_qa_table = BigQueryInsertJobOperator(
            task_id=f"upsert_{event}_dwh_into_qa_table",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/upsert_qa_table.sql', 
                        sql_params={
                            "table_id": f"{event}_dwh_qa"
                        }
                    ),
                    "useLegacySql": False
                }
            },
            project_id=BQ_DWH_PROJECT,
            location='US',
            dag=dag
        )
        
        delete_dwh_qa_table = BigQueryDeleteTableOperator(
            task_id=f"delete_{event}_dwh_qa_table",
            deletion_dataset_table=f"{BQ_DWH_PROJECT}.logging.{event}_dwh_qa",
            gcp_conn_id=BQ_GCP_CONN_ID,
            location='US',
            dag=dag
        )
        
        bq_validation_data >> upsert_qa_table >> delete_dwh_qa_table
    
    return bigquery_dwh_qa

  
def bigquery_dwh_tl(dag: str, sql_path_base: str, event: dict, test_name:str):
           
    FINAL_TABLE = f"viator_{event}{test_name}"
    
    with TaskGroup(group_id=f"bigquery_transform_load_{event}", dag=dag) as bigquery_transform_load:
        
        if event == "appsflyer_events":
            query_template = {
                "query": render_jinja(
                    path_base=sql_path_base, 
                    sql_path=f'bigquery/{event}.sql', 
                    sql_params={
                        "project_id": BQ_LAKE_PROJECT, 
                        "table_id": FINAL_TABLE
                    }
                ),
                "useLegacySql": False,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_APPEND",
                "destinationTable": {
                    "project_id": BQ_DWH_PROJECT,
                    "dataset_id": "viator_appsflyer",
                    "table_id": FINAL_TABLE,
                },
                "timePartitioning": {
                    "type": "DAY",
                    "field": "partition_date",
                },
                "clustering":{
                    "fields": [ "event_name" ]
                },
            }
        else:
            query_template = {
                "query": render_jinja(
                    path_base=sql_path_base, 
                    sql_path=f'bigquery/{event}.sql', 
                    sql_params={
                        "project_id": BQ_LAKE_PROJECT, 
                        "table_id": FINAL_TABLE
                    }
                ),
                "useLegacySql": False,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_APPEND",
                "destinationTable": {
                    "project_id": BQ_DWH_PROJECT,
                    "dataset_id": "viator_appsflyer",
                    "table_id": FINAL_TABLE,
                },
                "timePartitioning": {
                    "type": "DAY",
                    "field": "partition_date",
                },
            }
            
        load_transform_dwh = BigQueryInsertJobOperator(
            task_id=f"loading_to_dwh_{event}",
            gcp_conn_id=BQ_GCP_CONN_ID,
            project_id=BQ_LAKE_PROJECT,
            configuration={
                "query": query_template
                },
            dag=dag
        )
        
        delete_table = BigQueryDeleteTableOperator(
            task_id=f"delete_{FINAL_TABLE}_table",
            deletion_dataset_table=f"{BQ_LAKE_PROJECT}.data_landing.{FINAL_TABLE}",
            gcp_conn_id=BQ_GCP_CONN_ID,
            location=BQ_OMNI_CONN_LOCATION,
            dag=dag
        )
    
        load_transform_dwh >> delete_table
    
    return bigquery_transform_load


def factory(dag: str, sql_path_base:str, event:str, ref_date: str, ref_datetime: str, test_name=''):
    
    end_process = DummyOperator(
        task_id='end_process',
        trigger_rule='none_skipped',
        dag=dag  
    )
    
    with TaskGroup(group_id=f"aws_to_bigquery_{event}", dag=dag) as aws_to_bigquery:
        
        start = DummyOperator(
            task_id='start',
            dag=dag  
        )
        
        insert_config_date = BigQueryInsertJobOperator(
            task_id=f"insert_config_date",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/insert_conf_table.sql', 
                        sql_params={
                            'project_id': BQ_DWH_PROJECT,
                            'ref_date': ref_date,
                            'ref_datetime': ref_datetime,
                            'event_name': event,
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag
        )
                
        athena = athena_extract(
            dag=dag,
            sql_path_base=sql_path_base,
            event=event,
            ref_date=ref_date,
            test_name=test_name
        )
                    
        lake_to_dwh = bigquery_dwh_tl(
            dag=dag,
            sql_path_base=sql_path_base,
            event=event,
            test_name=test_name
        )
        
        update_config_date = BigQueryInsertJobOperator(
            task_id=f"update_config_date",
            gcp_conn_id=BQ_GCP_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='bigquery/update_conf_table.sql', 
                        sql_params={
                            'project_id': BQ_DWH_PROJECT,
                            'ref_date': ref_date,
                            'ref_datetime': ref_datetime,
                            'event_name': event,
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag
        )
                            
        start >> [insert_config_date, athena] >> lake_to_dwh >> update_config_date >> end_process
    
    return aws_to_bigquery


def backfill_factory(dag: str, sql_path_base:str, event:str, dt_filter: str, ref_datetime: str, test_name=''):
    
    from utils.airflow_render_jinja  import render_jinja
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    if event in ('appsflyer_installs', 'appsflyer_events'):
        athena_db = Variable.get("athena_appsflyer_database")
    else:
        athena_db = Variable.get("athena_appsflyer_skad_database")
                
    sql = render_jinja(
        path_base=sql_path_base, 
        sql_path='bigquery/check_backfill.sql', 
        sql_params={
            'project_id': BQ_DWH_PROJECT,
            'event_type': event,
            'dt_filter': dt_filter}
    )
        
    bq = BigQueryHook(gcp_conn_id=BQ_DWH_CONN, use_legacy_sql=False,)
    conn = bq.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    dates= [str(item[0]) for item in cursor.fetchall()]

    FINAL_TABLE = f"viator_{event}{test_name}"
    
    with TaskGroup(group_id=f"{event}_aws_to_bigquery_backfill", dag=dag) as aws_to_bigquery:
        
        for dt in dates:
            
            event_name = f"{event}_{dt.replace('-','')}"
            
            BUCKET_RESULT = f"athena_unload_result/backfill/{event}_{dt.replace('-','')}"

            URI = f"""s3://{ATHENA_S3_BUCKET}/{BUCKET_RESULT}"""
            
            backfill_table = f"backfill_{event}_{dt.replace('-','')}"
            
            with TaskGroup(group_id=f"athena_backfill_{event_name}", dag=dag) as athena_backfill:
                
                prepare_for_unload_data = S3DeleteObjectsOperator(
                    task_id=f"cleaning_bucket_{event_name}",
                    aws_conn_id=AWS_S3_CONN_ID,
                    bucket=ATHENA_S3_BUCKET,
                    prefix=f"{BUCKET_RESULT}/",
                    dag=dag
                )
                            
                athena_unload_data = AWSAthenaOperator(
                    task_id=f'athena_unload_{event_name}',
                    aws_conn_id=AWS_S3_CONN_ID,
                    query=render_jinja(
                                path_base=sql_path_base, 
                                sql_path=f'athena/unload_{event}.sql', 
                                sql_params={
                                    "s3_bucket": ATHENA_S3_BUCKET,
                                    "prefix_result": BUCKET_RESULT,
                                    "ref_date": dt,
                                }
                    ),
                    database=athena_db,
                    output_location=f"s3://{ATHENA_S3_BUCKET}/athena_query_result",
                    dag=dag
                )
                
                load_data_to_landing = BigQueryInsertJobOperator(
                    task_id=f"load_landing_{event_name}",
                    gcp_conn_id=BQ_GCP_CONN_ID, 
                    configuration={
                        "query": {
                            "query": render_jinja(
                                path_base=sql_path_base, 
                                sql_path='bigquery/load_aws_backfill.sql', 
                                sql_params={
                                    'project_id': BQ_LAKE_PROJECT,
                                    'table_id': backfill_table,
                                    's3_uri': URI,
                                    'gcp_conn': BQ_OMNI_CONN_ID
                                }
                            ),
                            "useLegacySql": False
                        }
                    },
                    project_id=BQ_LAKE_PROJECT,
                    location=BQ_OMNI_CONN_LOCATION,
                    dag=dag
                )
                
                prepare_for_unload_data >> athena_unload_data >> load_data_to_landing
    
            with TaskGroup(group_id=f"bigquery_backfill_load_{event_name}", dag=dag) as bigquery_backfill:
                
                if event == "appsflyer_events":
                    query_template = {
                        "query": render_jinja(
                            path_base=sql_path_base, 
                            sql_path=f'bigquery/{event}.sql', 
                            sql_params={
                                "project_id": BQ_LAKE_PROJECT, 
                                "table_id": backfill_table
                            }
                        ),
                        "useLegacySql": False,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_APPEND",
                        "destinationTable": {
                            "project_id": BQ_DWH_PROJECT,
                            "dataset_id": "viator_appsflyer",
                            "table_id": FINAL_TABLE,
                        },
                        "timePartitioning": {
                            "type": "DAY",
                            "field": "partition_date",
                        },
                        "clustering":{
                            "fields": [ "event_name" ]
                        },
                    }
                else:
                    query_template = {
                        "query": render_jinja(
                            path_base=sql_path_base, 
                            sql_path=f'bigquery/{event}.sql', 
                            sql_params={
                                "project_id": BQ_LAKE_PROJECT, 
                                "table_id": backfill_table
                            }
                        ),
                        "useLegacySql": False,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_APPEND",
                        "destinationTable": {
                            "project_id": BQ_DWH_PROJECT,
                            "dataset_id": "viator_appsflyer",
                            "table_id": FINAL_TABLE,
                        },
                        "timePartitioning": {
                            "type": "DAY",
                            "field": "partition_date",
                        },
                    }
            
                load_transform_dwh = BigQueryInsertJobOperator(
                    task_id=f"loading_to_dwh_{event_name}",
                    gcp_conn_id=BQ_GCP_CONN_ID,
                    project_id=BQ_LAKE_PROJECT,
                    configuration={
                        "query": query_template
                    },
                    dag=dag
                )
                
                update_config_date = BigQueryInsertJobOperator(
                    task_id=f"update_config_date_{event_name}",
                    gcp_conn_id=BQ_GCP_CONN_ID, 
                    configuration={
                        "query": {
                            "query": render_jinja(
                                path_base=sql_path_base, 
                                sql_path='bigquery/update_conf_table.sql', 
                                sql_params={
                                    'project_id': BQ_DWH_PROJECT,
                                    'ref_date': dt,
                                    'ref_datetime': ref_datetime,
                                    'event_name': event,
                                }
                            ),
                            "useLegacySql": False
                        }
                    },
                    location='US',
                    dag=dag
                )
        
                delete_table = BigQueryDeleteTableOperator(
                    task_id=f"delete_{backfill_table}_table",
                    deletion_dataset_table=f"{BQ_LAKE_PROJECT}.data_landing.{backfill_table}",
                    gcp_conn_id=BQ_GCP_CONN_ID,
                    location=BQ_OMNI_CONN_LOCATION,
                    dag=dag
                )
            
                load_transform_dwh >> [delete_table, update_config_date]
            
            athena_backfill >> bigquery_backfill 
    
    return aws_to_bigquery