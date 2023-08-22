#
# Created by @erenansouza at 2023-02-22
#

#from airflow.decorators import task_group, task

from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryDeleteTableOperator
)
#from airflow.utils.trigger_rule import TriggerRule
from package.utils.airflow_render_jinja  import render_jinja
from yaml import safe_load
from airflow.utils.task_group import TaskGroup


def bigquery_transformation(dag: str, dag_type: str, sql_path_base: str, job_name: str, dag_config: str, ref_date: str, ref_datetime: str):
    
    dag_config = safe_load(open(dag_config))
    
    # Name of the event from yaml
    event_name = dag_config['dag_config']['event_name']
    
    event_table_name = dag_config['dag_config']['event_table_name']
    
    # Events setted up in yaml file which will be extracted from S3 and ingest into GCS, after that converted on BQ tables
    events = dag_config['events']
    
    # BigQuery info from yaml
    is_test = dag_config['dag_config']['is_test']
    
    ref_column = dag_config['dag_config']['ref_column']
    partition_date = dag_config['dag_config']['bq_partition_column']
    dwh_project_id = Variable.get(dag_config['dag_config']['bq_project_id'])
    lake_project_id = Variable.get(dag_config['dag_config']['bq_lake_project_id'])
    lake_dataset_id = dag_config['dag_config']['bq_lake_dataset_id']
    gcp_dwh_conn = Variable.get(dag_config['dag_config']['gcp_conn'])
    jira_ticket = dag_config['dag_config']['jira_ticket']
    description = dag_config['dag_config']['description']

    
    with TaskGroup(group_id=f"bigquery_{event_table_name}_transformation", dag=dag) as bigquery_transformation:
        
        start_transformation = DummyOperator(
            task_id=f'start_dwh_transformation',
            dag=dag
        )
        
        end_transformation = DummyOperator(
            task_id=f'end_dwh_transformation',
            dag=dag
        )
        
        insert_config_date = BigQueryInsertJobOperator(
            task_id=f"insert_config_date",
            gcp_conn_id=gcp_dwh_conn, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='insert_conf_table.sql', 
                        sql_params={
                            'project_id': dwh_project_id,
                            'ref_date': ref_date,
                            'ref_datetime': ref_datetime,
                            'event_name': event_table_name,
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag
        )
        
        # Define SQL name like transformation.sql into project's sql folder
        create_table = BigQueryInsertJobOperator(
            task_id=f"create_table",
            gcp_conn_id=gcp_dwh_conn,
            project_id=dwh_project_id,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path=f'create_table.sql', 
                        sql_params={
                            "event_list": events,
                            "project_id": dwh_project_id, 
                            "dataset_id": f"viator_{event_name}",
                            "final_table": f"{event_table_name}_test" if is_test else event_table_name,
                            "project_lake_id": lake_project_id,
                            "dataset_lake_id": lake_dataset_id,
                            "partition_field": partition_date,
                            "dag_description": f'{jira_ticket}: {description}',
                            "dag_labels": [("job_name", f"{job_name}"), ("task_name", f"loading_to_dwh_{event_name}")]
                        }
                    ),
                    "useLegacySql": False,
                },
            },
            location='US',
            dag=dag
        )

        # Define SQL name like transformation.sql into project's sql folder
        load_dwh = BigQueryInsertJobOperator(
            task_id=f"loading_to_dwh",
            gcp_conn_id=gcp_dwh_conn,
            project_id=dwh_project_id,
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path=f'merge_tables.sql', 
                        sql_params={
                            "event_list": events,
                            "dwh_project_id": dwh_project_id, 
                            "dwh_dataset_id": f"viator_{event_name}",
                            "final_table": f"{event_table_name}_test" if is_test else event_table_name,
                            "project_lake_id": lake_project_id,
                            "dataset_lake_id": lake_dataset_id,
                            "ref_column": ref_column,
                            "ref_date": ref_date,
                            "dag_type": dag_type,
                            "partition_field": partition_date
                        }
                    ),
                    "useLegacySql": False,
                },
            },
            location='US',
            dag=dag
        )
                
        update_config_date = BigQueryInsertJobOperator(
            task_id=f"update_config_date",
            gcp_conn_id=gcp_dwh_conn, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=sql_path_base, 
                        sql_path='update_conf_table.sql', 
                        sql_params={
                            'project_id': dwh_project_id,
                            'ref_date': ref_date,
                            'ref_datetime': ref_datetime,
                            'event_name': event_table_name,
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag
        )
        
        start_transformation >> insert_config_date >> create_table >> load_dwh >> update_config_date >> end_transformation
        
    return bigquery_transformation


def bigquery_backfill(dag: str, dag_type: str, sql_path_base:str, dag_config: str, ref_datetime: str):
        
    dag_config = safe_load(open(dag_config))
    # Name of the event from yaml
    event_name = dag_config['dag_config']['event_name']
    
    event_table_name = dag_config['dag_config']['event_table_name']
    
    # Events setted up in yaml file which will be extracted from S3 and ingest into GCS, after that converted on BQ tables
    events = dag_config['events']
    
    # BigQuery info from yaml
    is_test = dag_config['dag_config']['is_test']
    
    ref_column = dag_config['dag_config']['ref_column']
    dwh_project_id = Variable.get(dag_config['dag_config']['bq_project_id'])
    lake_project_id = Variable.get(dag_config['dag_config']['bq_lake_project_id'])
    lake_dataset_id = dag_config['dag_config']['bq_lake_dataset_id']
    gcp_dwh_conn = Variable.get(dag_config['dag_config']['gcp_conn'])
    
    sql = render_jinja(
        path_base=sql_path_base, 
        sql_path='check_backfill.sql', 
        sql_params={
            'project_id': dwh_project_id,
            'event_type': event_table_name
        }
    )
        
    bq = BigQueryHook(gcp_conn_id=gcp_dwh_conn, use_legacy_sql=False,)
    conn = bq.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    dates= [str(item[0]) for item in cursor.fetchall()]
    
    with TaskGroup(group_id=f"{event_table_name}_backfill", dag=dag) as bigquery_backfill:
        
        for dt in dates:
            
            load_dwh = BigQueryInsertJobOperator(
                task_id=f"loading_to_dwh",
                gcp_conn_id=gcp_dwh_conn,
                project_id=dwh_project_id,
                configuration={
                    "query": {
                        "query": render_jinja(
                            path_base=sql_path_base, 
                            sql_path=f'merge_tables.sql', 
                            sql_params={
                                "event_list": events,
                                "dwh_project_id": dwh_project_id, 
                                "dwh_dataset_id": f"viator_{event_name}",
                                "final_table": f"{event_table_name}_test" if is_test else event_table_name,
                                "project_lake_id": lake_project_id,
                                "dataset_lake_id": lake_dataset_id,
                                "ref_column": ref_column,
                                "ref_date": dt,
                                "dag_type": dag_type
                            }
                        ),
                        "useLegacySql": False,
                    },
                },
                location='US',
                dag=dag
            )
                    
            update_config_date = BigQueryInsertJobOperator(
                task_id=f"update_config_date",
                gcp_conn_id=gcp_dwh_conn, 
                configuration={
                    "query": {
                        "query": render_jinja(
                            path_base=sql_path_base, 
                            sql_path='update_conf_table.sql', 
                            sql_params={
                                'project_id': dwh_project_id,
                                'ref_date': dt,
                                'ref_datetime': ref_datetime,
                                'event_name': event_table_name,
                            }
                        ),
                        "useLegacySql": False
                    }
                },
                location='US',
                dag=dag
            )
            
            load_dwh >> update_config_date


#-------------------------------Awin DAG's dependecies--------------------------------------------------------- 
class Routes:
    ADVERTISERS = {"url": "https://api.awin.com/accounts?type=advertiser", "type": "advertisers"}
    CREATIVE = {"url": "https://api.awin.com/advertisers/{advertiser_id}/reports/creative", "type": "creative",
                "table_name": "awin_creative_report"}
    CAMPAIGN = {"url": "https://api.awin.com/advertisers/{advertiser_id}/reports/campaign", "type": "campaign",
                "table_name": "awin_campaign_report"}
        
def awin_etl(dag_name, path_sql, report, account_list, ref_date):

    import pendulum
    from datetime import datetime
    from package.operator.awin import download_report_data

    CURRENT_DATETIME = datetime.now(pendulum.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
    
    BQ_CONN_ID = Variable.get("bq_data_lake_connection")
    DWH_PROJECT = Variable.get("bq_data_warehouse_project") 
    LAKE_PROJECT = Variable.get('bq_data_lake_project')
    EVENT_NAME = f"awin_{report}_report"   

    with TaskGroup(group_id=f'{report}_reports_lake_to_dwh', dag=dag_name) as event_report:
        
        downloaded = task(download_report_data, task_id=f"{report}_download_data")(
                accounts=account_list,
                report=report,
                ref_date=ref_date
            )
        
        check_table = BigQueryInsertJobOperator(
            task_id="check_if_dwh_table_exists",
            gcp_conn_id=BQ_CONN_ID,
            configuration={
                "query": {
                    "query": open(f"{path_sql}/create_{report}_report.sql", 'r').read()
                    .replace('{project}', DWH_PROJECT)
                    .replace('{dataset}', 'viator_awin')
                    .replace('{table_name}', EVENT_NAME),
                    "useLegacySql": False
                },
                "labels": {
                    "task_id": "check_if_dwh_table_exists"
                },
            },
            location='US',
            dag=dag_name
        )
        
        check_count = BigQueryCheckOperator(
            task_id="check_lake_count",
            gcp_conn_id=BQ_CONN_ID,
            sql=f"SELECT COUNT(*) FROM {LAKE_PROJECT}.data_landing.{EVENT_NAME};",
            use_legacy_sql=False,
            location='US',
            dag=dag_name
        )
        
        insert_config_date = BigQueryInsertJobOperator(
            task_id=f"insert_config_date",
            gcp_conn_id=BQ_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=path_sql, 
                        sql_path='insert_conf_table.sql', 
                        sql_params={
                            'project_id': DWH_PROJECT,
                            'ref_date': ref_date,
                            'ref_datetime': CURRENT_DATETIME,
                            'event_name': EVENT_NAME,
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag_name
        )
        
        inserting_to_dwh = BigQueryInsertJobOperator(
            task_id=f"inserting_to_dwh",
            gcp_conn_id=BQ_CONN_ID,
            configuration={
                "query": {
                    "query": open(f"{path_sql}/move_{report}_to_dwh.sql", 'r').read()
                    .replace('{project_dl}', LAKE_PROJECT)
                    .replace('{dataset_dl}', "data_landing")
                    .replace('{table_name_dl}', EVENT_NAME)
                    .replace('{project_dwh}', DWH_PROJECT)
                    .replace('{dataset_dwh}', "viator_awin")
                    .replace('{table_name_dwh}', EVENT_NAME)
                    .replace('{start_date}', str(ref_date)),
                    "useLegacySql": False
                },
                "timePartitioning": {
                    "type": 'DAY',
                    "field": 'report_date',
                }
            },
            location='US',
            dag=dag_name
        )
        
        update_config_date = BigQueryInsertJobOperator(
            task_id=f"update_config_date",
            gcp_conn_id=BQ_CONN_ID, 
            configuration={
                "query": {
                    "query": render_jinja(
                        path_base=path_sql, 
                        sql_path='update_conf_table.sql', 
                        sql_params={
                            'project_id': DWH_PROJECT,
                            'ref_date': ref_date,
                            'ref_datetime': CURRENT_DATETIME,
                            'event_name': EVENT_NAME,
                            'qtd_rows': downloaded
                        }
                    ),
                    "useLegacySql": False
                }
            },
            location='US',
            dag=dag_name
        )
        
        end = DummyOperator(task_id=f"finished_{report}_processing", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag_name)
        
        downloaded >> [check_table, insert_config_date] >> check_count >> inserting_to_dwh >> update_config_date >> end
        
    return event_report


def awin_backfill(dag_name, path_sql, report, account_list):

    import pendulum
    from datetime import datetime, timedelta
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from package.utils.airflow_render_jinja  import render_jinja
    from package.operator.awin import download_report_data

    CURRENT_DATETIME = datetime.now(pendulum.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
    
    BQ_CONN_ID = Variable.get("bq_data_lake_connection")
    DWH_PROJECT = Variable.get("bq_data_warehouse_project") 
    LAKE_PROJECT = Variable.get('bq_data_lake_project')
    EVENT_NAME = f"awin_{report}_report"
    
    #Get delta from Bigquery config_table
    sql = render_jinja(
        path_base=path_sql, 
        sql_path='backfill_check.sql', 
        sql_params={
            'dwh_project': DWH_PROJECT,
            'event_type': f"awin_{report}_report"
        }
    )
        
    bq = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False, location='US')
    #dates = bq.get_records(sql)
    #date_range = [str(dt[0]) for dt in dates]
    
    conn = bq.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    date_range= [str(item[0]) for item in cursor.fetchall()]
             
    with TaskGroup(group_id=f'{report}_report_backfill', dag=dag_name) as event_report:
        
        for ref_date in date_range:
            
            downloaded = task(download_report_data, task_id=f"{report}_download_backfill")(
                accounts=account_list,
                report=report,
                ref_date=ref_date,
                is_backfill=True
            )
            
            lake_table_id = f"awin_{report}_report_backfill"
            
            check_table = BigQueryInsertJobOperator(
                task_id="check_if_dwh_table_exists",
                gcp_conn_id=BQ_CONN_ID,
                configuration={
                    "query": {
                        "query": open(f"{path_sql}/create_{report}_report.sql", 'r').read()
                        .replace('{project}', DWH_PROJECT)
                        .replace('{dataset}', 'viator_awin')
                        .replace('{table_name}', f"awin_{report}_report"),
                        "useLegacySql": False
                    }
                },
                location='US',
                dag=dag_name
            )
            
            check_count = BigQueryCheckOperator(
                task_id="check_lake_count",
                gcp_conn_id=BQ_CONN_ID,
                sql=f"""SELECT COUNT(*) FROM {LAKE_PROJECT}.data_landing.{lake_table_id};""",
                use_legacy_sql=False,
                location='US',
                dag=dag_name
            )
            
            inserting_to_dwh = BigQueryInsertJobOperator(
                task_id=f"inserting_to_dwh",
                gcp_conn_id=BQ_CONN_ID,
                configuration={
                    "query": {
                        "query": open(f"{path_sql}/move_{report}_to_dwh.sql", 'r').read()
                        .replace('{project_dl}',LAKE_PROJECT)
                        .replace('{dataset_dl}', "data_landing")
                        .replace('{table_name_dl}', lake_table_id)
                        .replace('{project_dwh}', DWH_PROJECT)
                        .replace('{dataset_dwh}', "viator_awin")
                        .replace('{table_name_dwh}', EVENT_NAME)
                        .replace('{start_date}', str(ref_date)),
                        "useLegacySql": False
                    },
                    "timePartitioning": {
                        "type": 'DAY',
                        "field": 'report_date',
                    }
                },
                location='US',
                dag=dag_name
            )
            
            update_config_date = BigQueryInsertJobOperator(
                task_id=f"update_config_date",
                gcp_conn_id=BQ_CONN_ID, 
                configuration={
                    "query": {
                        "query": render_jinja(
                            path_base=path_sql, 
                            sql_path='update_conf_table.sql', 
                            sql_params={
                                'project_id': DWH_PROJECT,
                                'ref_date': ref_date,
                                'ref_datetime': CURRENT_DATETIME,
                                'event_name': EVENT_NAME,
                                'qtd_rows': downloaded
                            }
                        ),
                        "useLegacySql": False
                    }
                },
                location='US',
                dag=dag_name
            )
            
            end = DummyOperator(task_id=f"finished_{report}_processing", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag_name)
            
            #insert_config_date >> check_data >> missing_data >> end
            #insert_config_date >> check_data >> check_table >> inserting_to_dwh >> update_config_date >> end
            
            downloaded >>[check_table, update_config_date] >> check_count >> inserting_to_dwh >> end


#-------------------------------Awin DAG's dependecies--------------------------------------------------------- 
#TODO: add to plugins
def write_labels(dag,task_id,destination_table):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
    # Create a BigQueryHook to interact with BigQuery
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    hook.get_client()
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = f"""
    ALTER TABLE {destination_table}
    SET OPTIONS (
    labels = [('dag_id', '{dag.dag_id}'), ('task_id', '{task_id}'), ('schedule_interval', '{dag.schedule_interval}')]);
    """
    cursor.execute(sql)
    cursor.close()