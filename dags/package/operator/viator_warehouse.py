from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from yaml import safe_load
from airflow.operators.dummy_operator import DummyOperator
from package.utils.airflow_render_jinja  import render_jinja
from airflow.utils.trigger_rule import TriggerRule

def bigquery_dwh_load(dag: str, config_filename: str, sql_dir: str, job_name: str):


    dag_config = safe_load(open(config_filename))
    # Cloud environment connections
    gcp_conn = Variable.get('bq_data_warehouse_connection')
    project_id = Variable.get(dag_config['etl']['bq_project_id'])


    # Bigquery table informations
    table_name = dag_config['etl']['table_name']
    table_exists = dag_config['etl']['create_table_if_not_exist']
    jira_ticket = dag_config['etl']['jira']
    description = dag_config['etl']['description']
    sql_name = dag_config['etl']['sql']
    dataset_id = dag_config['etl']['bq_dataset_id']
    materialisation = dag_config['etl']['materialisation']
    table_schema = dag_config['etl']['table_schema']
    is_test = dag_config['etl']['is_test']
    partition_field= dag_config['etl']['partition_field']
    
    if 'view' in list(dag_config['etl'].keys()):
        # Bigquery view informations
        vw_project_id= Variable.get(dag_config['etl']['view']['bq_project_id'])
        vw_dataset_id=dag_config['etl']['view']['bq_dataset_id']
        vw_table_id=dag_config['etl']['view']['table_name']
        vw_materialisation=dag_config['etl']['view']['materialisation']
        vw_description=dag_config['etl']['view']['description']
        vw_table_exists=dag_config['etl']['view']['create_table_if_not_exist']
    
        query_params = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": f"{table_name}_test" if is_test else table_name,
            "materialisation": materialisation,
            "table_exists": table_exists,
            "partition_field": partition_field,
            "table_description": f'{jira_ticket}: {description}',
            "table_schema": table_schema,
            "labels": [("job_name", f"{job_name}"), ("task_name", f"execute_{table_name}_query")],
            "vw_project_id": vw_project_id,
            "vw_dataset_id": vw_dataset_id,
            "vw_table_id": vw_table_id,
            "vw_materialisation": vw_materialisation,
            "vw_description": vw_description,
            "vw_table_exists": vw_table_exists,
        }
    else:
        query_params = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": f"{table_name}_test" if is_test else table_name,
            "materialisation": materialisation,
            "table_exists": table_exists,
            "partition_field": partition_field,
            "table_description": f'{jira_ticket}: {description}',
            "table_schema": table_schema,
            "labels": [("job_name", f"{job_name}"), ("task_name", f"execute_{table_name}_query")]
        }

    execute_query = BigQueryInsertJobOperator(
        task_id=f"execute_{table_name}_query",
        gcp_conn_id=gcp_conn,
        project_id=project_id,
        configuration={
            "query": {
                "query": render_jinja(
                    path_base=sql_dir,
                    sql_path=f'{table_name}/{sql_name}',
                    sql_params=query_params
                ),
               "useLegacySql": False,
            },
        },
        dag=dag
    )

    return execute_query


def bigquery_extra_sql(dag: str, config_path: str, sql_dir: str):
    
    """
    Function to dynamic task mapping based on google spreadsheet passed on yaml dag config dependencies.
    
    Args:
        dag: dag name.
        config_path: string with path of yaml file.
        sql_dir: string with path of sql(s) file.

    Returns:
        Dynamic Task
    """
    
    # Cloud environment connections
    gcp_conn = Variable.get('bq_data_warehouse_connection')
    
    project_id = Variable.get('bq_data_warehouse_project')
    
    dag_config = safe_load(open(config_path))
    
    table_name = dag_config['etl']['table_name']
    jira_ticket = dag_config['etl']['jira']
    is_test = dag_config['etl']['is_test']
    
    description = dag_config['dependencies']['query']['description']
    
    #with TaskGroup(group_id=f"{table_name}_extra_sqls", dag=dag) as extra_sqls:
        
    extra_sqls = []
    
    for sql_name in dag_config['dependencies']['query']['sql']:
        
        table_id = sql_name.replace('.sql', '')
        
        sql = str(sql_name).replace('.sql', '').lower()
        
        extra_sqls.append(
            BigQueryInsertJobOperator(
                task_id=f"{sql}_extra_sql",
                gcp_conn_id=gcp_conn,
                project_id=project_id,
                configuration={
                    "query": {
                        "query": open(f'{sql_dir}{table_name}/{sql_name}', 'r').read(),
                        "useLegacySql": False,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "destinationTable": {
                            "project_id": project_id,
                            "dataset_id": 'landing',
                            "table_id": table_id,
                        },
                        "destinationTableProperties": {
                            'description': f'{jira_ticket}: {description}',
                            'labels': {
                                'Jira_ticket': jira_ticket,
                            }
                        }
                    }
                },
                dag=dag
            )
        )
    
    return extra_sqls[-1]


def bigquery_extra_gsheet(dag: str, config_path: str, sql_dir: str):
    """
    Function to dynamic task mapping based on google spreadsheet passed on yaml dag config dependencies.
    
    Args:
        dag: dag name.
        config_path: string with path of yaml file.
        sql_dir: string with path of sql(s) file.

    Returns:
        TaskGroup
    """
             
    gcp_conn = Variable.get('bq_data_warehouse_connection')
    
    dag_config = safe_load(open(config_path))
        
    # Cloud environment connections
    gcp_conn = Variable.get('bq_data_warehouse_connection')
    
    project_id = Variable.get('bq_data_warehouse_project')
        
    # Bigquery ETL Informations
    table_name = dag_config['etl']['table_name']
    jira_ticket = dag_config['etl']['jira']
    description = dag_config['etl']['description']

    gsheets = dag_config['dependencies']['gsheet']['sheets']    
    
    #with TaskGroup(group_id=f"{table_name}_load_gsheets", dag=dag) as final_table:
        
    execute_gheets = []
    
    for sheet in set(gsheets.keys()):
        
        execute_gheets.append(
            BigQueryInsertJobOperator(
                task_id=f"execute_external_{gsheets[sheet]['name']}",
                gcp_conn_id=gcp_conn,
                project_id=project_id,
                configuration={
                    "query": {
                        "query": render_jinja(
                            path_base=sql_dir, 
                            sql_path=f'{table_name}/external_table.sql', 
                            sql_params={
                                "project_id": project_id,
                                "dataset_id": 'landing',
                                "table_id": gsheets[sheet]['name'],
                                "sheet_uri": gsheets[sheet]['url']
                            }
                        ),
                        "useLegacySql": False,
                        "destinationTableProperties": {
                            'description': f'{jira_ticket}: {description}',
                            'labels': {
                                'Jira_ticket': jira_ticket,
                            }
                        },
                    }
                },
                location='US',
                dag=dag
            )
        )
    
    return execute_gheets[-1]


def extra_loads(dag: str, config_path: str, sql_dir: str):
    """
    Function to create a nested TaskGroup based on yaml dependencies passed.
    
    Args:
        dag: dag name.
        config_path: string with path of yaml file.
        sql_dir: string with path of sql(s) file.

    Returns:
        Nested TaskGroup
    """
    
    dag_config = safe_load(open(config_path))
    
    start_extras = DummyOperator(task_id='start_extras', dag=dag)
    
    # Bigquery ETL Informations
    with TaskGroup(group_id=f"dependencies_loads", dag=dag) as dependencies_loads:
        
        for dependency in set(dag_config['dependencies'].keys()):
                
            if dependency == 'query':
                
                with TaskGroup(group_id=f"query_dependency", dag=dag) as query_dependency:
                
                    bigquery_extra_sql(
                        dag=dag, 
                        config_path=config_path,
                        sql_dir=sql_dir
                    )
            else:
                
                with TaskGroup(group_id=f"gsheet_dependency", dag=dag) as gsheet_dependency:
                
                    bigquery_extra_gsheet(
                        dag=dag, 
                        config_path=config_path,
                        sql_dir=sql_dir
                    )
                    
    end_extras = DummyOperator(task_id='end_extras', dag=dag)   
    
    return start_extras >> dependencies_loads >> end_extras

