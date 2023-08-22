
def viator_warehouse_factory(dag: str, config_path: str, sql_dir: str, job_name: str):
    
    """
    This transformation function was created focused on Ocean Share necessity, so I 
    putted it inside project factory in case of refactor.

    Args:
        dag: Airflow DAG
        dag_config_path: yaml project path with contains DAG parameters
        sql_dir: sql queries project path
        job_name: Dag name

    Returns:
        Airflows Task Group
    """
    from package.operator.viator_warehouse import (
        bigquery_dwh_load,
        extra_loads
    )
    
    import glob
    from yaml import safe_load
    from airflow.utils.task_group import TaskGroup
    from re import findall
    from airflow.operators.dummy_operator import DummyOperator
    
    extras_tasks = []
    load_with_dependencies = []
    load_wo_dependencies = []

    for filename in glob.glob(f"{config_path}*.yml"):

        config_filename = findall('[ \w-]+?(?=\.)', filename)[0]
        
        dag_config = safe_load(open(filename))
                            
        if set(dag_config.keys()).__contains__('dependencies'):
            
            with TaskGroup(group_id=f"load_{config_filename}", dag=dag) as load_with_extras:
                
                end = DummyOperator(task_id=f'end_{config_filename}_tasks', dag=dag)
            
                extras_tasks.append(
                    extra_loads(
                        dag=dag, 
                        config_path=filename, 
                        sql_dir=sql_dir,
                    )
                )
                
                load_with_dependencies.append(
                    bigquery_dwh_load(
                        dag=dag, 
                        config_filename=filename,
                        sql_dir=sql_dir,
                        job_name=job_name
                    )
                )
            
                extras_tasks[-1] >> load_with_dependencies[-1] >> end
            
        else:
            
            with TaskGroup(group_id=f"load_{config_filename}", dag=dag) as load_wo_extras:
                
                start = DummyOperator(task_id=f'start_{config_filename}_tasks', dag=dag)
                end = DummyOperator(task_id=f'end_{config_filename}_tasks', dag=dag)
        
                load_wo_dependencies.append(
                    bigquery_dwh_load(
                        dag=dag, 
                        config_filename=filename,
                        sql_dir=sql_dir,
                        job_name=job_name
                    )
                )
                
                start >> load_wo_dependencies[-1] >> end

        