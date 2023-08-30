import logging
import os
from typing import Optional
from yaml import safe_load, dump
import glob
from typing import List
from datetime import timedelta

from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryUpsertTableOperator
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
#from airflow.providers.tableau.operators.tableau import TableauOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import get_current_context
from package.utils.airflow_render_jinja  import render_jinja


def add_views(dag, copy_yaml_path, start_task, end_task):
    
    SQL_PATH= f"{os.path.abspath(os.path.dirname(__file__))}/sql/"
    BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
    
    y = safe_load(open(copy_yaml_path, "r"))
        
    with TaskGroup(group_id=f'view_and_backup_job', dag=dag) as creation_job:
                
        for key, item in y.items():
            new_table_description = item.get("new_table_description")
            backup_description = item.get("backup_description")
            view_description = item.get("view_description")
            source = item.get("source")
            destination = item.get("destination")
            columns = item.get("columns")
            partition_field = item.get("partition_field")
            
            bq_table = destination.split(".")[-1]

            with TaskGroup(group_id=f'{bq_table}_view_creation', dag=dag) as view_creation:
                
                    copy_legacy_to_dwh = BigQueryInsertJobOperator(
                        task_id=f"copy_legacy_table",
                        gcp_conn_id=BQ_CONN_ID,
                        configuration={
                            "query": {
                                "query": render_jinja(
                                    path_base=SQL_PATH, 
                                    sql_path=f'create_table.sql', 
                                    sql_params={
                                        "source": source,
                                        "destination": destination, 
                                        "description": new_table_description,
                                        "dag_name": "{{dag.dag_id}}",
                                        "task_name": "{{task.task_id}}",
                                        "is_backup": False,                          
                                    }
                                ),
                                "useLegacySql": False,
                            }
                        },
                        location='US',
                        dag=dag
                    )
                    
                    bck_legacy_in_dwh = BigQueryInsertJobOperator(
                        task_id=f"bck_legacy_copy",
                        gcp_conn_id=BQ_CONN_ID,
                        configuration={
                            "query": {
                                "query": render_jinja(
                                    path_base=SQL_PATH, 
                                    sql_path=f'create_table.sql', 
                                    sql_params={
                                        "source": source,
                                        "destination": destination, 
                                        "description": backup_description,
                                        "dag_name": "{{dag.dag_id}}",
                                        "task_name": "{{task.task_id}}",
                                        "ref_date": f"{{{{ ds }}}}"                        
                                    }
                                ),
                                "useLegacySql": False,
                            }
                        },
                        location='US',
                        dag=dag
                    )
                    
                    bck_source = BigQueryInsertJobOperator(
                        task_id=f"bck_source_table",
                        gcp_conn_id=BQ_CONN_ID,
                        configuration={
                            "query": {
                                "query": render_jinja(
                                    path_base=SQL_PATH, 
                                    sql_path=f'create_table.sql', 
                                    sql_params={
                                        "source": source,
                                        "destination": source, 
                                        "description": backup_description,
                                        "dag_name": "{{dag.dag_id}}",
                                        "task_name": "{{task.task_id}}",
                                        "is_backup": True,
                                        "ref_date": f"{{{{ ds }}}}"                          
                                    }
                                ),
                                "useLegacySql": False,
                            }
                        },
                        location='US',
                        dag=dag
                    )
                    
                    create_view_table = BigQueryInsertJobOperator(
                        task_id=f"create_view_table",
                        gcp_conn_id=BQ_CONN_ID,
                        configuration={
                            "query": {
                                "query": render_jinja(
                                    path_base=SQL_PATH, 
                                    sql_path=f'create_view.sql', 
                                    sql_params={
                                        "source": source,
                                        "destination": destination, 
                                        "description": view_description,
                                        "dag_name": "{{dag.dag_id}}",
                                        "columns": columns,
                                        "partition_field": partition_field                
                                    }
                                ),
                                "useLegacySql": False,
                            }
                        },
                        location='US',
                        dag=dag
                    )
                    
                    [copy_legacy_to_dwh, bck_legacy_in_dwh, bck_source] >> create_view_table
    
    start_task >> creation_job >> end_task


def add_bq_extracts(dir_path, dag, start_task, end_task, ds_context_key="tomorrow_ds", on_failure_callback=None):
    """This Operator is a refactored version of on-premise's add_bq_extracts

    Args:
        dir_path: Path that contains the yaml files and sql files.
        dag: DAG's name.
        start_task: Task's name that will run before this operator.
        end_task: End Tasks.
        ds_context_key: Defaults to "tomorrow_ds".
        on_failure_callback: Defaults to None.

    Returns:
        A unique or group of bigquery_legacy_transformation tasks in parallel.
    """
    from re import search
    
    for yml_file in glob.glob(f'{dir_path}/*.yml'):
        
        y = safe_load(open(yml_file, "r"))
        
        task_name = search(r'/([^/]+)\.yml$', yml_file)
        
        with TaskGroup(group_id=f'{task_name.group(1)}_yml_extraction', dag=dag) as yml_extraction:
            prev = None
            for i in range(0, len(y)):
                e = y[f"extract{str(i)}"]
                
                sql_file_name = f"{dir_path }/{e['sqlFile'].strip()}"
                destination = e["destination"]
                table_description = e["description"] if "description" in e else None
                jira = e["jira"] if "jira" in e else None

                if jira is not None:
                    if table_description is None:
                        table_description = "Jira: " + jira
                    else:
                        table_description += "\nJira: " + jira

                arguments = e.get("args")
                if arguments is None:
                    arguments = dict()
                elif type(arguments) is str:
                    arg_dict = dict()
                    arg_list = arguments.split(",")
                    for arg in arg_list:
                        if len(arg.split("=")) > 1:
                            key = arg.split("=")[0].strip()
                            value = arg.split("=")[1].strip()
                            arg_dict[key] = value
                    arguments = arg_dict

                # Set the task_id to either the table name, or sql file
                # also set the time_partitioning field if we are writing to a partition
                time_partitioning = None
                if destination is not None:
                    task_id = destination.replace(":", "-").replace("$", "_")
                    if "$" in destination:
                        time_partitioning = {"type": "DAY"}
                else:
                    task_id = e.get("sqlFile")

                task = bigquery_legacy_transformation(
                    dag=dag,
                    pool='bq_data_extract',
                    bq_table_id=destination, #Must be fitted replacing : for .
                    write_disposition="WRITE_TRUNCATE",
                    table_description=table_description,
                    ds_context_key=ds_context_key,
                    sql_path=sql_file_name,
                    time_partitioning=time_partitioning,
                    arguments=arguments
                )
                if prev:
                    prev << task
                prev = task

                # AVRWH - 4158
                if on_failure_callback is not None:
                    task.on_failure_callback = on_failure_callback
                    
            #if end_task.task_id not in list(prev.downstream_task_ids):
            #    prev >> end_task

        start_task >> yml_extraction >> end_task

  
def bigquery_legacy_transformation(dag,
                                   pool, 
                                   bq_table_id, 
                                   write_disposition,
                                   table_description, 
                                   ds_context_key, 
                                   sql_path, 
                                   time_partitioning: Optional[dict]=None,
                                   arguments: Optional[str]=None):
    
    """
    Refactore of [from plugins.rex_bq_operators import REXBQExtractOperator], which is an on-premise Airflow Operator.

    Args:
        dag: string
        bq_table_id: string
        write_disposition: string
        table_description: string
        ds_context_key: string
        sql_path: string
        time_partitioning: dictionary
        arguments: String. Defaults to None.

    Returns:
        Airflow TaskGroup containing.
    """
    
    BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
    
    ed = "ts"
    if ds_context_key == "ds":
        yesterday_ds = f"{{{{ yesterday_ds }}}}"
        yesterday_ds_nodash = f"{{{{ yesterday_ds_nodash }}}}"
        ds = f"{{{{ ds }}}}"
        ds_nodash = f"{{{{ ds_nodash }}}}"
        # this is added as part of AVRWH-4806 to accomodate needing timestamp
        ed = f"{{{{ ts }}}}"
    elif ds_context_key == "tomorrow_ds":
        yesterday_ds = f"{{{{ ds }}}}"
        yesterday_ds_nodash = f"{{{{ ds_nodash }}}}"
        ds = f"{{{{ tomorrow_ds }}}}"
        ds_nodash = f"{{{{ tomorrow_ds_nodash }}}}"
        ed = f"{{{{ ts }}}}"
    elif ds_context_key == "yesterday_ds":
        # NOTE yesterday_ds should not come into use when passing in yesterday_ds as a context key
        yesterday_ds = f"{{{{ yesterday_ds }}}}"
        yesterday_ds_nodash = f"{{{{ yesterday_ds_nodash }}}}"
        ds = f"{{{{ yesterday_ds }}}}"
        ds_nodash = f"{{{{ yesterday_ds_nodash }}}}"
        ed = f"{{{{ ts }}}}"

    # Splitting informations between project, dataset, table
    split_name = bq_table_id.split(".")
    project_dataset_name = split_name[0].split(':')
    bq_project = project_dataset_name[0]
    bq_dataset = project_dataset_name[1]
    bq_table = split_name[1].replace("$yesterday", "").replace(
    "$today", ""
    )
        
    if split_name[1].__contains__( '$yesterday'):
        partition_table =split_name[1].replace('$yesterday', f'${yesterday_ds_nodash}')
    else:
        partition_table = split_name[1].replace("$today", f'${ds_nodash}')
    
    # Open the sql file and adjust the point inside it    
    run_sql = open(sql_path, "r").read()
    
    if arguments:
        for key, value in arguments.items():
            run_sql = run_sql.replace("<" + key + ">", value)

    run_sql = run_sql.replace("<yesterday>", yesterday_ds)
    run_sql = run_sql.replace("<today>", ds)
    run_sql = run_sql.replace("<day>", ds)
    run_sql = run_sql.replace("<execution_date>", ed)
    
    if time_partitioning:
        query = {
            "query": run_sql,
            "useLegacySql": False,
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": write_disposition,
            "priority": "INTERACTIVE",
            "destinationTable": {
                "projectId": bq_project,
                "datasetId": bq_dataset,
                "tableId": partition_table
            },
            "timePartitioning": time_partitioning,
        }
    else:
        query = {
            "query": run_sql,
            "useLegacySql": False,
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": write_disposition,
            "priority": "INTERACTIVE",
            "destinationTable": {
                "projectId": bq_project,
                "datasetId": bq_dataset,
                "tableId": bq_table
            },
        }

    with TaskGroup(group_id=f'{bq_table}_transformation', dag=dag) as bigquery_transformation:

        run_query = BigQueryInsertJobOperator(
            task_id=f"run_query",
            gcp_conn_id=BQ_CONN_ID, 
            configuration={
                "query": query,
            },
            project_id=bq_project,
            pool=pool,
            dag=dag
        )
        
        upsert_table_description = BigQueryUpsertTableOperator(
            task_id="upsert_table",
            gcp_conn_id=BQ_CONN_ID,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_resource={
                "tableReference": {"tableId": bq_table},
                "description": table_description,
                "labels": {
                    "job_name": dag.dag_id.lower(),
                    "task_name": "upsert_table"
                },
            },
            dag=dag
        )
        
        run_query >> upsert_table_description
    
    return bigquery_transformation


def bq_legacy_validations(
    dag, 
    task_group_id,
    last_modified_tables: Optional[list] = [], 
    expected_result_queries: Optional[dict] = {},
    partition_tables: Optional[list] = [],
    queries_to_count: Optional[list] = [],
    poke_interval=60 * 15,  # Default 15 minutes between pokes
    timeout=60 * 60 * 2,  # Default 2 hours before giving up
    mode="reschedule",
    retries=0,
    retry_delay: Optional[float] = timedelta(hours=5),
    ) -> TaskGroup:
    
    """
        Refactore of [from plugins.bq_table_ready_sensor import BQTableReadySensor], which is an on-premise Airflow Operator.

    Args:
        dag: string, 
        task_group_id: string.
        last_modified_tables: List of tables to check passed as strings. Default is []. 
        expected_result_queries: Dictionary of query as dictionary key and an expected value as dictionary value, being both strings. Default is {},
        partition_tables: List of tables to check passed as strings. Default is [].
        poke_interval: poke. Default is 60 * 15 (15 minutes between pokes).
        timeout: Taks's timeout. Difault is 60 * 60 * 2 (2 hours before giving up).
        mode: Task's modes. Default is "reschedule".
        retries: number of retreis. Default is 0.
        retry_delay: This parameter specifies the time delay between retries as a timedelta object. This delay is the period that Airflow will wait after a task fails before it tries to execute it again.
        
    Returns:
        Airflow TaskGroup
    """    
    BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
    
    def check_last_modified_tables():

        if last_modified_tables and len(last_modified_tables) > 0:
            
            with TaskGroup(group_id='check_last_modified_tables', dag=dag) as check_tables:
                
                for table in last_modified_tables:
                    table_parts = table.split(".")
                    bq_project = table_parts[0]
                    bq_dataset = table_parts[1]
                    bq_table = table_parts[2]
                    
                    check_table_modification = BigQueryCheckOperator(
                        task_id=f'check_{bq_dataset}-{bq_table}_last_modification',
                        use_legacy_sql=False,
                        gcp_conn_id=BQ_CONN_ID,
                        sql=f"""
                        SELECT 
                            COUNT(1) 
                        FROM `{bq_project}.{bq_dataset}.__TABLES__` 
                        WHERE table_id = '{bq_table}' 
                        AND TIMESTAMP_MILLIS(last_modified_time) > TIMESTAMP(concat(cast(current_date('UTC') AS string), ' 00:01:00 UTC'));
                        """,
                        location='US',
                        retries=retries,
                        retry_delay=retry_delay,
                        dag=dag
                    )

            return check_tables
    
    def check_expected_result_queries():
        
        from re import findall

        if expected_result_queries and len(expected_result_queries) > 0:
            
            ds = f"{{{{ ds }}}}"
            tomorrow_ds = f"{{{{ tomorrow_ds }}}}"
            # added to support ability to extract time as well as date for execution run
            #execution_date = f"{{{{ execution_date }}}}".to_datetime_string()
            execution_date = f"{{{{ execution_date }}}}"
            
            with TaskGroup(group_id='check_expected_result_queries', dag=dag) as check_results:
                
                count = 0
                
                for query, expected_result in expected_result_queries.items():
                    query = query.replace("<yesterday>", ds)
                    query = query.replace("<today>", tomorrow_ds)
                    query = query.replace("<execution_date>", execution_date)

                    table_name =  findall(r"`([^`]+)`", query)
                    prev_table = table_name[0].split(".")[-1]
                    
                    if prev_table == table_name[0].split(".")[-1]:
                        table = f"{table_name[0].split('.')[-1]}_{count}"
                        count+=1
                    else:
                        table=prev_table
                    
                    check_query_value_result = BigQueryValueCheckOperator(
                        task_id=f'check_{table}_query_result',
                        use_legacy_sql=False,
                        gcp_conn_id=BQ_CONN_ID,
                        sql=query,
                        pass_value=expected_result,
                        retry_delay=retry_delay,
                        retries=retries,
                        dag=dag,

                    )


            return check_results
    
    def check_queries_count():
        
        from re import findall

        if queries_to_count and len(queries_to_count) > 0:
            
            ds = f"{{{{ ds }}}}"
            tomorrow_ds = f"{{{{ tomorrow_ds }}}}"
            # added to support ability to extract time as well as date for execution run
            #execution_date = f"{{{{ execution_date }}}}".to_datetime_string()
            execution_date = f"{{{{ execution_date }}}}"
            
            with TaskGroup(group_id='check_queries_count', dag=dag) as check_count:
                
                for query in queries_to_count:
                    query = query.replace("<yesterday>", ds)
                    query = query.replace("<today>", tomorrow_ds)
                    query = query.replace("<execution_date>", execution_date)

                    table_name =  findall(r"`([^`]+)`", query)
                    table_name = table_name[0].split(".")[-1]
                    
                    check_query_count = BigQueryCheckOperator(
                        task_id=f'check_{table_name}_query_count',
                        gcp_conn_id=BQ_CONN_ID,
                        sql=query,
                        use_legacy_sql=False,
                        retry_delay=retry_delay,
                        retries=retries,
                        dag=dag,

                    )

            return check_count

    def check_partition_tables():
        
        if partition_tables and len(partition_tables) > 0:
            
            with TaskGroup(group_id='check_tables_partition', dag=dag) as check_partitions:
                
                for table in partition_tables:
                    split_name = table.split(".")
                    bq_project = split_name[0]
                    bq_dataset = split_name[1]
                    bq_table = split_name[2]
                    
                    check_partition: BaseSensorOperator = BigQueryTablePartitionExistenceSensor(
                        task_id=f"check_{bq_dataset}-{bq_table}_partition_existence",
                        gcp_conn_id=BQ_CONN_ID,
                        project_id=bq_project,
                        dataset_id=bq_dataset,
                        table_id=bq_table,
                        partition_id=f"{{{{ ds_nodash }}}}",
                        poke_interval=poke_interval,
                        timeout=timeout,
                        mode=mode,
                        dag=dag,
                        retries=retries
                    )
                    
            return check_partitions
        
    with TaskGroup(group_id=task_group_id, dag=dag) as tables_analysis:
        
        start = EmptyOperator(task_id=f'start_validations')
        end = EmptyOperator(task_id=f'end_validations')

        with TaskGroup(group_id='validations', dag=dag) as validations:
            
            verifications = []
            if last_modified_tables and len(last_modified_tables) > 0:
                verifications.append(check_last_modified_tables())
                
            if expected_result_queries and len(expected_result_queries) > 0:
                verifications.append(check_expected_result_queries())
                
            if partition_tables and len(partition_tables) > 0:
                verifications.append(check_partition_tables())
            
            if queries_to_count and len(queries_to_count) > 0:
                verifications.append(check_queries_count())
            
            verifications[-1]
    
        start >> validations >> end
    
    return tables_analysis 


def create_yaml_file(report_name, arg_count, dir_path):
    
    def generate_dict(partner, emails, status, arg_array, report_name, threshold, account_manager):
        
        DEFAULT_ACCOUNT_MANAGER = "3PAccountMgt@tripadvisor.com"
        
        query = """
            SELECT RecipientEmailAddress, SubscriberKey
            FROM `CRM.PartnerReportEmailSubscriberKeyMapping`
            where RecipientEmailAddress in ({})
        """
        # Template file contains information shared across all partners for the same report
        # Each report should have its own template file
        folder_dir = os.path.dirname(os.path.realpath(__file__))
        
        template_name = f"{folder_dir}/{report_name.lower().replace(' ', '_')}_template.yml"
        # open template file
        try:
            with open(template_name, "r") as f:
                template = safe_load(f)
        except Exception as e:
            logging.error("Failed reading file: %s ", e)

        template_id = template['templateId']
        business_domain = template['businessDomain']
        sheets = template['sheets']
        args = template['args']

        # fill in the template arguments with correct value
        for arg_key, arg_value in args.items():
            for i in range(len(arg_array)):
                arg_str = "<arg{}>".format(i + 1)
                args[arg_key] = arg_value.replace(arg_str, arg_array[i])

        partner_name = partner.replace(",", "").replace(" ", "")
        # Treat Loving new york special because it needs multiple reports for different puids
        if partner.startswith("Loving New York"):
            puid = arg_array[0]
            filename = report_name.replace(" ", "_") + "_" + puid
            partner_name = partner_name + puid
        else:
            filename = report_name.replace(" ", "_")

        # Get subscriber key for recipients
        recipients = dict()
        recipients_list = map(str.strip, emails.split(";"))
        query = query.format("\"" + "\",\"".join(recipients_list) + "\"")
        bq = BigQueryHook(BQ_CONN_ID)
        conn = bq.get_conn()
        cursor = conn.cursor().execute(query)
        row = cursor.fetchone()
        while row is not None:
            email = row[0]
            subscriber_key = row[1]
            recipients[email] = subscriber_key
            row = cursor.fetchone()

        # Write to dictionary
        output = dict()
        output["status"] = status
        output["reportName"] = report_name
        output["templateId"] = template_id
        output["businessDomain"] = business_domain
        output["recipients"] = recipients
        upload_bucket = "gs://partner-reports-" + partner
        upload_bucket = upload_bucket.lower().replace(".com", "").replace("google", "").replace("()", "").replace(",", "").strip().replace(" ", "-").replace(".", "-")
        output["uploadBucket"] = upload_bucket
        output["sheets"] = dict()
        for key, value in sheets.items():
            output["sheets"][key] = value
        output["args"] = args
        output["threshold"] = threshold
        output["filename"] = filename
        output["accountManager"] = account_manager or DEFAULT_ACCOUNT_MANAGER
        return partner_name, output
    
    from re import sub
    BQ_CONN_ID = Variable.get("bq_data_warehouse_connection")
    
    table_name = report_name.replace(" ", "") + "Partners"
    logging.info("TABLE_NAME: " + table_name)
    # Get most recent partition, in case the google sheet import failed for one day, the emails can still go out
    query = f"""
    SELECT Partner, Email, Status, PUID, Threshold, AccountManager
    FROM `ATTRDimensions.{table_name}`
    WHERE _PARTITIONDATE IN (SELECT MAX(_PARTITIONDATE) FROM `ATTRDimensions.{table_name}`)
    """
    bq = BigQueryHook(BQ_CONN_ID)
    conn = bq.get_conn()
    cursor = conn.cursor()
    
    # Loop through each row in query result and write data to a dictionary.
    output = {}
    row = cursor.execute(query).fetchone()
    
    while row is not None:
        logging.info("row: " + str(row))
        partner = sub('[^A-Za-z0-9]+', '', row[0])
        emails = row[1].strip()
        status = row[2]
        threshold = row[4]
        account_manager = row[5]
        arg_array = [arg_count]
        for i in range(arg_count):
            arg_array[i] = row[i + 3]
        key, value = generate_dict(partner, emails, status, arg_array, report_name, threshold, account_manager)
        output[key] = value
        row = cursor.execute(query).fetchone()
        
    out_file_name = f"{dir_path}/{report_name.lower().replace(' ', '_')}.yml"
    
    logging.info("Output file name: " + out_file_name)
    
    with open(out_file_name, 'w+') as out_file:
        dump(output, out_file, default_flow_style=False)
    
    
#--------------start------------Migration Quality Check--------------start-------------------------------------- 

def quality_check_v2(legacy_table: str,
                     migrated_table: str,
                     dag: str,
                     BQ_CONN_ID: str,
                     legacy_ts_column: str,
                     migrated_ts_column: str,
                     upper_boundary_days:int,
                     lower_boundary_days:int,
                     delete_past: bool
                     ):
    """
     Perform quality checks for data migration between a legacy table and a migrated table.

     Args:
         legacy_table (str): The fully qualified name of the legacy table in the format 'project.dataset.table'.
         migrated_table (str): The fully qualified name of the migrated table in the format 'project.dataset.table'.
         dag (str): The DAG ID of the Airflow DAG that contains this task.
         BQ_CONN_ID (str): The ID of the BigQuery connection defined in Airflow.
         legacy_ts_column (str): Timestamp column for time-frame analysis in the legacy table.
         migrated_ts_column (str): Timestamp column for time-frame analysis in the migrated table.
         upper_boundary_days_ago (int): The upper boundary in number of days ago for the time-frame analysis.
         lower_boundary_days_ago (int): The lower boundary in number of days ago for the time-frame analysis.
         delete_past (bool): Flag indicating whether to delete past records in the logging table before running the quality checks.

 Returns
     None
 """

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    # Create a BigQueryHook to interact with BigQuery
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

    # Get the current context and retrieve the DAG execution timestamp
    context = get_current_context()
    dag_execution_ts = f"SAFE_CAST(SPLIT('{context['dag_run'].run_id}','__')[OFFSET(1)] AS TIMESTAMP)"

    # Get the BigQuery client and cursor
    hook.get_client()
    conn = hook.get_conn()
    cursor = conn.cursor()

    delete_exp=''
    if delete_past:
        delete_exp = f"""
        DELETE
        FROM
            `viator-data-warehouse-dev.logging.migration_general_check`
        WHERE
            legacy_table = '{legacy_table}'
            AND migrated_table = '{migrated_table}';
        """
    try:
        cursor.execute(delete_exp)
    except Exception as e:
        log_info = "Table does not exist"
        logging.info(log_info)
    
    # Split the table names into project, dataset, and tablename
    legacy_project, legacy_dataset, legacy_tablename = legacy_table.split('.')
    migrated_project, migrated_dataset, migrated_tablename = migrated_table.split('.')
    print(legacy_project, legacy_dataset, legacy_tablename)
    print(migrated_project, migrated_dataset, migrated_tablename)

    # Execute dynamic SQL to get the list of legacy columns
    legacy_columns_query = f"""
     SELECT column_name
     FROM {legacy_project}.{legacy_dataset}.INFORMATION_SCHEMA.COLUMNS
     WHERE table_name = '{legacy_tablename}';
     """
    cursor.execute(legacy_columns_query)
    legacy_cols_list = cursor.fetchall()
    legacy_columns = [item for sublist in legacy_cols_list for item in sublist]
    print(legacy_columns)

    # Execute dynamic SQL to get the list of migrated columns
    migrated_columns_query = f"""
     SELECT column_name 
     FROM {migrated_project}.{migrated_dataset}.INFORMATION_SCHEMA.COLUMNS
     WHERE table_name = '{migrated_tablename}'
     """
    cursor.execute(migrated_columns_query)
    migrated_cols_list = cursor.fetchall()
    migrated_columns = [item for sublist in migrated_cols_list for item in sublist]
    print(migrated_columns)

    # Add columns from migrated table that are not present in the legacy table
    if len(migrated_columns) > len(legacy_columns):
        for col in migrated_columns:
            if col not in legacy_columns:
                legacy_columns.append(col + "_not_in_legacy")

    # Define the base directory and SQL directory
    base_dir = os.path.dirname(os.path.realpath(__file__))
    sql_dir = os.path.join(base_dir, 'sql')

    # Read the insert query from file and replace placeholders with actual values
    insert_query = open(f"{sql_dir}/logging_migration_insert.sql", 'r').read() \
        .replace('{legacy_project}', legacy_project) \
        .replace('{legacy_dataset}', legacy_dataset) \
        .replace('{legacy_tablename}', legacy_tablename) \
        .replace('{migrated_project}', migrated_project) \
        .replace('{migrated_dataset}', migrated_dataset) \
        .replace('{migrated_tablename}', migrated_tablename) \
        .replace("{context['dag'].dag_id}", context['dag'].dag_id) \
        .replace('{legacy_ts_column}', legacy_ts_column) \
        .replace('{migrated_ts_column}', migrated_ts_column) \
        .replace('{dag_execution_ts}', dag_execution_ts) \
        .replace('{legacy_table}', legacy_table) \
        .replace('{migrated_table}', migrated_table)\


    # Create a BigQueryInsertJobOperator to run the insert query
    insert_job = BigQueryInsertJobOperator(
        task_id=f"insert_query",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": insert_query,
                "useLegacySql": False,
                "writeDisposition": 'WRITE_APPEND',
                "createDisposition": 'CREATE_IF_NEEDED',
                "priority": "INTERACTIVE",
                "destinationTable": {
                    "projectId": 'viator-data-warehouse-dev',
                    "datasetId": 'logging',
                    "tableId": "migration_general_check"
                },
            },
        },
        project_id="viator-data-warehouse-dev",
        dag=dag
    )
    print(insert_query)

    # Execute the insert job
    insert_job.execute(get_current_context())

    # Define timestamp variables for query
    PARTITION_TO = f'TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL {upper_boundary_days} DAY))'
    PARTITION_FROM = f'TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL {lower_boundary_days} DAY))'

    # Iterate over the legacy columns and execute update queries.
    # Case column is not in legacy, we borrow it from migrated
    for i in range(len(legacy_columns)):
        print(f"{i} in {range(len(legacy_columns))}")
    #for i in range(2):
        print(f"COLUMN: {legacy_columns[i]}")
        if not (legacy_columns[i].startswith("_") or legacy_columns[i].startswith("_")):
            query = open(f"{sql_dir}/logging_migration_update.sql", 'r').read() \
                .replace('{legacy_project}', legacy_project) \
                .replace('{legacy_dataset}', legacy_dataset) \
                .replace('{legacy_tablename}', legacy_tablename) \
                .replace('{migrated_project}', migrated_project) \
                .replace('{migrated_dataset}', migrated_dataset) \
                .replace('{migrated_tablename}', migrated_tablename) \
                .replace("{context['dag'].dag_id}", context['dag'].dag_id) \
                .replace('{legacy_ts_column}', legacy_ts_column) \
                .replace('{migrated_ts_column}', migrated_ts_column) \
                .replace('{dag_execution_ts}', dag_execution_ts) \
                .replace("{legacy_columns[i]}", legacy_columns[i]) \
                .replace("{migrated_columns[i]}", migrated_columns[i]) \
                .replace('{PARTITION_TO}', PARTITION_TO) \
                .replace('{PARTITION_FROM}', PARTITION_FROM)

        print(query)
        cursor.execute(query)
        cursor.close()
#---------------end-------------Migration Quality Check---------------end--------------------------------------- 
#--------------start------------Migration Quality Check--------------start--------------------------------------


def check_source_query(table_ids: List[str], target_table_id: str, condition: str) -> str:
    
    """
    Generate a BigQuery SQL query to check if all or any of the tables in a list were updated after a target table.

    Parameters:
    table_ids (List[str]): A list of table IDs in the format "project.dataset.table_name".
    target_table_id (str): The ID of the target table in the format "project.dataset.table_name".
    condition (str): A string that can be either "any" or "all". If "any", the function checks if any of the tables in the list were updated after the target table. If "all", it checks if all of them were.

    Returns:
    str: The generated BigQuery SQL query.

    Raises:
    AssertionError: If the condition is not "any" or "all".
    """

    assert condition in ["any", "all"], "Condition must be either 'any' or 'all'"

    # Split the target table_id into project, dataset, and table_name
    target_project, target_dataset, target_table_name = target_table_id.split('.')

    # Generate the subquery for the target table
    target_subquery = f"""
    SELECT
      TIMESTAMP_MILLIS(last_modified_time) as last_modified_time_target
    FROM
      `{target_project}.{target_dataset}.__TABLES__`
    WHERE
      table_id = '{target_table_name}'
    """

    # Generate the subqueries for the source tables
    source_subqueries = []
    for table_id in table_ids:
        project, dataset, table_name = table_id.split('.')
        subquery = f"""
        SELECT
          TIMESTAMP_MILLIS(last_modified_time) as last_modified_time_source
        FROM
          `{project}.{dataset}.__TABLES__`
        WHERE
          table_id = '{table_name}'
        """
        source_subqueries.append(subquery)

    # Combine all the source subqueries
    source_subquery = " UNION ALL ".join(source_subqueries)

    # Generate the final query
    if condition == "any":
        query = f"""
        WITH t AS ({target_subquery}),
        s AS ({source_subquery})
        SELECT
          count(1)
        FROM
          t
        JOIN
          s
        ON
          t.last_modified_time_target < s.last_modified_time_source
        """
    else:  # condition == "all"
        query = f"""
        WITH t AS ({target_subquery}),
        s AS ({source_subquery})
        SELECT
          count(1)
        FROM
          t
        LEFT JOIN
          s
        ON
          t.last_modified_time_target < s.last_modified_time_source
        WHERE
          s.last_modified_time_source IS NULL
        """
    print(query)
    return query
