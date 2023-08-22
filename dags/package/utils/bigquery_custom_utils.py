#
# created by kxu on 2022-06-17 1:37 pm
# additional functions for Aiflow BigQuery operators
#
import logging
from datetime import datetime

import pendulum
from airflow.models import Variable

from utils.airflow_custom_variables import *

DEFAULT_TZ = pendulum.timezone("UTC")


def job_configuration_query(query, project=None, dataset=None, table=None, partitioned=False):
    """
    create a bigquery query job configuration
    :param query: string: Query to be executed in BigQuery
    :param project: string: project ID of destination table
    :param dataset: string: dataset ID of destination table
    :param table: string: table ID of destination table
    :param partitioned: bool: specify if target table is partitioned table TRUE or not FALSE
    REF https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
    """

    query_job_config = {
        "query": query,
        "useLegacySql": False
    }

    if project and dataset and table:  # extract job: query to table
        query_job_config["writeDisposition"] = "WRITE_TRUNCATE"
        query_job_config["createDisposition"] = "CREATE_IF_NEEDED"

        destination_table_config = {
            "project_id": project,
            "dataset_id": dataset,
            "table_id": f"{table}${{{{ ds_nodash }}}}" if partitioned else table
        }
        query_job_config["destinationTable"] = destination_table_config
    elif not project and not dataset and not table:  # DML query, no extra config required
        pass
    else:
        raise TypeError("One or more destination table name component is missing: project_id | dataset_id | table_id")

    logging.info(query_job_config)

    return {
        "query": query_job_config
    }


def reformat_partition_date(partition_date):
    """
    convert date format from YYYY-MM-DD to YYYYMMDD ("%Y-%m-%d" > "%Y%m%d")
    :param partition_date: string: format YYYY-MM-DD ("%Y-%m-%d")
    :rtype: string: new date format YYYYMMDD ("%Y%m%d")
    """

    try:
        partition_date_bq = datetime.strptime(partition_date, "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError:
        raise ValueError("Incorrect partition date format, should be YYYY-MM-DD")

    return partition_date_bq


def build_table_resource(dag_id, task_id, schema=None, partitioned=False, description=None, **kwargs):
    """
    create table resource for adding table labels and description
    :param dag_id: string
    :param task_id: string
    :param schema: JSON object
    :param partitioned: boolean
    :param description: string
    :return: dict: table_resource
    """

    """
    Basic table resource template without schema, used directly for creating Views
    """
    table_resource = {
        "labels": {
            "dag": dag_id.lower(),
            "task": task_id.lower()
        },
        "description": description
    }

    """
    Add table schema fields if target is a table and schema provided
    """
    if schema:
        table_resource["schema"] = {"fields": schema}

    """
    Add table partition details is target table is partitioned
    """
    if partitioned:
        table_resource["time_partitioning"] = {"type": "DAY"}

    for key, value in kwargs.items():
        table_resource[key] = value

    return table_resource


def build_xload_query(dag_id, task_id, project, dataset, table, s3_uris, data_format, description=""):
    """
    Generate a LOAD DATA INTO query for transferring data from S3 to BigQuery
    :param dag_id: string
    :param task_id: string
    :param project: string
    :param dataset: string
    :param table: string
    :param s3_uris: string
    :param data_format: string
    :param description: string
    :return: string
    """
    bq_omni_connection = Variable.get(BQ_OMNI_CONNECTION_NAME)
    labels = f"('dag', '{dag_id.lower()}'), ('task', '{task_id}')"

    load_data_query = f"""
        LOAD DATA
          OVERWRITE `{project}.{dataset}.{table}` OPTIONS(labels=[{labels}], description='{description}')
        FROM
          FILES (uris = ['{s3_uris}'],
            format = '{data_format.upper()}')
        WITH CONNECTION `{bq_omni_connection}`
    """

    return load_data_query


def bigquery_omni_location():
    """
    Return the default location for BigQuery Omni connection, default: aws-us-east-1
    """
    return Variable.get(BQ_OMNI_CONNECTION_LOCATION)


def bigquery_extract_query(project, dataset, table):
    """
    Generate a SELECT query for transferring data from between BigQuery tables
    :param project: string
    :param dataset: string
    :param table: string
    """

    return f"SELECT * FROM `{project}.{dataset}.{table}`"
