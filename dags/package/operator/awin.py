
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import AirflowException
import pandas as pd
from airflow.models import Variable
import requests

AWIN_AUTH = Variable.get("AWIN_AUTH")
BQ_CONN_ID = Variable.get("bq_data_lake_connection")
INGESTION_CONFIG = Variable.get("awin_data_ingestion_settings", deserialize_json=True)
LAKE_PROJECT = Variable.get('bq_data_lake_project')
DWH_PROJECT = Variable.get("bq_data_warehouse_project")

class Routes:
    ADVERTISERS = {"url": "https://api.awin.com/accounts?type=advertiser", "type": "advertisers"}
    CREATIVE = {"url": "https://api.awin.com/advertisers/{advertiser_id}/reports/creative", "type": "creative",
                "table_name": "awin_creative_report"}
    CAMPAIGN = {"url": "https://api.awin.com/advertisers/{advertiser_id}/reports/campaign", "type": "campaign",
                "table_name": "awin_campaign_report"}


def get_advertisers_list():
    """
        Use the AWIN API to get current advertiser list
        We remove the ones with "CLOSED" tag on account Name
        Ex: "accountName": "TripAdvisor Rentals AU - CLOSED 30.04.2020"
    """
    try:
        resp = requests.get(
            url="https://api.awin.com/accounts?type=advertiser", 
            headers={
                'Authorization': AWIN_AUTH
                }).json()
        
        if "error" in resp.keys():
            raise Exception(f"Request state: {resp['error']}\n"
                            f"Request Description {resp['description']}")
        accounts = [x['accountId'] for x in resp['accounts'] if "CLOSED" not in x['accountName']]
    except Exception as e:
        raise e
    return accounts
    
    
def process_dataframe(df: pd.DataFrame, report_date: str) -> pd.DataFrame:
   
    def get_first_element_of_data_frame(series: pd.Series):
        """
            Get the first Non NaN element from a Pandas Series
            Fails if no element is found

            :param series: Pandas series to retrieve
        """
        return series.loc[~series.isnull()].iloc[0]

    def explode_json(json_struct: dict, parent: str, leaves: list):
        """
            Recursively explore a dict and return the leave names to create multiple columns from complex objects

            Populates the leaves data structure with all the elements
        """
        for key, values in json_struct.items():
            if isinstance(values, dict):
                explode_json(values, f"{key}_", leaves)
            else:
                leaf = f"{parent}{key}"
                leaves.append(leaf)

    def camel_case_to_snake(s: str):
        """
            Replaces camelCase string for snake_case
        """
        return ''.join(['_' + c.lower() if c.isupper() else c for c in s])

    def safe_dict_get(dct: dict, keys: list):
        """
            Get the value from the last key of a nested dict based on a list of keys
        """
        for key in keys:
            try:
                dct = dct[key]
            except KeyError:
                return None
        return dct

    # Replace any nulls for empty strings
    df.fillna("", inplace=True)
    # Find any column that has a nested struct (json) and "explode" it into multiple columns
    for col in df.columns:
        first_non_nan = get_first_element_of_data_frame(df[col])
        if isinstance(first_non_nan, dict):
            leaves = []
            explode_json(first_non_nan, "", leaves)
            for leave in leaves:
                keys = leave.split('_')
                df[camel_case_to_snake(leave)] = df[col].apply(lambda x: safe_dict_get(x, keys))
            # Drop original column
            df.drop(col, axis=1, inplace=True)
    df = df.rename(camel_case_to_snake, axis="columns")
    df['report_date'] = report_date
    return df


def download_report_data(accounts, report, ref_date, is_backfill=False):
    '''
        Here we have a stable amount of accounts (it shouldn't change much) so it's safe to leave it as dynamic

        Params:
            - example: ?startDate=2023-01-01&endDate=2023-01-01&region=GB&timezone=UTC
            - startDate
            - endDate
            - region
            - timezone (default UTC)
    '''
    
    def get_json_data(advertiser_id: int, route: str, route_type: str, parameters) -> pd.DataFrame:
        resp = requests.get(url=route.replace("{advertiser_id}", str(advertiser_id)),
                            headers={'Authorization': AWIN_AUTH},
                            params=parameters)
        resp = resp.json()
        try:
            if route_type == Routes.CAMPAIGN.get("type"):
                resp = resp["result"]

            return pd.DataFrame.from_dict(resp)
        except Exception as e:
            print(e)
            return None

    def pandas_to_bq(df: pd.DataFrame, table_name: str):
    
        bq = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

        try:
            df.to_gbq(destination_table=table_name,
                    project_id=LAKE_PROJECT,
                    if_exists="replace",
                    credentials=bq.get_credentials())
        except Exception as e:
            raise e
    
    df: pd.DataFrame = pd.DataFrame()

    # Defining report variables in order to applied in each below functions
    if report=='creative':
        report_route= Routes.CREATIVE.get("url")
        report_route_type = Routes.CREATIVE.get("type")
        report_table_name = Routes.CREATIVE.get('table_name')
    else:
        report_route= Routes.CAMPAIGN.get("url")
        report_route_type = Routes.CAMPAIGN.get("type")
        report_table_name = Routes.CAMPAIGN.get('table_name')

    for account in accounts:
        print(f"Processing {account} data - Current df size: {df.shape}")
        if report=='creative':
            account_df = get_json_data(
                advertiser_id=account,
                route=report_route,
                route_type=report_route_type,
                parameters={
                    "startDate": ref_date,
                    "endDate": ref_date,
                    "region": INGESTION_CONFIG["advertiser_region"][str(account)],
                    "timezone": "UTC"
                }
            )
        else:
            account_df = get_json_data(
                advertiser_id=account,
                route=report_route,
                route_type=report_route_type,
                parameters={
                    "startDate": ref_date,
                    "endDate": ref_date
                }
            )
        
        # If it's the first non-empty DataFrame we use it to build our final DF schema
        # Otherwise we concat it
        if account_df is not None:
            if df.empty:
                df = account_df
            else:
                df = pd.concat([df, account_df])
                
    print(f"Finished processing, columns : {df.columns}")
    
    table_id  = f"{LAKE_PROJECT}.data_landing.{report_table_name}_backfill" if is_backfill else f"{LAKE_PROJECT}.data_landing.{report_table_name}"
    
    if df.shape[0] > 0:
        df = process_dataframe(df=df, report_date=ref_date)
        pandas_to_bq(df=df, table_name=table_id)
        print(f"Sent {df.shape[0]} rows to {table_id}")
        
    return df.shape[0]


def get_delta(sql_path_base, report):
    
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from package.utils.airflow_render_jinja  import render_jinja
    
    sql = render_jinja(
        path_base=sql_path_base, 
        sql_path='backfill_check.sql', 
        sql_params={
            'dwh_project': DWH_PROJECT,
            'event_type': f"awin_{report}_report"
        }
    )
        
    bq = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False, location='US')
    dates = bq.get_records(sql)
    if dates[0]:
        return dates[0]
    else:
        raise AirflowException(
            "There are no dates to backfill data!"
        )        

