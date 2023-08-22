{% macro get_lake_columns(event_name) -%}
{% set columns = ({
'awin_campaign_report': """
    {"name": "campaign", "type": "STRING"},
    {"name": "publisher_id", "type": "STRING"},
    {"name": "publisher_name", "type": "STRING"},
    {"name": "commission_amount_approved", "type": "STRING"},
    {"name": "commission_amount_bonus", "type": "STRING"},
    {"name": "commission_amount_declined", "type": "STRING"},
    {"name": "commission_amount_pending", "type": "STRING"},
    {"name": "commission_amount_total", "type": "STRING"},
    {"name": "quantity_approved", "type": "INTEGER"},
    {"name": "quantity_bonus", "type": "INTEGER"},
    {"name": "quantity_click", "type": "INTEGER"},
    {"name": "quantity_declined", "type": "INTEGER"},
    {"name": "quantity_pending", "type": "INTEGER"},
    {"name": "quantity_total", "type": "INTEGER"},
    {"name": "sale_amount_approved", "type": "STRING"},
    {"name": "sale_amount_bonus", "type": "STRING"},
    {"name": "sale_amount_declined", "type": "STRING"},
    {"name": "sale_amount_pending", "type": "STRING"},
    {"name": "sale_amount_total", "type": "STRING"},
    {"name": "report_date", "type": "DATE"}
""",
'awin_creative_report': """
    {"name": "advertiser_id", "type": "INTEGER"},
    {"name": "advertiser_name", "type": "STRING"},
    {"name": "publisher_id", "type": "INTEGER"},
    {"name": "publisher_name", "type": "STRING"},
    {"name": "region", "type": "STRING"},
    {"name": "currency", "type": "STRING"},
    {"name": "impressions", "type": "INTEGER"},
    {"name": "clicks", "type": "INTEGER"},
    {"name": "creative_id", "type": "INTEGER"},
    {"name": "creative_name", "type": "STRING"},
    {"name": "tag_name", "type": "STRING"},
    {"name": "pending_no", "type": "INTEGER"},
    {"name": "pending_value", "type": "FLOAT"},
    {"name": "pending_comm", "type": "FLOAT"},
    {"name": "confirmed_no", "type": "INTEGER"},
    {"name": "confirmed_value", "type": "FLOAT"},
    {"name": "confirmed_comm", "type": "FLOAT"},
    {"name": "bonus_no", "type": "INTEGER"},
    {"name": "bonus_value", "type": "FLOAT"},
    {"name": "bonus_comm", "type": "FLOAT"},
    {"name": "total_no", "type": "INTEGER"},
    {"name": "total_value", "type": "FLOAT"},
    {"name": "total_comm", "type": "FLOAT"},
    {"name": "declined_no", "type": "INTEGER"},
    {"name": "declined_value", "type": "FLOAT"},
    {"name": "declined_comm", "type": "FLOAT"},
    {"name": "tags", "type": "STRING"},
    {"name": "report_date", "type": "DATE"}
"""})%} {{ columns[event_name] }} {% endmacro %}

CREATE TABLE IF NOT EXISTS {{ project_id }}.{{ dataset_id }}.{{ table_id }}
(
    {{get_lake_columns(table_id)}}
)
PARTITION BY {{ partition_field }}
OPTIONS(
    labels=[("task_id", "{{task_name}}")]
)