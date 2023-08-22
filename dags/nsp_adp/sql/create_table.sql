{% macro get_lake_columns(event_name) -%}
{% set columns = ({
'nsp_adp_product_activation': """
    eventId as event_id
    ,TIMESTAMP_SECONDS(event_timestamp) as event_timestamp
    ,TIMESTAMP_SECONDS(dataStreamTimestamp) as data_stream_timestamp
    ,supplier_code
    ,product_code
    ,triggered_by
    ,DATE(TIMESTAMP_SECONDS(event_timestamp)) as partition_date
""",
'nsp_adp_product_stage': """
    eventId as event_id
    ,TIMESTAMP_SECONDS(dataStreamTimestamp) as data_stream_timestamp
    ,TIMESTAMP_SECONDS(event_timestamp) as event_timestamp
    ,product_stage_id
    ,supplier_code
    ,product_code
    ,stage_id
    ,criteria_rule_id
    ,criteria_data
    ,cancellation_rate_level
    ,total_booking_count_last_30_Days
    ,cancelled_booking_count_last_30_days
    ,nsp_adp_product_timestamp
    ,DATE(TIMESTAMP_SECONDS(event_timestamp)) as partition_date
""",
'other_product_stage': """
    eventId as event_id
    , TIMESTAMP_SECONDS(dataStreamTimestamp) as data_stream_timestamp
    , TIMESTAMP_SECONDS(event_timestamp) as event_timestamp
    , null as product_stage_id
    , supplier_code
    , product_code
    , stage_id
    , criteria_rule_id
    , criteria_data
    , cancellation_rate_level
    , total_booking_count_last_30_Days
    , cancelled_booking_count_last_30_days
    , nsp_adp_product_timestamp
    , DATE(TIMESTAMP_SECONDS(event_timestamp)) as partition_date
""",})%} {{ columns[event_name] }} {% endmacro %}

{% set events = event_list %}
{% set nsp_adp_product_activation = ['nsp_adp_product_activated','nsp_adp_product_deactivated'] %}
{% set nsp_adp_product_stage = ['nsp_adp_product_stage_created','nsp_adp_product_stage_updated'] %}

{% set label_list = dag_labels %}
CREATE TABLE IF NOT EXISTS {{ project_id }}.{{ dataset_id }}.{{ final_table }}
({% if final_table|replace('_test', '')=='nsp_adp_product_activation'%}
    event_id STRING,
    event_timestamp TIMESTAMP,
    data_stream_timestamp TIMESTAMP,
    supplier_code STRING,
    product_code STRING,
    triggered_by STRING,
    partition_date DATE,
    event_type STRING  {# Below is the other option of columns #}  {% else %}
    event_id STRING,
    data_stream_timestamp TIMESTAMP,
    event_timestamp TIMESTAMP,
    product_stage_id INTEGER,
    supplier_code STRING,
    product_code STRING,
    stage_id INTEGER,
    criteria_rule_id INTEGER,
    criteria_data STRING,
    cancellation_rate_level STRING,
    total_booking_count_last_30_Days INTEGER,
    cancelled_booking_count_last_30_days INTEGER,
    nsp_adp_product_timestamp INTEGER,
    partition_date DATE,
    event_type STRING {% endif %}
)
PARTITION BY {{ partition_field }}
OPTIONS(
    description="{{ dag_description }}",
    labels=[{% for label in label_list %}{{label}}{% if not loop.last %}, {% endif %}{% endfor %}]
) as {% for event in events %}
SELECT {% if event|replace('_test', '') in nsp_adp_product_activation %}
{{ get_lake_columns('nsp_adp_product_activation') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% elif event|replace('_test', '') in nsp_adp_product_stage %}
{{ get_lake_columns('nsp_adp_product_stage') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% else %}
{{ get_lake_columns('other_product_stage') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% endif %}
FROM {{ project_lake_id }}.{{ dataset_lake_id }}.{{event}} {% if not loop.last %}
UNION ALL{% endif %}{% endfor %};