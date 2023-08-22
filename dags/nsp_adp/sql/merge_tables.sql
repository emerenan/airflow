{% macro get_insert_columns(event_name) -%}
{% if event_name=='nsp_adp_product_activation' %}
    event_id,
    event_timestamp,
    data_stream_timestamp,
    supplier_code,
    product_code,
    triggered_by,
    partition_date,
    event_type  {# Below is the other option of columns #} {% else %}
    event_id,
    data_stream_timestamp,
    event_timestamp,
    product_stage_id,
    supplier_code,
    product_code,
    stage_id,
    criteria_rule_id,
    criteria_data,
    cancellation_rate_level,
    total_booking_count_last_30_Days,
    cancelled_booking_count_last_30_days,
    nsp_adp_product_timestamp,
    partition_date,
    event_type {% endif %}
{% endmacro %}

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

{% macro get_filter(type_filter) -%}
{% if type_filter == 'hourly' %}
    DATE(_PARTITIONTIME) BETWEEN DATE_ADD('{{ ref_date }}', INTERVAL -1 DAY) AND '{{ ref_date }}' {% else %} 
    DATE(_PARTITIONTIME) = '{{ ref_date }}'{% endif %}{% endmacro %}

{% set events = event_list %}
{% set nsp_adp_product_activation = ['nsp_adp_product_activated','nsp_adp_product_deactivated'] %}
{% set nsp_adp_product_stage = ['nsp_adp_product_stage_created','nsp_adp_product_stage_updated'] %}

MERGE {{ dwh_project_id }}.{{ dwh_dataset_id }}.{{ final_table }} AS a 
USING (
WITH {% for event in events %} {{event}} as (
SELECT {% if event|replace('_test', '') in nsp_adp_product_activation %}
{{ get_lake_columns('nsp_adp_product_activation') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% elif event|replace('_test', '') in nsp_adp_product_stage %}
{{ get_lake_columns('nsp_adp_product_stage') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% else %}
{{ get_lake_columns('other_product_stage') }} 
    ,'{{ event | replace("nsp_adp_", "") | replace("_test", "")}}' as event_type {% endif %}
FROM {{ project_lake_id }}.{{ dataset_lake_id }}.{{event}}
WHERE {{ get_filter(dag_type) }}
){% if not loop.last %},
{% endif %}{% endfor %}
{% for event in events %}SELECT * FROM {{event}} {% if not loop.last %}
UNION ALL 
{% endif %}{% endfor %}
) AS b 
ON a.{{ ref_column }} = b.{{ ref_column }}
WHEN NOT MATCHED THEN 
    INSERT({{ get_insert_columns(final_table|replace('_test', ''))}})
    VALUES({{ get_insert_columns(final_table|replace('_test', ''))}})
