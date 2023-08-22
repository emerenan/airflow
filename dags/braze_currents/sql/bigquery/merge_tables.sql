{% macro create_columns(event_name) -%}
{% set columns = ({
    "message_engagement": """
    id STRING,
    guid STRING,
    user_id STRING,
    customer_id STRING,
    time TIMESTAMP,
    timezone STRING,
    event_date DATE,
    event_hour TIME,
    abort_log STRING,
    abort_type STRING,
    action STRING,
    ad_id STRING,
    ad_id_type STRING,
    ad_tracking_enabled BOOLEAN,
    app_group_id STRING,
    app_id STRING,
    bounce_reason STRING,
    button_action_type STRING,
    button_id STRING,
    button_string STRING,
    campaign_id STRING,
    campaign_name STRING,
    canvas_id STRING,
    canvas_name STRING,
    canvas_step_id STRING,
    canvas_step_message_variation_id STRING,
    canvas_step_name STRING,
    canvas_variation_id STRING,
    canvas_variation_name STRING,
    card_id STRING,
    category STRING,
    channel STRING,
    content_card_id STRING,
    conversion_behavior STRING,
    conversion_behavior_index INTEGER,
    device_id STRING,
    device_model STRING,
    dispatch_id STRING,
    email_address STRING,
    error STRING,
    esp STRING,
    experiment_split_id STRING,
    experiment_split_name STRING,
    experiment_step_id STRING,
    from_domain STRING,
    from_phone_number STRING,
    in_control_group BOOLEAN,
    inbound_media_urls STRING,
    inbound_phone_number STRING,
    ip_pool STRING,
    is_amp BOOLEAN,
    is_drop BOOLEAN,
    link_alias STRING,
    link_id STRING,
    machine_open STRING,
    message_body STRING,
    message_variation_id STRING,
    message_variation_name STRING,
    os_version STRING,
    phone_number STRING,
    platform STRING,
    provider_error_code STRING,
    send_id STRING,
    sending_ip STRING,
    subscription_group_api_id STRING,
    subscription_group_id STRING,
    subscription_status STRING,
    to_phone_number STRING,
    url STRING,
    user_agent STRING,
    user_phone_number STRING,
    workflow_id STRING,
    athena_partition STRING,
    event_type STRING""",
"customer_behavior": """
    id STRING,
    guid STRING,
    user_id STRING,
    customer_id STRING,
    time TIMESTAMP,
    event_date DATE,
    event_hour TIME,
    timezone STRING,
    name STRING,
    app_id STRING,
    platform STRING,
    os_version STRING,
    device_model STRING,
    device_id STRING,
    properties STRING,
    ad_id STRING,
    ad_id_type STRING,
    ad_tracking_enabled BOOLEAN,
    product_id STRING,
    price FLOAT64,
    currency STRING,
    session_id STRING,
    gender STRING,
    country STRING,
    language STRING,
    sdk_version STRING,
    duration FLOAT64,
    longitude FLOAT64,
    latitude FLOAT64,
    altitude FLOAT64,
    ll_accuracy FLOAT64,
    alt_accuracy FLOAT64,
    source STRING,
    app_group_id STRING,
    random_bucket_number INTEGER,
    prev_random_bucket_number INTEGER,
    athena_partition STRING,
    event_type STRING"""}) %}
{{ columns[event_name] }}
{% endmacro %}

{% macro insert_columns(event_name) -%}
{% set columns = ({
"message_engagement":"""
    id
    , guid
    , user_id
    , customer_id
    , time
    , timezone
    , event_date
    , event_hour
    , abort_log
    , abort_type
    , action
    , ad_id
    , ad_id_type
    , ad_tracking_enabled
    , app_group_id
    , app_id
    , bounce_reason 
    , button_action_type
    , button_id
    , button_string
    , campaign_id
    , campaign_name
    , canvas_id
    , canvas_name
    , canvas_step_id
    , canvas_step_message_variation_id
    , canvas_step_name
    , canvas_variation_id
    , canvas_variation_name
    , card_id
    , category
    , channel
    , content_card_id
    , conversion_behavior
    , conversion_behavior_index
    , device_id
    , device_model
    , dispatch_id
    , email_address
    , error
    , esp
    , experiment_split_id
    , experiment_split_name
    , experiment_step_id
    , from_domain
    , from_phone_number
    , in_control_group
    , inbound_media_urls
    , inbound_phone_number
    , ip_pool
    , is_amp
    , is_drop
    , link_alias
    , link_id
    , machine_open
    , message_body
    , message_variation_id
    , message_variation_name
    , os_version
    , phone_number
    , platform
    , provider_error_code
    , send_id
    , sending_ip
    , subscription_group_api_id
    , subscription_group_id
    , subscription_status
    , to_phone_number
    , url
    , user_agent
    , user_phone_number
    , workflow_id
    , athena_partition
    , event_type""",
"customer_behavior": """
    id,
    guid,
    user_id,
    customer_id,
    time,
    event_date,
    event_hour,
    timezone,
    name,
    app_id,
    platform,
    os_version,
    device_model,
    device_id,
    properties,
    ad_id,
    ad_id_type,
    ad_tracking_enabled,
    product_id,
    price,
    currency,
    session_id,
    gender,
    country,
    language,
    sdk_version,
    duration,
    longitude,
    latitude,
    altitude,
    ll_accuracy,
    alt_accuracy,
    source,
    app_group_id,
    random_bucket_number,
    prev_random_bucket_number,
    athena_partition,
    event_type"""}) %}
{{ columns[event_name] }}
{% endmacro %}

CREATE TABLE IF NOT EXISTS {{ dwh_project_id }}.{{ dwh_dataset_id }}.{{ final_table }} (
{{  create_columns(final_table|replace('_test', ''))}}
)
PARTITION BY {{ partition_field }};

MERGE {{ dwh_project_id }}.{{ dwh_dataset_id }}.{{ final_table }} AS a 
USING {{ lake_project_id }}.{{ lake_dataset_id }}.{{ temp_table }} AS b 
ON a.{{ ref_column }} = b.{{ ref_column }}
WHEN NOT MATCHED THEN 
    INSERT({{ insert_columns(final_table|replace('_test', ''))}})
    VALUES({{ insert_columns(final_table|replace('_test', ''))}})