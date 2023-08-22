with cte as (
SELECT
    id,
    '' as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type
    , '' as action
    , '' as ad_id
    , '' as ad_id_type
    , false as ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , '' as button_action_type
    , '' as button_id
    , '' as button_string
    , campaign_id
    , campaign_name
    , '' as canvas_id
    , '' as canvas_name
    , '' as canvas_step_id
    , '' as canvas_step_message_variation_id
    , '' as canvas_step_name
    , '' as canvas_variation_id
    , '' as canvas_variation_name
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , conversion_behavior
    , conversion_behavior_index
    , '' as device_id
    , '' as device_model
    , '' as dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , message_variation_id
    , '' as message_variation_name
    , '' as os_version
    , '' as phone_number
    , '' as platform
    , '' as provider_error_code
    , send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'campaigns_conversion' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_campaigns_conversion
UNION ALL
SELECT
    id,
    '' as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type
    , '' as action
    , '' as ad_id
    , '' as ad_id_type
    , false as ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , '' as button_action_type
    , '' as button_id
    , '' as button_string
    , campaign_id
    , campaign_name
    , '' as canvas_id
    , '' as canvas_name
    , '' as canvas_step_id
    , '' as canvas_step_message_variation_id
    , '' as canvas_step_name
    , '' as canvas_variation_id
    , '' as canvas_variation_name
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , '' as conversion_behavior
    , 0 as conversion_behavior_index
    , '' as device_id
    , '' as device_model
    , '' as dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , message_variation_id
    , '' as message_variation_name
    , '' as os_version
    , '' as phone_number
    , '' as platform
    , '' as provider_error_code
    , send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'campaigns_enrollincontrol' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_campaigns_enrollincontrol
UNION ALL
SELECT
    id,
    '' as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    '' as campaign_id, 
    '' as campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    conversion_behavior, 
    conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    '' as dispatch_id, 
    '' as email_address, 
    '' as error, 
    '' as esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    '' as from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    '' as ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    '' as message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    '' as send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'canvas_conversion' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_canvas_conversion
UNION ALL
SELECT
    id,
    '' as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    '' as campaign_id, 
    '' as campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    '' as dispatch_id, 
    '' as email_address, 
    '' as error, 
    '' as esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    '' as from_domain, 
    '' as from_phone_number,  
    in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    '' as ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    '' as message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    '' as send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'canvas_entry' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_canvas_entry
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    ad_id,
    ad_id_type,
    ad_tracking_enabled,
    '' as app_group_id,
    app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    '' as message_variation_name, 
    os_version,
    '' as phone_number,
    platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'contentcard_click' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_contentcard_click
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    ad_id,
    ad_id_type,
    ad_tracking_enabled,
    '' as app_group_id,
    app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    '' as message_variation_name,
    os_version,
    '' as phone_number,
    platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'contentcard_impression' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_contentcard_impression
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    '' as app_group_id,
    '' as app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    '' as device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    '' as message_variation_name, 
    '' as os_version,
    '' as phone_number,
    '' as platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'contentcard_send' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_contentcard_send
UNION ALL
SELECT 
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour,
    abort_log,
    abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    app_group_id,
    '' as app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    '' as device_model,
    dispatch_id,
    email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    message_variation_name,
    '' as os_version,
    '' as phone_number,
    '' as platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date`,
    'email_abort' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_abort
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    false as is_amp, 
    is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email bounce' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_bounce
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    is_amp, 
    false as is_drop, 
    link_alias, 
    link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    url, 
    user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_click' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_click
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_delivery' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_delivery
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    '' as app_group_id,
    '' as app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_open' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_open
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    '' as esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    '' as from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name, 
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_send' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_send
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    '' as user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_softbounce' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_softbounce
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action, 
    '' as ad_id, 
    '' as ad_id_type, 
    false as ad_tracking_enabled, 
    '' as app_group_id, 
    '' as app_id, 
    '' as bounce_reason, 
    '' as button_action_type, 
    '' as button_id, 
    '' as button_string, 
    campaign_id, 
    campaign_name, 
    canvas_id, 
    canvas_name, 
    canvas_step_id, 
    '' as canvas_step_message_variation_id, 
    canvas_step_name, 
    canvas_variation_id, 
    canvas_variation_name, 
    '' as card_id, 
    '' as category, 
    '' as channel, 
    '' as content_card_id, 
    '' as conversion_behavior, 
    0 as conversion_behavior_index, 
    '' as device_id, 
    '' as device_model, 
    dispatch_id, 
    email_address, 
    '' as error, 
    esp, 
    '' as experiment_split_id, 
    '' as experiment_split_name, 
    '' as experiment_step_id, 
    from_domain, 
    '' as from_phone_number,  
    false as in_control_group, 
    '' as inbound_media_urls, 
    '' as inbound_phone_number, 
    ip_pool, 
    false as is_amp, 
    false as is_drop, 
    '' as link_alias, 
    '' as link_id, 
    '' as machine_open, 
    '' as message_body, 
    message_variation_id, 
    '' as message_variation_name,
    '' as os_version, 
    '' as phone_number, 
    '' as platform, 
    '' as provider_error_code, 
    send_id, 
    '' as sending_ip, 
    '' as subscription_group_api_id, 
    '' as subscription_group_id, 
    '' as subscription_status,  
    '' as to_phone_number, 
    '' as url, 
    user_agent, 
    '' as user_phone_number, 
    '' as workflow_id,
    `date` as athena_partition,
    'email_markasspam' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_email_markasspam
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    '' as timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    '' as app_group_id,
    '' as app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    '' as campaign_id,
    '' as campaign_name,
    '' as canvas_id,
    '' as canvas_name,
    '' as canvas_step_id,
    '' as canvas_step_message_variation_id,
    '' as canvas_step_name,
    '' as canvas_variation_id,
    '' as canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    conversion_behavior_index,
    '' as device_id,
    '' as device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    experiment_split_id,
    '' as experiment_split_name,
    experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    '' as message_variation_id,
    '' as message_variation_name, 
    '' as os_version,
    '' as phone_number,
    '' as platform,
    '' as provider_error_code,
    '' as send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'experimentstep_conversion' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_canvas_experimentstep_conversion
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    '' as timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    '' as app_group_id,
    '' as app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    '' as campaign_id,
    '' as campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    '' as canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    '' as device_id,
    '' as device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    experiment_split_id,
    experiment_split_name,
    experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    '' as message_variation_id,
    '' as message_variation_name,
    '' as os_version,
    '' as phone_number,
    '' as platform,
    '' as provider_error_code,
    '' as send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'experimentstep_splitentry' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_canvas_experimentstep_splitentry
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    ad_id,
    ad_id_type,
    ad_tracking_enabled,
    '' as app_group_id,
    app_id,
    '' as bounce_reason,
    '' as button_action_type,
    button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    '' as message_variation_name,
    os_version,
    '' as phone_number,
    platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'inappmessage_click' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_inappmessage_click
UNION ALL
SELECT
    id,
    '' as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action,
    ad_id,
    ad_id_type,
    ad_tracking_enabled,
    '' as app_group_id,
    app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    '' as canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    device_model,
    '' as dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    '' as message_variation_name,
    os_version,
    '' as phone_number,
    platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'inappmessage_impression' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_inappmessage_impression
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id,
    external_user_id,
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour,
    abort_log,
    abort_type,
    '' as action,
    '' as ad_id,
    '' as ad_id_type,
    false as ad_tracking_enabled,
    app_group_id,
    app_id,
    '' as bounce_reason,
    '' as button_action_type,
    '' as button_id,
    '' as button_string,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    canvas_step_message_variation_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    '' as card_id,
    '' as category,
    '' as channel,
    '' as content_card_id,
    '' as conversion_behavior,
    0 as conversion_behavior_index,
    device_id,
    '' as device_model,
    dispatch_id,
    '' as email_address,
    '' as error,
    '' as esp,
    '' as experiment_split_id,
    '' as experiment_split_name,
    '' as experiment_step_id,
    '' as from_domain,
    '' as from_phone_number,
    false as in_control_group,
    '' as inbound_media_urls,
    '' as inbound_phone_number,
    '' as ip_pool,
    false as is_amp,
    false as is_drop,
    '' as link_alias,
    '' as link_id,
    '' as machine_open,
    '' as message_body,
    message_variation_id,
    message_variation_name,
    '' as os_version,
    '' as phone_number,
    platform,
    '' as provider_error_code,
    send_id,
    '' as sending_ip,
    '' as subscription_group_api_id,
    '' as subscription_group_id,
    '' as subscription_status,
    '' as to_phone_number,
    '' as url,
    '' as user_agent,
    '' as user_phone_number,
    '' as workflow_id,
    `date` as athena_partition,
    'pushnotification_abort' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_pushnotification_abort
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action
    , ad_id
    , ad_id_type
    , ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , '' as button_action_type
    , '' as button_id
    , '' as button_string
    , campaign_id
    , campaign_name
    , canvas_id
    , canvas_name
    , canvas_step_id
    , '' as canvas_step_message_variation_id
    , canvas_step_name
    , canvas_variation_id
    , canvas_variation_name
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , '' as conversion_behavior
    , 0 as conversion_behavior_index
    , device_id
    , '' as device_model
    , dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , message_variation_id
    ,'' as message_variation_name
    , '' as os_version
    , '' as phone_number
    , platform
    , '' as provider_error_code
    , send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'pushnotification_bounce' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_pushnotification_bounce
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type,
    '' as action
    , ad_id
    , ad_id_type
    , ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , button_action_type
    , '' as button_id
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
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , '' as conversion_behavior
    , 0 as conversion_behavior_index
    , device_id
    , device_model
    , dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , message_variation_id
    , '' as message_variation_name
    , os_version
    , '' as phone_number
    , platform
    , '' as provider_error_code
    , send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'pushnotification_open' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_pushnotification_open
UNION ALL
SELECT
    id,
    regexp_replace(to_hex(md5(lower(dispatch_id||external_user_id))), '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5') as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type
    ,'' as action
    , ad_id
    , ad_id_type
    , ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , '' as button_action_type
    , '' as button_id
    , '' as button_string
    , campaign_id
    , campaign_name
    , canvas_id
    , canvas_name
    , canvas_step_id
    , '' as canvas_step_message_variation_id
    , canvas_step_name
    , canvas_variation_id
    , canvas_variation_name
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , '' as conversion_behavior
    , 0 as conversion_behavior_index
    , device_id
    , '' as device_model
    , dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , message_variation_id
    , '' as message_variation_name
    , '' as os_version
    , '' as phone_number
    , platform
    , '' as provider_error_code
    , send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'pushnotification_send' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_messages_pushnotification_send
UNION ALL
SELECT
    id,
    '' as guid,
    user_id, 
    external_user_id as customer_id, 
    TIMESTAMP_SECONDS(time) as time,
    '' as timezone,
    cast(TIMESTAMP_SECONDS(time) as DATE) as event_date,
    cast(TIMESTAMP_SECONDS(time) as TIME) as event_hour
    , '' as abort_log
    , '' as abort_type
    , '' as action
    , '' as ad_id
    , '' as ad_id_type
    , false as ad_tracking_enabled
    , '' as app_group_id
    , app_id
    , '' as bounce_reason 
    , '' as button_action_type
    , '' as button_id
    , '' as button_string
    , '' as campaign_id
    , '' as campaign_name
    , '' as canvas_id
    , '' as canvas_name
    , '' as canvas_step_id
    , '' as canvas_step_message_variation_id
    , '' as canvas_step_name
    , '' as canvas_variation_id
    , '' as canvas_variation_name
    , '' as card_id
    , '' as category
    , '' as channel
    , '' as content_card_id
    , '' as conversion_behavior
    , 0 as conversion_behavior_index
    , device_id
    , '' as device_model
    , '' as dispatch_id
    , '' as email_address
    , '' as error
    , '' as esp
    , '' as experiment_split_id
    , '' as experiment_split_name
    , '' as experiment_step_id
    , '' as from_domain
    , '' as from_phone_number
    , false as in_control_group
    , '' as inbound_media_urls
    , '' as inbound_phone_number
    , '' as ip_pool
    , false as is_amp
    , false as is_drop
    , '' as link_alias
    , '' as link_id
    , '' as machine_open
    , '' as message_body
    , '' as message_variation_id
    , '' as message_variation_name
    , '' as os_version
    , '' as phone_number
    , '' as platform
    , '' as provider_error_code
    , '' as send_id
    , '' as sending_ip
    , '' as subscription_group_api_id
    , '' as subscription_group_id
    , '' as subscription_status
    , '' as to_phone_number
    , '' as url
    , '' as user_agent
    , '' as user_phone_number
    , '' as workflow_id
    , `date` as athena_partition
    , 'unistall' as event_type
FROM {{project_id}}.{{dataset_id}}.event_type_users_behaviors_uninstall
)
select * except(row_num) 
from (
    select *,
    row_number() over ( partition by id order by id ) row_num
    from cte) t
where row_num=1;