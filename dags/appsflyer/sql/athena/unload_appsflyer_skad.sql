UNLOAD (
with t_skad_installs as (
SELECT 
    'skad_installs' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,ad_network_adset_id AS adset_id
    ,ad_network_adset_name AS adset_name
    ,ad_network_campaign_id AS campaign_id
    ,ad_network_campaign_name AS campaign_name
    ,ad_network_channel AS channel
    ,CAST(null as VARCHAR) AS network_name
    ,ad_network_source_app_id AS source_app_id
    ,af_attribution_flag AS flag_ssot
    ,af_prt AS partner
    ,app_id
    ,city
    ,CAST(null as VARCHAR) AS did_win
    ,event_name
    ,CAST(null as VARCHAR) AS event_revenue
    ,event_uuid
    ,event_value
    ,CAST(null as VARCHAR) AS fidelity_type
    ,install_date
    ,install_type
    ,interval
    ,ip
    ,max_event_counter
    ,max_revenue
    ,max_time_post_install
    ,measurement_window
    ,media_source
    ,min_event_counter
    ,min_revenue
    ,min_time_post_install
    ,skad_ambiguous_event
    ,CAST(null as VARCHAR) AS skad_app_id
    ,skad_campaign_id
    ,skad_conversion_value
    ,skad_did_win
    ,skad_fidelity_type
    ,skad_redownload
    ,skad_revenue
    ,skad_source_app_id
    ,skad_transaction_id
    ,skad_version
    ,"timestamp" AS timestamp_postback
    ,user_agent 
FROM "t_skad_installs"
WHERE dt = '{{ref_date}}'
),
t_skad_inapps as (
SELECT 
    'skad_inapps' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,ad_network_adset_id AS adset_id
    ,ad_network_adset_name AS adset_name
    ,ad_network_campaign_id AS campaign_id
    ,ad_network_campaign_name AS campaign_name
    ,ad_network_channel AS channel
    ,CAST(null as VARCHAR) AS network_name
    ,ad_network_source_app_id AS source_app_id
    ,af_attribution_flag AS flag_ssot
    ,af_prt AS partner
    ,app_id
    ,city
    ,CAST(null as VARCHAR) AS  did_win
    ,event_name
    ,CAST(null as VARCHAR) AS  event_revenue
    ,event_uuid
    ,event_value
    ,CAST(null as VARCHAR) AS  fidelity_type
    ,install_date
    ,install_type
    ,interval
    ,ip
    ,max_event_counter
    ,max_revenue
    ,max_time_post_install
    ,measurement_window
    ,media_source
    ,min_event_counter
    ,min_revenue
    ,min_time_post_install
    ,skad_ambiguous_event
    ,CAST(null as VARCHAR) AS  skad_app_id
    ,skad_campaign_id
    ,skad_conversion_value
    ,skad_did_win
    ,skad_fidelity_type
    ,skad_redownload
    ,skad_revenue
    ,skad_source_app_id
    ,skad_transaction_id
    ,skad_version
    ,"timestamp" AS timestamp_postback
    ,user_agent
FROM "t_skad_inapps"
WHERE dt = '{{ref_date}}'
),
t_skad_redownloads as (
SELECT 
    'skad_redownloads' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,ad_network_adset_id AS adset_id
    ,ad_network_adset_name AS adset_name
    ,ad_network_campaign_id AS campaign_id
    ,ad_network_campaign_name AS campaign_name
    ,ad_network_channel AS channel
    ,CAST(null as VARCHAR) AS network_name
    ,ad_network_source_app_id AS source_app_id
    ,af_attribution_flag AS flag_ssot
    ,af_prt AS partner
    ,app_id
    ,city
    ,CAST(null as VARCHAR) AS did_win
    ,event_name
    ,CAST(null as VARCHAR) AS event_revenue
    ,event_uuid
    ,event_value
    ,CAST(null as VARCHAR) AS fidelity_type
    ,install_date
    ,install_type
    ,interval
    ,ip
    ,max_event_counter
    ,max_revenue
    ,max_time_post_install
    ,measurement_window
    ,media_source
    ,min_event_counter
    ,min_revenue
    ,min_time_post_install
    ,skad_ambiguous_event
    ,CAST(null as VARCHAR) AS skad_app_id
    ,skad_campaign_id
    ,skad_conversion_value
    ,skad_did_win
    ,skad_fidelity_type
    ,skad_redownload
    ,skad_revenue
    ,skad_source_app_id
    ,skad_transaction_id
    ,skad_version
    ,"timestamp" AS timestamp_postback
    ,user_agent
FROM "t_skad_redownloads"
WHERE dt = '{{ref_date}}'
),
t_skad_postbacks_copy as (
SELECT 
    'skad_postbacks_copy' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,CAST(null as VARCHAR) AS adset_id
    ,CAST(null as VARCHAR) AS adset_name
    ,CAST(null as VARCHAR) AS campaign_id
    ,CAST(null as VARCHAR) AS campaign_name
    ,CAST(null as VARCHAR) AS channel
    ,CAST(null as VARCHAR) AS network_name
    ,CAST(null as VARCHAR) AS source_app_id
    ,CAST(null as VARCHAR) AS flag_ssot
    ,CAST(null as VARCHAR) AS partner
    ,app_id
    ,CAST(null as VARCHAR) AS city
    ,did_win
    ,CAST(null as VARCHAR) AS event_name
    ,CAST(null as VARCHAR) AS event_revenue
    ,CAST(null as VARCHAR) AS event_uuid
    ,CAST(null as VARCHAR) AS event_value
    ,fidelity_type
    ,CAST(null as VARCHAR) AS install_date
    ,CAST(null as VARCHAR) AS install_type
    ,cast(null as VARCHAR) AS  interval
    ,CAST(null as VARCHAR) AS ip
    ,CAST(null as VARCHAR) AS max_event_counter
    ,CAST(null as VARCHAR) AS max_revenue
    ,CAST(null as VARCHAR) AS max_time_post_install
    ,CAST(null as VARCHAR) AS measurement_window
    ,CAST(null as VARCHAR) AS media_source
    ,CAST(null as VARCHAR) AS min_event_counter
    ,CAST(null as VARCHAR) AS min_revenue
    ,CAST(null as VARCHAR) AS min_time_post_install
    ,CAST(null as VARCHAR) AS skad_ambiguous_event
    ,skad_app_id
    ,CAST(null as VARCHAR) AS skad_campaign_id
    ,CAST(null as VARCHAR) AS skad_conversion_value
    ,CAST(null as VARCHAR) AS skad_did_win
    ,CAST(null as VARCHAR) AS skad_fidelity_type
    ,CAST(null as VARCHAR)  AS skad_redownload
    ,CAST(null as VARCHAR) AS skad_revenue
    ,CAST(null as VARCHAR) AS skad_source_app_id
    ,CAST(null as VARCHAR) AS skad_transaction_id
    ,CAST(null as VARCHAR)  AS skad_version
    ,"timestamp" AS timestamp_postback
    ,CAST(null as VARCHAR) AS user_agent  
FROM "t_skad_postbacks_copy"
WHERE dt = '{{ref_date}}'
),
t_skad_postbacks as (
SELECT 
    'skad_postbacks' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,ad_network_adset_id AS adset_id
    ,ad_network_adset_name AS adset_name
    ,ad_network_campaign_id AS campaign_id
    ,ad_network_campaign_name AS campaign_name
    ,ad_network_channel AS channel
    ,ad_network_ad_name AS network_name
    ,skad_source_app_id AS source_app_id
    ,CAST(null as VARCHAR) AS flag_ssot
    ,af_prt AS partner
    ,app_id
    ,city
    ,did_win
    ,CAST(null as VARCHAR) AS event_name
    ,CAST(null as VARCHAR) AS event_revenue
    ,CAST(null as VARCHAR) AS event_uuid
    ,CAST(null as VARCHAR) AS event_value
    ,fidelity_type
    ,CAST(null as VARCHAR) AS install_date
    ,CAST(null as VARCHAR) AS install_type
    ,CAST(null as VARCHAR)  AS interval
    ,ip
    ,CAST(null as VARCHAR) AS max_event_counter
    ,CAST(null as VARCHAR) AS max_revenue
    ,CAST(null as VARCHAR) AS max_time_post_install
    ,CAST(null as VARCHAR) AS measurement_window
    ,CAST(null as VARCHAR) AS media_source
    ,CAST(null as VARCHAR) AS min_event_counter
    ,CAST(null as VARCHAR) AS min_revenue
    ,CAST(null as VARCHAR) AS min_time_post_install
    ,CAST(null as VARCHAR) AS skad_ambiguous_event
    ,skad_app_id
    ,CAST(null as VARCHAR) AS skad_campaign_id
    ,CAST(null as VARCHAR) AS skad_conversion_value
    ,CAST(null as VARCHAR) AS skad_did_win
    ,CAST(null as VARCHAR) AS skad_fidelity_type
    ,CAST(null as VARCHAR) AS skad_redownload
    ,CAST(null as VARCHAR) AS skad_revenue
    ,CAST(null as VARCHAR) AS skad_source_app_id
    ,CAST(null as VARCHAR) AS skad_transaction_id
    ,CAST(null as VARCHAR) AS skad_version
    ,"timestamp" AS timestamp_postback
    ,CAST(null as VARCHAR) AS user_agent 
FROM "t_skad_postbacks"
WHERE dt = '{{ref_date}}'
),
t_skad_srn_advertising_dimensions as (
SELECT 
    'skad_srn_advertising_dimensions' AS skad_event_type
    ,date_format(cast("timestamp" as timestamp),'%Y-%m-%d') AS partition_date
    ,ad_network_adset_id AS adset_id
    ,ad_network_adset_name AS adset_name
    ,ad_network_campaign_id AS campaign_id
    ,ad_network_campaign_name AS campaign_name
    ,CAST(null as VARCHAR) AS channel
    ,CAST(null as VARCHAR) AS network_name
    ,CAST(null as VARCHAR) AS source_app_id
    ,CAST(null as VARCHAR) AS flag_ssot
    ,CAST(null as VARCHAR) AS partner
    ,app_id
    ,CAST(null as VARCHAR) AS city
    ,CAST(null as VARCHAR) AS did_win
    ,event_name
    ,event_revenue
    ,CAST(null as VARCHAR) AS event_uuid
    ,CAST(null as VARCHAR) AS event_value
    ,CAST(null as VARCHAR) AS fidelity_type
    ,CAST(null as VARCHAR) AS install_date
    ,CAST(null as VARCHAR) AS install_type
    ,CAST(null as VARCHAR) AS interval
    ,CAST(null as VARCHAR) AS ip
    ,CAST(null as VARCHAR) AS max_event_counter
    ,CAST(null as VARCHAR) AS max_revenue
    ,CAST(null as VARCHAR) AS max_time_post_install
    ,CAST(null as VARCHAR) AS measurement_window
    ,media_source
    ,CAST(null as VARCHAR) AS min_event_counter
    ,CAST(null as VARCHAR) AS min_revenue
    ,CAST(null as VARCHAR) AS min_time_post_install
    ,CAST(null as VARCHAR) AS skad_ambiguous_event
    ,CAST(null as VARCHAR) AS skad_app_id
    ,CAST(null as VARCHAR) skad_campaign_id
    ,CAST(null as VARCHAR) skad_conversion_value
    ,CAST(null as VARCHAR) skad_did_win
    ,skad_fidelity_type
    ,CAST(null as VARCHAR) AS skad_redownload
    ,CAST(null as VARCHAR) AS skad_revenue
    ,CAST(null as VARCHAR) AS skad_source_app_id
    ,CAST(null as VARCHAR) AS skad_transaction_id
    ,CAST(null as VARCHAR) AS skad_version
    ,"timestamp" AS timestamp_postback
    ,CAST(null as VARCHAR) AS user_agent 
FROM "t_skad_srn_advertising_dimensions"
WHERE dt = '{{ref_date}}'
)
select * from t_skad_installs
union all 
select * from t_skad_inapps
union all 
select * from t_skad_redownloads
union all 
select * from t_skad_postbacks_copy
union all 
select * from t_skad_postbacks
union all 
select * from t_skad_srn_advertising_dimensions
)
TO 's3://{{ s3_bucket }}/{{ prefix_result }}'
WITH (format = 'AVRO');