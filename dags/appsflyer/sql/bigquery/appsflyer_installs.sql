SELECT  
    DATE(cast(event_time AS TIMESTAMP)) AS partition_date
    ,CAST(install_time AS TIMESTAMP) AS install_time
    ,CAST(event_time AS TIMESTAMP) AS event_time
    ,event_name
    ,CASE WHEN UPPER(media_source) LIKE '%ORGANIC'
                THEN 'Organic' 
        WHEN media_source is null THEN 'Organic' 
        ELSE 'Non-Organic' END AS source
    ,media_source
    ,campaign AS campaign_name
    ,campaign_type
    ,af_c_id AS campaign_id
    ,appsflyer_id
    ,customer_user_id AS user_id
    ,REPLACE(json_extract(custom_data, '$.device_id'),'"','') AS device_id
    ,platform
    ,device_category AS device
    ,os_version
    ,device_model
    ,app_version
    ,country_code
    ,region
    ,state
    ,city
    ,ip
    ,language
    ,af_keywords AS keywords
    ,advertising_id
    ,af_ad AS ad
    ,af_ad_id AS ad_id
    ,af_ad_type AS ad_type
    ,af_adset AS adset
    ,af_adset_id AS adset_id
    ,af_channel AS channel
    ,af_attribution_lookback AS attribution_lookback
    ,is_retargeting
    ,af_reengagement_window AS reengagement_window
    ,deeplink_url
    ,http_referrer
    ,att
    ,CAST(attributed_touch_time AS DATETIME) attributed_touch_time
    ,CAST(substring(attributed_touch_time_selected_timezone, 0, length(attributed_touch_time_selected_timezone)-5) AS DATETIME) AS attributed_touch_time_selected_timezone
    ,attributed_touch_type
    ,is_primary_attribution
    ,contributor_1_af_prt
    ,contributor_1_campaign
    ,contributor_1_match_type
    ,contributor_1_media_source
    ,CAST(contributor_1_touch_time AS DATETIME) AS contributor_1_touch_time
    ,contributor_1_touch_type
    ,contributor_2_af_prt
    ,contributor_2_campaign
    ,contributor_2_match_type
    ,contributor_2_media_source
    ,CAST(contributor_2_touch_time AS DATETIME) AS contributor_2_touch_time
    ,contributor_2_touch_type
    ,contributor_3_af_prt
    ,contributor_3_campaign
    ,contributor_3_match_type
    ,contributor_3_media_source
    ,CAST(contributor_3_touch_time AS DATETIME) AS contributor_3_touch_time
    ,contributor_3_touch_type
    ,wifi
    ,conversion_type
    ,event_source
    ,sdk_version
    ,api_version
    ,user_agent
    ,app_id
    ,CAST(device_download_time AS DATETIME) AS device_download_time
    ,CAST(SUBSTRING(device_download_time_selected_timezone, 0, length(device_download_time_selected_timezone)-5) AS DATETIME) AS device_download_time_selected_timezone
    ,CAST(SUBSTRING(install_time_selected_timezone, 0, length(install_time_selected_timezone)-5) AS DATETIME) AS install_time_selected_timezone
    ,CAST(event_time_selected_timezone AS TIMESTAMP) event_time_selected_timezone
    ,match_type
    ,keyword_match_type
    ,store_reinstall
    ,idfa
    ,idfv
    ,android_id
    ,af_siteid AS siteid
    ,retargeting_conversion_type
    ,selected_currency
    ,selected_timezone
FROM {{ project_id }}.data_landing.{{table_id}};