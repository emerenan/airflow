SELECT 
    DATE(cast(event_time as TIMESTAMP)) AS partition_date
    ,CAST(install_time as  TIMESTAMP) AS install_time
    ,CAST(event_time as  TIMESTAMP) AS event_time
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
    ,REPLACE(json_extract(event_value, '$.af_city'),'"','') AS event_city
    ,ip
    ,language
    ,CAST(REPLACE(json_extract(event_value, '$.af_order_id'),'"','') AS NUMERIC) AS event_order_id
    ,REPLACE(json_extract(event_value, '$.af_content_id'),'"','') AS event_product_id
    ,REPLACE(json_extract(event_value, '$.af_content_type'),'"','') AS event_content_type
    ,REPLACE(json_extract(event_value, '$.af_country'),'"','') AS event_destination_country
    ,REPLACE(json_extract(event_value, '$.af_currency'),'"','') AS event_currency
    ,CASE WHEN REPLACE(json_extract(event_value, '$.af_price'),'"','')=''
            THEN 0
        ELSE cast(REPLACE(json_extract(event_value, '$.af_price'),'"','') AS NUMERIC) END AS event_price
    ,event_revenue_usd
    ,CAST(REPLACE(json_extract(event_value, '$.af_revenue'),'"','') AS NUMERIC) as event_revenue
    ,revenue_in_selected_currency AS revenue_selected_currency
    ,REPLACE(json_extract(event_value, '$.af_success'),'"','') AS event_success
    ,CASE WHEN cast(REPLACE(json_extract(event_value, '$.adult'),'"','') AS NUMERIC) IS NOT NULL
            THEN CAST(REPLACE(json_extract(event_value, '$.adult'),'"','') AS NUMERIC)
        ELSE CAST(REPLACE(json_extract(event_value, '$.af_num_adults'),'"','') AS NUMERIC) END AS event_num_adults
    ,CASE WHEN CAST(REPLACE(json_extract(event_value, '$.child'),'"','') AS NUMERIC) IS NOT NULL
            THEN CAST(REPLACE(json_extract(event_value, '$.child'),'"','') AS NUMERIC)
        ELSE CAST(REPLACE(json_extract(event_value, '$.af_num_children'),'"','') AS NUMERIC) END AS event_num_children
    ,CASE WHEN CAST(REPLACE(json_extract(event_value, '$.infant'),'"','') AS NUMERIC) IS NOT NULL
            THEN CAST(REPLACE(json_extract(event_value, '$.infant'),'"','') AS NUMERIC)
        ELSE CAST(REPLACE(json_extract(event_value, '$.af_num_infants'),'"','') AS NUMERIC) END AS event_num_infants
    ,REPLACE(json_extract(event_value, '$.af_registration_method'),'"','') AS event_registration_method
    ,CASE WHEN (CAST(REPLACE(app_version, '.','') AS INT64)>= 930 
                AND CAST(REPLACE(app_version, '.','') AS INT64)< 940) 
                AND SAFE_CAST(json_extract_scalar(event_value, '$.af_date_a') AS DATE) IS NULL 
                AND REGEXP_CONTAINS(LEFT(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''),1), r"[a-zA-Z]+$")=TRUE
            THEN REPLACE(json_extract(event_value, '$.af_date_a'),'"','')
        WHEN (CAST(REPLACE(app_version, '.','') AS INT64)>= 930 
                AND CAST(REPLACE(app_version, '.','') AS INT64)< 940) 
                AND SAFE_CAST(json_extract_scalar(event_value, '$.af_date_a') AS DATE) IS NULL 
            THEN CAST(TIMESTAMP_MILLIS(cast(REPLACE(json_extract_scalar(event_value, '$.af_date_a'),'.','') AS INT64)) AS STRING) 
        WHEN (CAST(REPLACE(app_version, '.','') AS INT64)>= 930 
                AND CAST(REPLACE(app_version, '.','') AS INT64)< 940) 
                AND SAFE_CAST(json_extract_scalar(event_value, '$.af_date_a') AS DATE) IS NOT NULL 
            THEN REPLACE(json_extract(event_value, '$.af_date_a'),'"','')
        WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101 
                AND REGEXP_CONTAINS(LEFT(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''),1), r"[a-zA-Z]+$")=TRUE
            THEN REPLACE(json_extract(event_value, '$.af_date_a'),'"','')
        WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6101
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6102
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6103
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6111
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6112
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6113
                AND STRPOS(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''), '.') - 1 = -1
            THEN CAST(TIMESTAMP_MILLIS(cast(REPLACE(json_extract_scalar(event_value, '$.af_date_a'),'.','') AS INT64)) AS STRING) 
        WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6101
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6102
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6103
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6111
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6112
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6113
            THEN CAST(TIMESTAMP_SECONDS(CAST(SUBSTR(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''), (LENGTH(REPLACE(json_extract(event_value, '$.af_date_a'),'"','')) - STRPOS(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''), '.')),(STRPOS(REPLACE(json_extract(event_value, '$.af_date_a'),'"',''), '.') - 1))AS INT64)) AS STRING)
        ELSE REPLACE(json_extract(event_value, '$.af_date_a'),'"','')  END as event_travel_date 
    ,CASE WHEN (CAST(REPLACE(app_version, '.','') AS INT64)>= 930 
                AND CAST(REPLACE(app_version, '.','') AS INT64)< 940) 
                AND SAFE_CAST(json_extract_scalar(event_value, '$.af_travel_start') AS DATE) IS NULL 
            THEN DATE(TIMESTAMP_MILLIS(cast(REPLACE(json_extract_scalar(event_value, '$.af_travel_start'),'.','') AS INT64)))
        WHEN (CAST(REPLACE(app_version, '.','') AS INT64)>= 930 
                AND CAST(REPLACE(app_version, '.','') AS INT64)< 940) 
                AND SAFE_CAST(json_extract_scalar(event_value, '$.af_travel_start') AS DATE) IS NOT NULL 
            THEN CAST(REPLACE(json_extract(event_value, '$.af_travel_start'),'"','') AS DATE)
        WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101 
                AND STRPOS(REPLACE(json_extract(event_value, '$.af_travel_start'),'"',''), '.') - 1 = -1
            THEN DATE(TIMESTAMP_MILLIS(cast(REPLACE(json_extract_scalar(event_value, '$.af_travel_start'),'.','') AS INT64)))
        WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101
            THEN  CAST(TIMESTAMP_SECONDS(CAST(SUBSTR(REPLACE(json_extract(event_value, '$.af_travel_start'),'"',''), (LENGTH(REPLACE(json_extract(event_value, '$.af_travel_start'),'"','')) - STRPOS(REPLACE(json_extract(event_value, '$.af_travel_start'),'"',''), '.')),(STRPOS(REPLACE(json_extract(event_value, '$.af_travel_start'),'"',''), '.') - 1))AS INT64)) AS DATE)
        ELSE CAST(REPLACE(json_extract(event_value, '$.af_travel_start'),'"','') AS DATE) END as event_travel_start
    ,CASE WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101
            THEN DATE(TIMESTAMP_SECONDS(CAST(SUBSTR(REPLACE(json_extract(event_value, '$.af_travel_end'),'"',''), (LENGTH(REPLACE(json_extract(event_value, '$.af_travel_end'),'"','')) - STRPOS(REPLACE(json_extract(event_value, '$.af_travel_end'),'"',''), '.')),(STRPOS(REPLACE(json_extract(event_value, '$.af_travel_end'),'"',''), '.') - 1))AS INT64)))
        ELSE DATE(REPLACE(json_extract(event_value, '$.af_travel_end'),'"','')) END as event_travel_end
    ,CASE WHEN CAST(REPLACE(app_version, '.','') AS INT64)>= 940 
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 8101
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6101
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6102
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6103
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6111
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6112
                AND CAST(REPLACE(app_version, '.','') AS INT64)<> 6113
            THEN TIMESTAMP(CAST(TIMESTAMP_SECONDS(CAST(SUBSTR(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''), (LENGTH(REPLACE(json_extract(event_value, '$.af_date_b'),'"','')) - STRPOS(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''), '.')),(STRPOS(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''), '.') - 1))AS INT64)) AS STRING))
            WHEN REGEXP_CONTAINS(LEFT(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),1), r"[a-zA-Z]+$")=TRUE
            THEN TIMESTAMP(DATETIME(CAST(RIGHT(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),4) AS INT64),
            EXTRACT(month from parse_date('%b', SUBSTRING(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),4,4))),
            CAST(SUBSTRING(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),8,3) AS INT64),
            CAST(SUBSTRING(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),11,3) AS INT64),
            CAST(SUBSTRING(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),15,2) AS INT64),
            CAST(SUBSTRING(REPLACE(json_extract(event_value, '$.af_date_b'),'"',''),18,2)AS INT64)))
        ELSE timestamp(REPLACE(json_extract(event_value, '$.af_date_b'),'"','')) END as event_date_b
    ,REPLACE(json_extract(event_value, '$.auto_login'),'"','') AS event_auto_login
    ,REPLACE(json_extract(event_value, '$.reason'),'"','') AS event_reason
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
    ,CAST(SUBSTRING(attributed_touch_time_selected_timezone, 0, length(attributed_touch_time_selected_timezone)-5) AS DATETIME) AS attributed_touch_time_selected_timezone
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
    ,CAST(substring(device_download_time_selected_timezone, 0, length(device_download_time_selected_timezone)-5) AS DATETIME) AS device_download_time_selected_timezone
    ,CAST(substring(install_time_selected_timezone, 0, length(install_time_selected_timezone)-5) AS DATETIME) AS install_time_selected_timezone
    ,CAST(event_time_selected_timezone AS TIMESTAMP) event_time_selected_timezone
    ,match_type
    ,keyword_match_type
    ,store_reinstall
    ,idfa
    ,idfv
    ,android_id
    ,af_siteid AS siteid
    ,retargeting_conversion_type
    ,af_cost_currency AS cost_currency_code
    ,selected_currency
    ,selected_timezone
FROM {{ project_id }}.data_landing.{{ table_id }}