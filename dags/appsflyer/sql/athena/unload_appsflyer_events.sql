UNLOAD (
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_add_payment_info"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL 
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone
    ,contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_add_to_wishlist"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL 
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM  "event_type_af_complete_registration"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL 
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_initiated_checkout"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_login"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_purchase"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_travel_booking"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
FROM "event_type_re_attribution"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_re_engagement"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_share"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
UNION ALL
SELECT 
    idfv
    ,device_category /*,store_product_page
    ,af_sub1
    ,af_sub2
    ,af_sub3
    ,af_sub4
    ,af_sub5*/
    ,customer_user_id /*,is_lat
    ,bundle_id
    ,gp_broadcast_referrer*/,contributor_1_touch_time
    ,contributor_2_touch_time
    ,contributor_3_touch_time
    ,contributor_1_touch_type
    ,contributor_2_touch_type
    ,contributor_3_touch_type
    ,contributor_1_match_type
    ,contributor_2_match_type
    ,contributor_3_match_type
    ,event_source /*,af_cost_value*/
    ,app_version
    ,custom_data /*,gp_install_begin*/
    ,city /*,amazon_aid*/
    ,device_model /*,gp_referrer
    ,af_cost_model*/
    ,af_c_id
    ,attributed_touch_time_selected_timezone
    ,selected_currency /*,app_name*/
    ,install_time_selected_timezone /*,postal_code*/
    ,wifi
    ,install_time /*,operator*/
    ,attributed_touch_type
    ,af_attribution_lookback
    ,campaign_type
    ,keyword_match_type
    ,af_adset_id
    ,device_download_time_selected_timezone, contributor_1_media_source
    ,contributor_2_media_source
    ,contributor_3_media_source
    ,conversion_type
    ,api_version
    ,attributed_touch_time
    ,revenue_in_selected_currency
    ,is_retargeting
    ,country_code /*,gp_click_time*/
    ,match_type
    ,appsflyer_id /*,dma*/
    ,http_referrer /*,af_prt*/
    ,contributor_1_af_prt
    ,contributor_2_af_prt  
    ,contributor_3_af_prt   /*,event_revenue_currency*/
    ,store_reinstall /*,install_app_store*/
    ,media_source
    ,deeplink_url
    ,campaign
    ,af_keywords
    ,region /*,cost_in_selected_currency*/
    ,event_value
    ,ip /*,oaid*/
    ,event_time /*,is_receipt_validated*/
    ,contributor_1_campaign
    ,contributor_2_campaign
    ,contributor_3_campaign /*,imei*/
    ,event_revenue_usd /*,original_url*/
    ,android_id
    ,att
    ,af_adset
    ,af_ad
    ,state /*,network_account_id
    ,device_type*/
    ,idfa
    ,retargeting_conversion_type
    ,af_channel
    ,af_cost_currency /*,custom_dimension
    ,keyword_id*/
    ,device_download_time
    ,af_reengagement_window /*,app_type*/
    ,af_siteid
    ,language
    ,app_id /*,event_revenue */
    ,af_ad_type /*,carrier*/
    ,event_name /*,af_sub_siteid*/
    ,advertising_id
    ,os_version
    ,platform
    ,selected_timezone
    ,af_ad_id
    ,user_agent
    ,is_primary_attribution
    ,sdk_version
    ,event_time_selected_timezone 
    
FROM "event_type_af_content_view"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
)
TO 's3://{{ s3_bucket }}/{{ prefix_result }}'
WITH (format = 'AVRO');