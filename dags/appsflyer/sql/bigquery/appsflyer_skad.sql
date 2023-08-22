SELECT 
skad_event_type
,cast(partition_date as date) as partition_date
,adset_id
,adset_name
,campaign_id
,campaign_name
,channel
,network_name
,source_app_id
,flag_ssot
,partner
,app_id
,city
,did_win
,event_name
,event_revenue
,event_uuid
,event_value
,fidelity_type
,install_date
,install_type
,`interval`
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
,skad_app_id
,skad_campaign_id
,skad_conversion_value
,skad_did_win
,skad_fidelity_type
,skad_redownload
,skad_revenue
,skad_source_app_id
,skad_transaction_id
,skad_version
,timestamp_postback
,user_agent
FROM {{ project_id}}.data_landing.{{ table_id }};