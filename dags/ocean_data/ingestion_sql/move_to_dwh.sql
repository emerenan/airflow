INSERT INTO `{dwh_project}.{dwh_dataset}.{dwh_table_name}`(
    ad_group,
    advertiser,
    campaign,
    creative,
    date,
    device_type,
    clicks,
    impressions,
    advertiser_cost_USD,
    player_completed_views,
    total_click_view_conversions1,
    total_click_view_conversions2,
    total_click_view_conversions3,
    total_click_view_conversions4,
    total_click_view_conversions_revenue1
)
SELECT *
FROM `{dl_project}.{dl_dataset}.{dl_table_name}`
WHERE DATE(_PARTITIONTIME) = DATE("{partition_date}")