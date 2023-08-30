CREATE TABLE IF NOT EXISTS {project}.{dataset}.{table_name}(
    ad_group    STRING,
    advertiser  STRING,
    campaign    STRING,
    creative    STRING,
    date    DATE,
    device_type STRING,
    clicks  INT64,
    impressions INT64,
    advertiser_cost_USD FLOAT64,
    player_completed_views  INT64,
    total_click_view_conversions1   INT64,
    total_click_view_conversions2   INT64,
    total_click_view_conversions3   INT64,
    total_click_view_conversions4   INT64,
    total_click_view_conversions_revenue1   FLOAT64
) PARTITION BY _PARTITIONDATE
OPTIONS (require_partition_filter = FALSE);