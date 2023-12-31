CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table_name}` (
    advertiser_id    INT64,
    advertiser_name  STRING,
    publisher_id     INT64,
    publisher_name   STRING,
    region          STRING,
    currency        STRING,
    impressions     INT64,
    clicks          INT64,
    creative_id      INT64,
    creative_name    STRING,
    tag_name         STRING,
    pending_no       INT64,
    pending_value    FLOAT64,
    pending_comm     FLOAT64,
    confirmed_no     INT64,
    confirmed_value  FLOAT64,
    confirmed_comm   FLOAT64,
    bonus_no         INT64,
    bonus_value      FLOAT64,
    bonus_comm       FLOAT64,
    total_no         INT64,
    total_value      FLOAT64,
    total_comm       FLOAT64,
    declined_no      INT64,
    declined_value   FLOAT64,
    declined_comm    FLOAT64,
    tags             STRING,
    report_date	     DATE
)
PARTITION BY report_date
OPTIONS (require_partition_filter = FALSE);