CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table_name}` (
    campaign	STRING,
    publisher_id	INT64,
    publisher_name	STRING,
    commission_amount_approved	FLOAT64,
    commission_amount_bonus	FLOAT64,
    commission_amount_declined	FLOAT64,
    commission_amount_pending	FLOAT64,
    commission_amount_total	FLOAT64,
    quantity_approved	INT64,
    quantity_bonus	INT64,
    quantity_click	INT64,
    quantity_declined	INT64,
    quantity_pending	INT64,
    quantity_total	INT64,
    sale_amount_approved	FLOAT64,
    sale_amount_bonus	FLOAT64,
    sale_amount_declined	FLOAT64,
    sale_amount_pending	FLOAT64,
    sale_amount_total	FLOAT64,
    report_date	DATE,
)
PARTITION BY report_date
OPTIONS (require_partition_filter = FALSE);