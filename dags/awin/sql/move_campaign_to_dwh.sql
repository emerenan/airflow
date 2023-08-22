INSERT INTO `{project_dwh}.{dataset_dwh}.{table_name_dwh}`
SELECT
    campaign,
    CAST(publisher_id as INT64) as publisher_id,
    publisher_name,
    CAST(commission_amount_approved as FLOAT64) as commission_amount_approved,
    CAST(commission_amount_bonus as FLOAT64) as commission_amount_bonus,
    CAST(commission_amount_declined as FLOAT64) as commission_amount_declined,
    CAST(commission_amount_pending as FLOAT64) as commission_amount_pending,
    CAST(commission_amount_total as FLOAT64) as commission_amount_total,
    CAST(quantity_approved as INT64) as quantity_approved,
    CAST(quantity_bonus as INT64) as quantity_bonus,
    CAST(quantity_click as INT64) as quantity_click,
    CAST(quantity_declined as INT64) as quantity_declined,
    CAST(quantity_pending as INT64) as quantity_pending,
    CAST(quantity_total as INT64) as quantity_total,
    CAST(sale_amount_approved as FLOAT64) as sale_amount_approved,
    CAST(sale_amount_bonus as FLOAT64) as sale_amount_bonus,
    CAST(sale_amount_declined as FLOAT64) as sale_amount_declined,
    CAST(sale_amount_pending as FLOAT64) as sale_amount_pending,
    CAST(sale_amount_total as FLOAT64) as sale_amount_total,
    CAST(report_date as DATE) as report_date
FROM `{project_dl}.{dataset_dl}.{table_name_dl}`
WHERE report_date = '{start_date}';