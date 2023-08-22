CREATE TABLE IF NOT EXISTS {{ dwh_project_id }}.{{ simon_dataset_id }}.{{ delivered_table }} (
id STRING,
customer_id STRING,
event_type STRING,
event_datetime TIMESTAMP,
event_date DATE,
campaign_name STRING,
canvas_name STRING,
canvas_variation_name STRING,
athena_partition STRING
)
PARTITION BY event_date
AS 
select 
    id,
    customer_id,
    event_type,
    time,
    event_date,
    campaign_name,
    canvas_name,
    canvas_variation_name,
    athena_partition
from {{ dwh_project_id }}.{{ dwh_dataset_id }}.{{ dwh_table }}
where event_date <= date_sub(current_date, INTERVAL 1 day);

MERGE {{ dwh_project_id }}.{{ simon_dataset_id }}.{{ delivered_table }} AS a 
USING (
SELECT
    id,
    customer_id,
    event_type,
    time,
    event_date,
    campaign_name,
    canvas_name,
    canvas_variation_name,
    athena_partition
FROM {{ dwh_project_id }}.{{ dwh_dataset_id }}.{{ dwh_table }}
    WHERE event_date = date_sub(current_date, INTERVAL 1 day)
)
AS b 
ON a.id = b.id
WHEN NOT MATCHED THEN 
INSERT(
    id,
    customer_id,
    event_type,
    event_datetime,
    event_date,
    campaign_name,
    canvas_name,
    canvas_variation_name,
    athena_partition
    )
VALUES(
    id,
    customer_id,
    event_type,
    time,
    event_date,
    campaign_name,
    canvas_name,
    canvas_variation_name,
    athena_partition
    );