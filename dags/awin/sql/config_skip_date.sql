INSERT INTO `{project}.{dataset}.{table_name}`
SELECT
  '{dag_id}' as id,
  '{topic}' as event_name,
  DATE('{processing_date}') as dt_init,
  NULL as dt_time_init,
  DATE('{processing_date}') as dt_end,
  NULL as dt_time_end,
  False as is_processed,
  0 as number_row