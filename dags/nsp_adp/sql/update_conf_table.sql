DECLARE number_execution int64 default (
  select dag_executions from {{project_id}}.logging.config_table
  WHERE id = to_hex(md5('{{event_name}}'||'{{ref_date}}'))
);

UPDATE {{project_id}}.logging.config_table
SET dt_end = cast('{{ref_date}}' as DATE),
    dt_time_end = cast('{{ref_datetime}}' as DATETIME),
    is_processed = true,
    number_row = 0
    dag_executions = number_execution + 1
WHERE id = to_hex(md5('{{event_name}}'||'{{ref_date}}'))