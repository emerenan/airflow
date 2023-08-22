MERGE {{project_id}}.logging.config_table AS a
USING ( select to_hex(md5('{{event_name}}'||'{{ref_date}}')) as id ) AS b 
ON a.id = b.id
WHEN NOT MATCHED THEN
  insert(id, event_name, dt_init, dt_time_init, dt_end, dt_time_end, is_processed, number_row)
  values(
    to_hex(md5('{{event_name}}'||'{{ref_date}}')),
    '{{event_name}}',
    cast('{{ref_date}}' as DATE),
    cast('{{ref_datetime}}' as DATETIME),
    CAST('1900-01-01' AS DATE),
    CAST('1900-01-01 00:00:00' AS DATETIME),
    false,
    0
  )
WHEN MATCHED THEN 
  UPDATE SET dt_time_init = '{{ref_datetime}}'
