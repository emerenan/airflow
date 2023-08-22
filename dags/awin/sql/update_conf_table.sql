MERGE {{project_id}}.logging.config_table AS a
USING ( select to_hex(md5('{{event_name}}'||'{{ref_date}}')) as id ) AS b 
ON a.id = b.id
WHEN MATCHED THEN UPDATE
  SET dt_end = cast('{{ref_date}}' as DATE),
    dt_time_end = cast('{{ref_datetime}}' as DATETIME),
    is_processed = true,
    number_row = {{qtd_rows}}