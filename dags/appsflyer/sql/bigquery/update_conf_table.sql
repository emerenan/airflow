UPDATE {{project_id}}.logging.config_table
SET dt_end = cast('{{ref_date}}' as DATE),
    dt_time_end = cast('{{ref_datetime}}' as DATETIME),
    is_processed = true,
    number_row = 0
WHERE id = to_hex(md5('{{event_name}}'||'{{ref_date}}'))