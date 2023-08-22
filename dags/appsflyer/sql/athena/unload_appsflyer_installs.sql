UNLOAD (
SELECT 
    DISTINCT * 
FROM "event_type_install"
WHERE "date" = date_format(cast('{{ref_date}}' as date), '%Y%m%d')
)
TO 's3://{{ s3_bucket }}/{{ prefix_result }}'
WITH (format = 'AVRO');