{% macro get_dt_range(time_range) %}
{% set ranges = ({
    '2022': "between cast('2022-10-01' as date) and cast('2022-12-31' as date)",
    '2023jan': "between cast('2023-01-01' as date) and cast('2023-02-28' as date)",
    '2023march': "between cast('2023-03-01' as date) and current_date",
    'daily': "between date_add('day', -1, current_date) and current_date"})%}{{ ranges[time_range] }}{% endmacro %}

UNLOAD (
select * from {{ params.event }} 
{% if params.filter == 'time' %}WHERE cast(from_unixtime("time") as date) {{ get_dt_range(params.etl_type) }}   
{% else %}WHERE cast(SUBSTR("date", 1, 10) as date) {{ get_dt_range(params.etl_type) }}{% endif %}
)
TO 's3://{{ params.s3_bucket }}/{{ params.prefix_result }}'
WITH (format = 'AVRO');