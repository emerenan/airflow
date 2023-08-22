{% set tables = params.table_list %}
UNLOAD (
with {% for table in tables %}{{table}} as (
SELECT{% if params.event_type == 'skad' %}
    date_format(cast("timestamp" as timestamp),'%Y-%m-%d') as event_date,{% else %}
    date_format(cast(event_time as timestamp),'%Y-%m-%d') as event_date,{% endif %}
    count(*) as number_rows
FROM "{{table}}"{% if params.event_type == 'skad' %}
where cast(date_format(cast("timestamp" as timestamp),'%Y-%m-%d') as date) 
    between date_add('day', -3, current_date) 
    and current_date{% else %}
where cast(date_format(cast(event_time as timestamp),'%Y-%m-%d') as date) 
    between date_add('day', -3, current_date) 
    and current_date{% endif %}
group by 1
){% if not loop.last %}, 
{% endif %}{% endfor %},
united as (
{% for table in tables %}SELECT * FROM {{table}}{% if not loop.last %}
UNION ALL 
{% endif %}{% endfor %}
)
SELECT
    event_date,
    'D-1' as extract_type,
    'athena' as database,
    '{{ params.event_type }}' as event_type,
    sum(number_rows) as number_rows
from united
group by 1
order by 1
)
TO 's3://{{ params.s3_bucket }}/{{ params.prefix_result }}'
WITH (format = 'AVRO');