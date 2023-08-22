with cte as (
SELECT 
    dt_init, 
    dt_time_init
FROM {{project_id}}.logging.config_table 
where event_name = '{{event_type}}'
and is_processed = false
and dt_init < current_date
)
select
    dt_init
from cte 
group by 1
having count(dt_time_init) > 12
limit 1;