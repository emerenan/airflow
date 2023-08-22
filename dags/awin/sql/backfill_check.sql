DECLARE event STRING DEFAULT '{{event_type}}';

with cte as (
select 
  min(dt_init) as ref_date
from {{dwh_project}}.logging.config_table
  where event_name=event 
  and  is_processed = false
  and dt_init < current_date
)
select 
  dt_init
from {{dwh_project}}.logging.config_table
  where event_name=event 
  and  is_processed = false
  and ( dt_init < current_date and dt_init >= (select ref_date from cte))
order by 1
limit 1;