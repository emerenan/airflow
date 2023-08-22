{% macro get_dt_range(dt_range) -%}
{% set dates_range = ({'today': 'current_date',
    'd-1': 'date_add(current_date, INTERVAL -1 DAY)',
    'd-2': 'date_add(current_date, INTERVAL -2 DAY)'}) %} {{ dates_range[dt_range] }} {% endmacro %}
/* 
The SQL starts below
*/
SELECT
    dt_init as dates
from 
    {{project_id}}.logging.config_table
WHERE event_name = '{{event_type}}'
and is_processed = false
and dt_init <> {{get_dt_range(dt_filter)}}
limit 1;