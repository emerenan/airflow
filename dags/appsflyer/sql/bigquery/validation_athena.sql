select 
    TO_HEX(md5(lower(event_date||database||event_type))) as id,
    cast(event_date as date) as event_date,
    extract_type,
    database,
    event_type,
    number_rows
from {{ project_id }}.data_landing.viator_appsflyer_{{ table_id }};