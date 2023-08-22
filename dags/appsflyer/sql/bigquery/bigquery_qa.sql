SELECT
    TO_HEX(md5(lower(partition_date||'dwh'||'{{ table_event }}'))) as id,
    partition_date as event_date,
    'D-1' as extract_type,
    'dwh' as database,
    '{{ table_event }}' AS event_type,
    count(*) as number_rows
FROM {{ project_id }}.viator_appsflyer.viator_appsflyer_{{table_event}}{{test}}
WHERE partition_date between date_add(CURRENT_DATE, INTERVAL -7 DAY) and current_date
group by 1, 2
order by 2;