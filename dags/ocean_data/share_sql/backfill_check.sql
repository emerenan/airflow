with date_range as (
  SELECT
  extract(year from day) as year,
  extract(week from day) as week_number,
  min(day) as week_start,
  max(day) as week_end
  FROM (
  SELECT
    day
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2023-01-01'), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 DAY)
  ) AS day)
  GROUP BY year, week_number
)
select
  min(dr.year) as year,
  min(dr.week_number) as week_number,
  min(dr.week_start) as week_start,
  min(dr.week_end) as week_end
  from date_range dr
LEFT JOIN `{project}.{dataset}.{table_name}` source
  ON dr.year = source.year
  AND dr.week_number = source.week_number
WHERE source.year is null
AND   source.week_number is null