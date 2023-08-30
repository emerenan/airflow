CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.{table_name}`
(
  year INT64,
  week_number INT64,
  Date DATE,
  BookerType STRING,
  BookerCountCount INT64,
  BookingCount INT64,
  Revenue FLOAT64
) PARTITION BY DATE_TRUNC(Date, YEAR)