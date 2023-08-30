CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.{table_name}`
(
  year INT64,
  week_number INT64,
  Date DATE,
  BookerType STRING,
  AppType STRING,
  BookerCountCount INT64,
  Bookings INT64,
  Revenue FLOAT64
) PARTITION BY DATE_TRUNC(Date, YEAR)