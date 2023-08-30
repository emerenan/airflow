CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.{table_name}`
(
  year INT64,
  week_number INT64,
  Date DATE,
  AnalyticsType STRING,
  repeatbrowsersends INT64,
  newbrowsersends INT64,
  repeatbookersends INT64,
  newbookersends INT64,
  repeatbrowseropens INT64,
  newbrowseropens INT64,
  repeatbookeropens INT64,
  newbookeropens INT64,
  repeatbrowserclicks INT64,
  newbrowserclicks INT64,
  repeatbookerclicks INT64,
  newbookerclicks INT64,
  totalusers INT64
) PARTITION BY DATE_TRUNC(Date, YEAR)