EXPORT DATA OPTIONS(
  uri='{destination_cloud_storage_uris}',
  format='CSV',
  overwrite=true,
  header=true,
  field_delimiter=',') AS

SELECT * EXCEPT(year, week_number)
FROM `{project}.{dataset}.{table_name}`
WHERE year={year} AND week_number={week_number}