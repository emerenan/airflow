LOAD DATA OVERWRITE `{{ dwh_project_id }}.{{ dataset_id }}.{{ table_id }}`
FROM
FILES (uris = ['{{ s3_uri }}/*'],
    format = 'AVRO')
WITH CONNECTION `{{ gcp_conn }}`;