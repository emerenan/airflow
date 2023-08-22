LOAD DATA INTO `{{ project_id }}.data_landing.{{ table_id }}`
FROM
FILES (uris = ['{{ s3_uri }}/*'],
    format = 'AVRO')
WITH CONNECTION `{{ gcp_conn }}`;