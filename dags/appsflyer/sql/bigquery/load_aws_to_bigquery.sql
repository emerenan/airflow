LOAD DATA OVERWRITE `{{ project_id }}.data_landing.viator_{{ table_id }}`
FROM
FILES (uris = ['{{ s3_uri }}/*'],
    format = 'AVRO')
WITH CONNECTION `{{ gcp_conn }}`;