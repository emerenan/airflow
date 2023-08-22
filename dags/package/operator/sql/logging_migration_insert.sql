WITH
              legacy AS(
              SELECT
                COALESCE(ordinal_position, 0) AS ordinal_position,
                column_name AS legacy_columns,
                data_type AS legacy_datatypes,
                is_nullable AS legacy_is_nullable,
                is_hidden AS legacy_is_hidden,
                is_partitioning_column AS legacy_is_partitioning_column
              FROM
                {legacy_project}.{legacy_dataset}.INFORMATION_SCHEMA.COLUMNS
              WHERE
                table_name = '{legacy_tablename}' ),
              migrated AS(
              SELECT
                COALESCE(ordinal_position, 0) AS ordinal_position,
                column_name AS migrated_columns,
                data_type AS migrated_datatypes,
                is_nullable AS migrated_is_nullable,
                is_hidden AS migrated_is_hidden,
                is_partitioning_column AS migrated_is_partitioning_column
              FROM
                {migrated_project}.{migrated_dataset}.INFORMATION_SCHEMA.COLUMNS
              WHERE
                table_name = '{migrated_tablename}' ),  get_errors AS (
                  SELECT
                    legacy.ordinal_position AS ordinal_position,
                    legacy_columns,
                    migrated_columns,
                    CASE
                      WHEN legacy_columns != migrated_columns THEN STRUCT ('COLUMN_NAME' AS error_type, legacy_columns, migrated_columns )
                      WHEN legacy_datatypes != migrated_datatypes THEN STRUCT ('DATATYPE' AS error_type,
                      legacy_datatypes,
                      migrated_datatypes )
                      WHEN legacy_is_nullable != migrated_is_nullable THEN STRUCT ('NULLABLE' AS error_type, legacy_is_nullable, migrated_is_nullable )
                      WHEN legacy_is_hidden != migrated_is_hidden THEN STRUCT ('HIDDEN' AS error_type,
                      legacy_is_hidden,
                      migrated_is_hidden )
                      WHEN legacy_is_partitioning_column != migrated_is_partitioning_column THEN STRUCT ('PARTITION' AS error_type, legacy_is_partitioning_column, migrated_is_partitioning_column )
                    ELSE
                    NULL
                  END
                    AS error_type
                  FROM
                    legacy
                  FULL OUTER JOIN
                    migrated
                  ON
                    legacy.ordinal_position = migrated.ordinal_position )
                SELECT
                  '{context['dag'].dag_id}' AS dag_name,
                  {dag_execution_ts} AS execution_timestamp,
                  '{legacy_table}' AS legacy_table,
                  '{migrated_table}' AS migrated_table,
                  SAFE_CAST(NULL AS DATE) AS legacy_min_partition,
                  SAFE_CAST(NULL AS DATE) AS legacy_max_partition,
                  SAFE_CAST(NULL AS DATE) AS migrated_min_partition,
                  SAFE_CAST(NULL AS DATE) AS migrated_max_partition,
                  SAFE_CAST(NULL AS TIMESTAMP) AS legacy_min_legacy_ts_column,
                  SAFE_CAST(NULL AS TIMESTAMP) AS legacy_max_legacy_ts_column,
                  SAFE_CAST(NULL AS TIMESTAMP) AS migrated_min_migrated_ts_column,
                  SAFE_CAST(NULL AS TIMESTAMP) AS migrated_max_migrated_ts_column,
                  get_errors.ordinal_position,
                  legacy_columns AS column_name_legacy,
                  SAFE_CAST(NULL AS INT64) AS legacy_distinct_values,
                  SAFE_CAST(NULL AS INT64) AS migrated_distinct_values,
                  SAFE_CAST(NULL AS FLOAT64) AS migrated_over_legacy_percent,
                  get_errors.*EXCEPT( ordinal_position,
                    legacy_columns,
                    migrated_columns)
                FROM
                  get_errors
                  ORDER BY 2 desc, 1