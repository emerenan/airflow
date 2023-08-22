MERGE
    viator-data-warehouse-dev.logging.migration_general_check T
  USING
    (
    
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
                  table_name = '{migrated_tablename}' ),
                get_errors AS(
                SELECT
                  legacy.ordinal_position AS ordinal_position,
                  legacy_columns,
                  migrated_columns,
                  CASE
                    WHEN legacy_columns != migrated_columns THEN STRUCT('COLUMN_NAME' AS error_type, legacy_columns, migrated_columns )
                    WHEN legacy_datatypes != migrated_datatypes THEN STRUCT('DATATYPE' AS error_type,
                    legacy_datatypes,
                    migrated_datatypes )
                    WHEN legacy_is_nullable != migrated_is_nullable THEN STRUCT('NULLABLE' AS error_type, legacy_is_nullable, migrated_is_nullable )
                    WHEN legacy_is_hidden != migrated_is_hidden THEN STRUCT('HIDDEN' AS error_type,
                    legacy_is_hidden,
                    migrated_is_hidden )
                    WHEN legacy_is_partitioning_column != migrated_is_partitioning_column THEN STRUCT('PARTITION' AS error_type, legacy_is_partitioning_column, migrated_is_partitioning_column )
                  ELSE
                  NULL
                END
                  AS error_type
                FROM
                  legacy
                FULL OUTER JOIN
                  migrated
                ON
                  legacy.ordinal_position = migrated.ordinal_position),
                legacy_count AS(
                SELECT
                  '{legacy_columns[i]}' AS column_name,
                  COUNT(DISTINCT({legacy_columns[i]})) AS count_distinct,
                  MIN(_PARTITIONDATE) AS min_partition,
                  MAX(_PARTITIONDATE) AS max_partition,
                  MIN(SAFE_CAST({legacy_ts_column} AS TIMESTAMP)) AS min_legacy_ts_column,
                  MAX(SAFE_CAST({legacy_ts_column} AS TIMESTAMP)) AS max_legacy_ts_column,
                FROM
                  `{legacy_project}.{legacy_dataset}.{legacy_tablename}`
                WHERE
                  _PARTITIONDATE > DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
                  AND SAFE_CAST({legacy_ts_column} AS TIMESTAMP) BETWEEN {PARTITION_FROM}
                  AND {PARTITION_TO} ),
                migrated_count AS(
                SELECT
                  '{migrated_columns[i]}' AS column_name,
                  COUNT(DISTINCT({migrated_columns[i]})) AS count_distinct,
                  MIN(_PARTITIONDATE) AS min_partition,
                  MAX(_PARTITIONDATE) AS max_partition,
                  MIN(SAFE_CAST({migrated_ts_column} AS TIMESTAMP)) AS min_migrated_ts_column,
                  MAX(SAFE_CAST({migrated_ts_column} AS TIMESTAMP)) AS max_migrated_ts_column,
                FROM
                  `{migrated_project}.{migrated_dataset}.{migrated_tablename}`
                WHERE
                  _PARTITIONDATE > DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
                  AND SAFE_CAST({migrated_ts_column} AS TIMESTAMP) BETWEEN {PARTITION_FROM}
                  AND {PARTITION_TO} )
              SELECT
                '{context['dag'].dag_id}' AS dag_name,
                {dag_execution_ts} AS execution_timestamp,
                legacy_count.min_partition AS               legacy_min_partition,
                legacy_count.max_partition AS               legacy_max_partition,
                migrated_count.min_partition AS             migrated_min_partition,
                migrated_count.max_partition AS             migrated_max_partition,
                legacy_count.min_legacy_ts_column     AS      legacy_min_legacy_ts_column,
                legacy_count.max_legacy_ts_column     AS      legacy_max_legacy_ts_column,
                migrated_count.min_migrated_ts_column AS  migrated_min_migrated_ts_column,
                migrated_count.max_migrated_ts_column AS  migrated_max_migrated_ts_column,
                get_errors.ordinal_position,
                legacy_count.column_name,
                legacy_count.count_distinct              AS legacy_distinct_values,
                migrated_count.count_distinct AS            migrated_distinct_values,
                CASE WHEN legacy_count.count_distinct = 0 THEN NULL ELSE
                (migrated_count.count_distinct - legacy_count.count_distinct) * 100 / (legacy_count.count_distinct) END AS migrated_over_legacy_percent,
                get_errors.* EXCEPT(ordinal_position,
                  legacy_columns,
                  migrated_columns)
              FROM
                legacy_count
              FULL JOIN
                get_errors
              ON
                get_errors.legacy_columns = legacy_count.column_name
              FULL JOIN
                migrated_count
              ON
               UPPER(soundex(migrated_count.column_name)) = UPPER(soundex(get_errors.legacy_columns)) 
      ) S
  ON
    T.ordinal_position    = S.ordinal_position AND
    T.dag_name            = S.dag_name AND
    T.execution_timestamp = S.execution_timestamp AND
    UPPER(soundex(S.column_name)) = UPPER(soundex('{legacy_columns[i]}'))
    WHEN MATCHED THEN UPDATE SET  
        legacy_min_partition                = S.legacy_min_partition,   
        legacy_max_partition                = S.legacy_max_partition,    
        migrated_min_partition              = S.migrated_min_partition,          
        migrated_max_partition              = S.migrated_max_partition,            
        legacy_min_legacy_ts_column         = S.legacy_min_legacy_ts_column,                
        legacy_max_legacy_ts_column         = S.legacy_max_legacy_ts_column,          
        migrated_min_migrated_ts_column     = S.migrated_min_migrated_ts_column,                
        migrated_max_migrated_ts_column     = S.migrated_max_migrated_ts_column,   
        legacy_distinct_values              = S.legacy_distinct_values,           
        migrated_distinct_values            = S.migrated_distinct_values,       
        migrated_over_legacy_percent        = S.migrated_over_legacy_percent,       
        error_type                          = S.error_type                        