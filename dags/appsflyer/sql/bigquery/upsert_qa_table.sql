MERGE logging.appsflyers_qa as a
USING logging.{{ table_id }} as b
ON a.id = b.id
WHEN MATCHED THEN
  UPDATE SET number_rows = b.number_rows
WHEN NOT MATCHED THEN 
    INSERT (
        id, 
        event_date, 
        extract_type, 
        database, 
        event_type,
        number_rows) 
    VALUES (
        b.id, 
        b.event_date, 
        b.extract_type, 
        b.database, 
        b.event_type, 
        b.number_rows)