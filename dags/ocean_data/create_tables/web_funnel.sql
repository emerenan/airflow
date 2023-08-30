CREATE TABLE IF NOT EXISTS  `{project}.{dataset}.{table_name}`
(
    year INT64,
    week_number INT64,
    Date DATE,
    VisitorType STRING,
    VisitCount	INTEGER,
    SiteSearchCount	INTEGER,
    PDPCount	INTEGER,
    CheckAvailabilityCount	INTEGER,
    CheckoutCount	INTEGER,
    Bookings	INTEGER,
    Revenue	FLOAT64,
) PARTITION BY DATE_TRUNC(Date, YEAR)