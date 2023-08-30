WITH DateRange AS (
    SELECT
        CAST ('2020-01-01' AS DATE) AS MinVisitorDate
        ,CAST('{min-event-date}' AS DATE) AS MinEventDate
        ,CAST('{max-event-date}' AS DATE) AS MaxEventDate
) , VisitorDates AS (
    SELECT DISTINCT
        c.BookerEmail ,
        bi.FirstTransactionDate AS VisitDate ,
        DATE_DIFF(bi.FirstTransactionDate,DATE('2000-01-01'),DAY) AS DateNum
    FROM `avr-warehouse.ATTRData.vwBookingItem` bi, DateRange
    JOIN `avr-warehouse.ATTRData.BookingItemDetail` bid ON bi.ItineraryItemID = bid.ItineraryItemID
    JOIN `avr-warehouse.ATTRData.vwCustomer` c ON bi.BookerId = c.BookerId
    WHERE bi.FirstTransactionDate >= DateRange.MinVisitorDate
    AND bid.EntryPointOfSale IN ('viator.com','m.viator.com')
) , RepeatVisitor AS (
    SELECT
        vd.BookerEmail ,
        vd.VisitDate ,
        IF(COUNT(1) OVER (PARTITION BY vd.BookerEmail ORDER BY vd.DateNum RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) > 0, 1, 0) AS Repeat30Days ,
        IF(COUNT(1) OVER (PARTITION BY vd.BookerEmail ORDER BY vd.DateNum RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING) > 0, 1, 0) AS Repeat90Days ,
        IF(COUNT(1) OVER (PARTITION BY vd.BookerEmail ORDER BY vd.DateNum RANGE BETWEEN 180 PRECEDING AND 1 PRECEDING) > 0, 1, 0) AS Repeat180Days ,
        IF(COUNT(1) OVER (PARTITION BY vd.BookerEmail ORDER BY vd.DateNum RANGE BETWEEN 365 PRECEDING AND 1 PRECEDING) > 0, 1, 0) AS Repeat365Days
    FROM VisitorDates vd
), Bookings AS (
    SELECT
        c.BookerEmail AS BookerEmail ,
        bi.FirstTransactionDate AS VisitDate ,
        CASE
            WHEN s.RetailsiteUrl IN ('viandroid.shop.viator.com', 'vitablet.android.shop.viator.com', 'android.shop.viator.com') THEN 'Android'
            WHEN s.RetailsiteUrl IN ('viipad.shop.viator.com', 'viiphone.shop.viator.com' , 'ipadUE.shop.viator.com', 'ipad.shop.viator.com', 'iphone.shop.viator.com') THEN 'iOS'
        END AS AppType ,
        COUNT(DISTINCT bi.ItineraryItemID) AS Bookings ,
        SUM(bi.NetViatorRevenueUSD) AS Revenue
    FROM `avr-warehouse.ATTRData.vwBookingItem` bi, DateRange
    JOIN `avr-warehouse.ATTRData.BookingItemDetail` bid ON bi.ItineraryItemID = bid.ItineraryItemID
    JOIN `avr-warehouse.ATTRData.vwCustomer` c ON bi.BookerId = c.BookerId
    JOIN `avr-warehouse.ATTRData.vwSite` s ON bi.RetailSiteURL = s.RetailsiteUrl
    JOIN VisitorDates vd ON c.BookerEmail = vd.BookerEmail AND bi.FirstTransactionDate = vd.VisitDate
    WHERE bi.FirstTransactionDate BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
        AND bi.RetailSiteURL IN ('viandroid.shop.viator.com','vitablet.android.shop.viator.com','android.shop.viator.com' ,
        'viipad.shop.viator.com','viiphone.shop.viator.com','ipadUE.shop.viator.com',
        'ipad.shop.viator.com','iphone.shop.viator.com')
    GROUP BY BookerEmail, VisitDate, AppType
)
SELECT
    {year} as year,
    {week_number} as week_number,
    vd.VisitDate AS Date ,
    CASE
        WHEN Repeat365Days >= 1 THEN 'Repeat'
        ELSE 'New'
    END AS BookerType ,
    AppType ,
    COUNT(DISTINCT vd.BookerEmail) AS BookerCountCount ,
    SUM(Bookings) AS Bookings ,
    SUM(Revenue) AS Revenue
FROM VisitorDates vd, DateRange
LEFT JOIN RepeatVisitor rv
    ON vd.BookerEmail = rv.BookerEmail
    AND vd.VisitDate = rv.VisitDate
JOIN Bookings b
    ON vd.BookerEmail = b.BookerEmail
    AND vd.VisitDate = b.VisitDate
WHERE vd.VisitDate BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
GROUP BY Date, BookerType, AppType