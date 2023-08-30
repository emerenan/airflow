WITH DateRange AS (
    SELECT
        CAST ('2020-01-01' AS DATE) AS MinVisitorDate
        ,CAST('{min-event-date}' AS DATE) AS MinEventDate
        ,CAST('{max-event-date}' AS DATE) AS MaxEventDate
), VisitorDates AS (
        SELECT DISTINCT
            pv.VisitorID
            ,CAST(pv._PARTITIONDATE AS DATE) AS VisitDate
            ,DATE_DIFF(pv._PARTITIONDATE,DATE('2000-01-01'),DAY) AS DateNum
        FROM `avr-warehouse.ATTRData.VIPageView` pv, DateRange
        WHERE pv._PARTITIONDATE >= DateRange.MinVisitorDate AND pv.POS IN ('viator.com','m.viator.com')
), RepeatVisitor AS (
    SELECT
        vd.VisitorID
        ,vd.VisitDate
        ,IF(COUNT(1) OVER (PARTITION BY vd.VisitorID ORDER BY vd.DateNum RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) > 0,1,0) AS Repeat30Days
        ,IF(COUNT(1) OVER (PARTITION BY vd.VisitorID ORDER BY vd.DateNum RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING) > 0,1,0) AS Repeat90Days
        ,IF(COUNT(1) OVER (PARTITION BY vd.VisitorID ORDER BY vd.DateNum RANGE BETWEEN 180 PRECEDING AND 1 PRECEDING) > 0,1,0) AS Repeat180Days
        ,IF(COUNT(1) OVER (PARTITION BY vd.VisitorID ORDER BY vd.DateNum RANGE BETWEEN 365 PRECEDING AND 1 PRECEDING) > 0,1,0) AS Repeat365Days
    FROM VisitorDates vd
), PDPAndCheckout AS (
    SELECT DISTINCT
        pv.VisitorID
        ,CAST(pv._PARTITIONDATE AS DATE) AS VisitDate
        ,pv.ServletName
        ,COUNT(DISTINCT UUID) AS PVCount
    FROM `avr-warehouse.ATTRData.VIPageView` pv, DateRange
    JOIN VisitorDates vd ON pv.VisitorID = vd.VisitorID AND CAST(pv._PARTITIONDATE AS DATE) = vd.VisitDate
    WHERE pv._PARTITIONDATE BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
    AND pv.ServletName IN ('product_detail','checkout_flow')
    GROUP BY pv.VisitorID, VisitDate, ServletName
), CheckAvailSearch AS (
    SELECT DISTINCT
        pa.VisitorID
        ,CAST(pa._PARTITIONDATE AS DATE) AS VisitDate
        ,CASE WHEN
            LOWER(pa.PageAction) LIKE '%check_availability_success%' THEN 'Check Availability'
            WHEN (pa.PageProperties LIKE '%searchId%'
            OR pa.PageProperties LIKE '%searchType%'
            OR pa.PageProperties LIKE '%section:typeahead%'
            OR pa.PageProperties LIKE'%search_form_typeahead_search_results_menu%'
            OR pa.PageAction = 'click_search_open'
            OR pa.PageProperties LIKE '%header_search_typeahead_search_results_menu%') THEN 'Search'
        END AS EventType
        ,COUNT(DISTINCT PUID) AS EventPVCount
        ,COUNT(DISTINCT UUID) AS EventCount
    FROM `avr-warehouse.ATTRData.VIPageAction` pa, DateRange
    JOIN VisitorDates vd ON pa.VisitorID = vd.VisitorID
        AND CAST(pa._PARTITIONDATE AS DATE) = vd.VisitDate
    WHERE pa._PARTITIONDATE BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
    AND (LOWER(pa.PageAction) LIKE '%check_availability_success%'
        OR (pa.PageProperties LIKE '%searchId%'
        OR pa.PageProperties LIKE '%searchType%'
        OR pa.PageProperties LIKE '%section:typeahead%'
        OR pa.PageProperties LIKE'%search_form_typeahead_search_results_menu%'
        OR pa.PageAction = 'click_search_open'
        OR pa.PageProperties LIKE '%header_search_typeahead_search_results_menu%'))
    GROUP BY pa.VisitorID, VisitDate, EventType
)

, Bookings AS (
    SELECT
        bid.EntryVisitorID AS VisitorID
        ,bi.FirstTransactionDate AS VisitDate
        ,COUNT(DISTINCT bi.ItineraryItemID) AS Bookings
        ,SUM(bi.NetViatorRevenueUSD) AS Revenue
    FROM `avr-warehouse.ATTRData.vwBookingItem` bi, DateRange
    JOIN `avr-warehouse.ATTRData.BookingItemDetail` bid ON bi.ItineraryItemID = bid.ItineraryItemID
    JOIN VisitorDates vd ON bid.EntryVisitorID = vd.VisitorID AND bi.FirstTransactionDate = vd.VisitDate
    WHERE bi.FirstTransactionDate BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
    GROUP BY VisitorID, VisitDate
)

SELECT
    {year} as year,
    {week_number} as week_number,
    vd.VisitDate AS Date
    ,CASE WHEN Repeat365Days >= 1 THEN 'Repeat' ELSE 'New' END AS VisitorType
    ,COUNT(DISTINCT vd.VisitorID) AS VisitCount
    ,SUM(s.EventPVCount) AS SiteSearchCount
    ,SUM(pdp.PVCount) AS PDPCount
    ,SUM(ca.EventCount) AS CheckAvailabilityCount
    ,SUM(ch.PVCount) AS CheckoutCount
    ,SUM(Bookings) AS Bookings
    ,SUM(Revenue) AS Revenue
FROM VisitorDates vd, DateRange
LEFT JOIN RepeatVisitor rv ON vd.VisitorID = rv.VisitorID AND vd.VisitDate = rv.VisitDate
LEFT JOIN CheckAvailSearch s ON vd.VisitorID = s.VisitorID AND vd.VisitDate = s.VisitDate AND s.EventType = 'Search'
LEFT JOIN PDPAndCheckout pdp ON vd.VisitorID = pdp.VisitorID AND vd.VisitDate = pdp.VisitDate AND pdp.ServletName = 'product_detail'
LEFT JOIN CheckAvailSearch ca ON vd.VisitorID = ca.VisitorID AND vd.VisitDate = ca.VisitDate AND ca.EventType = 'Check Availability'
LEFT JOIN PDPAndCheckout ch ON vd.VisitorID = ch.VisitorID AND vd.VisitDate = ch.VisitDate AND ch.ServletName = 'checkout_flow'
LEFT JOIN Bookings b ON vd.VisitorID = b.VisitorID AND vd.VisitDate = b.VisitDate
WHERE vd.VisitDate BETWEEN DateRange.MinEventDate AND DateRange.MaxEventDate
GROUP BY Date, VisitorType