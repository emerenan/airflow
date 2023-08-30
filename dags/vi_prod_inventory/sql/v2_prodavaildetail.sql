
WITH
  sitours AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY product_code ORDER BY DATE_DIFF(CURRENT_DATE(), pulldowndate, DAY),
      ABS(DATE_DIFF(CURRENT_DATE(), releasedate, DAY))) AS rn
  FROM (
    SELECT
      code AS product_code,
      SAFE_CAST(CONCAT(LEFT(releasedate,4), '-', RIGHT(LEFT(releasedate,6),2), '-', RIGHT(releasedate,2)) AS DATE) AS releasedate,
      SAFE_CAST(CONCAT(LEFT(pulldowndate,4), '-', RIGHT(LEFT(pulldowndate,6),2), '-', RIGHT(pulldowndate,2)) AS DATE) AS pulldowndate,
      PRICE_FROM
    FROM
      `avr-warehouse.VISaintDataSnapshot.sitours_new`
    WHERE
      TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2023-08-17") ) ),
  dedup_price AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY productcode ORDER BY DATE_DIFF(CURRENT_DATE(), pulldowndate, DAY),
      ABS(DATE_DIFF(CURRENT_DATE(), releasedate, DAY))) AS rn
  FROM (
    SELECT
      *,
      SAFE_CAST(REGEXP_EXTRACT(p.PricingScheduleId, r'(\d{4}-\d{2}-\d{2})') AS DATE) AS pulldowndate,
      SAFE_CAST((REGEXP_EXTRACT_ALL(p.PricingScheduleId, r'(\d{4}-\d{2}-\d{2})'))[
      OFFSET
        (1)] AS DATE) AS releasedate,
    FROM
      `viator-availability-prod.vi_prod_inventory.inventory_pricing` p ) ),
  price AS (
  SELECT
    *
  FROM
    dedup_price
  WHERE
    rn = 1),
  schedule AS (
  SELECT
    s.PricingScheduleId,
    s.SeasonFrom,
    s.SeasonTo,
    s.OpeningTimes,
    s.PriceType,
    s.ProductCode,
    s.TourGradeCode
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_pricing_schedule` s
),
  booking_cutoff AS (
  SELECT
    *EXCEPT(CutoffOverridesByTime)
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_metadata_booking_cutoff`
  CROSS JOIN
    UNNEST(CutoffOverridesByTime) AS flat_CutoffOverridesByTime ),
  
a AS (
  SELECT
    p.PricingScheduleVersion,
    p.PricingScheduleId,
    p.ProductCode,
    p.TourGradeCode,
    p.PricingType,
    p.currency,
    adult.*,
    ROW_NUMBER() OVER(PARTITION BY p.ProductCode, p.TourGradeCode ORDER BY PARSE_NUMERIC(retailPrice)) AS rn
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_pricing` p,
    UNNEST(priceForAdult) AS adult
  INNER JOIN price on 
    p.PricingScheduleVersion = price.PricingScheduleVersion
   AND p.PricingScheduleId = price.PricingScheduleId
   AND p.ProductCode = price.ProductCode
   AND p.TourGradeCode = price.TourGradeCode
  WHERE
    (minPax = '2'
      OR maxPax = '2') 
      )
  ,ip AS (
  SELECT
    p.*,
    adult.*,
    ROW_NUMBER() OVER(PARTITION BY p.ProductCode, p.TourGradeCode ORDER BY PARSE_NUMERIC(adult.retailPrice)) AS rn
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_pricing` p,
    UNNEST(priceForAdult) AS adult
    INNER JOIN price on 
    p.PricingScheduleVersion = price.PricingScheduleVersion
   AND p.PricingScheduleId = price.PricingScheduleId
   AND p.ProductCode = price.ProductCode
   AND p.TourGradeCode = price.TourGradeCode
  LEFT JOIN
    a
  ON
    p.ProductCode = a.ProductCode
    AND p.TourGradeCode = a.TourGradeCode
  WHERE
    (a.productcode IS NULL AND a.tourgradecode is null) 
    ), final_price as (
SELECT
  PricingScheduleVersion,
  PricingScheduleId,
  ProductCode,
  TourGradeCode,
  PricingType,
  Currency,
  minpax AS adult_minpax,
  maxpax AS adult_maxpax,
  retailPrice AS retail_price_from,
  netPrice AS supplier_price_from,

FROM
  ip
WHERE
  rn = 1 
UNION ALL
SELECT
  PricingScheduleVersion,
  PricingScheduleId,
  ProductCode,
  TourGradeCode,
  PricingType,
  Currency,
  minpax AS adult_minpax,
  maxpax AS adult_maxpax,
  retailPrice AS retail_price_from,
  netPrice AS supplier_price_from,
FROM
  a
WHERE
  rn = 1 
UNION ALL
SELECT *EXCEPT(rn) FROM (
SELECT 
  p.PricingScheduleVersion,
  p.PricingScheduleId,
  p.ProductCode,
  p.TourGradeCode,
  p.PricingType,
  p.Currency,
  SAFE_CAST(NULL AS STRING) AS adult_minpax,
  SAFE_CAST(NULL AS STRING) AS adult_maxpax,
  p.groupPrice.retailPrice AS retail_price_from,
  p.groupPrice.netPrice AS supplier_price_from,
  ROW_NUMBER() OVER(PARTITION BY p.ProductCode, p.TourGradeCode) AS rn
FROM
  `viator-availability-prod.vi_prod_inventory.inventory_pricing` p
    INNER JOIN price on 
    p.PricingScheduleVersion = price.PricingScheduleVersion
   AND p.PricingScheduleId = price.PricingScheduleId
   AND p.ProductCode = price.ProductCode
   AND p.TourGradeCode = price.TourGradeCode

WHERE
p.PricingType = "Group" 
)
where rn = 1 
),
  pricing AS (
  SELECT
    p.ProductCode,
    p.TourGradeCode,
    p.currency,
    p.PricingType,
    p.PricingScheduleId,
    p.PricingScheduleVersion,
    CASE
      WHEN MIN(SAFE_CAST(p.PriceForAdult[SAFE_OFFSET(0)].minPax AS INT64)) > 0 THEN MIN(CONCAT(p.PriceForAdult[SAFE_OFFSET(0)].minPax,'A'))
    ELSE
    '0A'
  END
    AS A_min,
    CASE
      WHEN MIN(SAFE_CAST(p.PriceForChild[SAFE_OFFSET(0)].minPax AS INT64)) > 0 THEN MIN(CONCAT(p.PriceForChild[SAFE_OFFSET(0)].minPax,'C'))
    ELSE
    '0C'
  END
    AS C_min,
    CASE
      WHEN MIN(SAFE_CAST(p.PriceForInfant[SAFE_OFFSET(0)].minPax AS INT64)) > 0 THEN MIN(CONCAT(p.PriceForInfant[SAFE_OFFSET(0)].minPax,'I'))
    ELSE
    '0I'
  END
    AS I_min,
    CASE
      WHEN MIN(SAFE_CAST(p.PriceForYouth[SAFE_OFFSET(0)].minPax AS INT64)) > 0 THEN MIN(CONCAT(p.PriceForYouth[SAFE_OFFSET(0)].minPax,'I'))
    ELSE
    '0Y'
  END
    AS Y_min,
    CASE
      WHEN MIN(SAFE_CAST(p.PriceForSenior[SAFE_OFFSET(0)].minPax AS INT64)) > 0 THEN MIN(CONCAT(p.PriceForSenior[SAFE_OFFSET(0)].minPax,'I'))
    ELSE
    '0S'
  END
    AS S_min
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_pricing` p,
    UNNEST(priceForAdult) AS adult
    INNER JOIN price on 
    p.PricingScheduleVersion = price.PricingScheduleVersion
   AND p.PricingScheduleId = price.PricingScheduleId
   AND p.ProductCode = price.ProductCode
   AND p.TourGradeCode = price.TourGradeCode
  GROUP BY
    p.ProductCode,
    p.TourGradeCode,
    p.currency,
    p.PricingType,
    p.PricingScheduleId,
    p.PricingScheduleVersion,
    p.PriceForAdult[SAFE_OFFSET(0)].minPax,
    p.PriceForChild[SAFE_OFFSET(0)].minPax,
    p.PriceForInfant[SAFE_OFFSET(0)].minPax,
    p.PriceForYouth[SAFE_OFFSET(0)].minPax,
    p.PriceForSenior[SAFE_OFFSET(0)].minPax ),
V2_detail as (
SELECT
  final_price.ProductCode AS product_code,
  final_price.TourGradeCode AS tour_grade_code,
  s.SeasonFrom AS season_from,
  s.SeasonTo AS season_to,
  final_price.currency AS retail_price_from_currency,
  SAFE_CAST(sitours.price_from AS FLOAT64) AS retail_price_from,
  SAFE_CAST(NULL AS FLOAT64) AS sugg_retail_price_from,
  --same as retail
  final_price.currency AS supplier_price_from_currency,
  SAFE_CAST(final_price.supplier_price_from AS FLOAT64) AS supplier_price_from,
  #CUTOFF
  cutoff.BookingCutoffHours AS booking_cutoff_hours,
  cutoff.BookingCutoffType AS booking_cutoff_type,
  cutoff.FixedTime AS fixed_time,
  meta.TimeZone AS timezone,
  meta.StartTimeType AS start_time_type,
  final_price.PricingType AS pricing_model_type,
  meta.live,
  meta.RequiresAdult AS requires_adult,
  NULL AS unitType,
  --CONCAT(a_min,c_min,i_min,y_min,s_min) AS traveller_mix_from,
  SAFE_CAST(NULL AS STRING) AS traveller_mix_from,
IF
  (meta.MaxTravellerCount>1000, 'N/A', CONCAT( meta.MaxTravellerCount,'A', meta.MaxTravellerCount, 'C', meta.MaxTravellerCount, 'I', meta.MaxTravellerCount, 'Y', meta.MaxTravellerCount, 'S') ) AS traveller_mix_to,
  s.SeasonFrom,
  s.SeasonTo,
  meta.MaxTravellerCount,
  ROW_NUMBER() OVER (PARTITION BY final_price.productcode, final_price.tourgradecode ORDER BY final_price.PricingType DESC) AS rn
FROM
  final_price
  left join sitours on sitours.product_code = final_price.productcode
LEFT JOIN
  schedule s
ON
  final_price.PricingType = s.PriceType
  AND final_price.PricingScheduleId = s.PricingScheduleId
  AND final_price.ProductCode = s.ProductCode
  AND final_price.TourGradeCode = s.TourGradeCode

LEFT JOIN
  `viator-availability-prod.vi_prod_inventory.inventory_metadata` meta
ON
  final_price.ProductCode = meta.ProductCode
LEFT JOIN
  booking_cutoff cutoff
ON
  final_price.ProductCode = cutoff.ProductCode
GROUP BY
  s.SeasonFrom,
  s.SeasonTo,
  final_price.ProductCode,
  final_price.TourGradeCode,
  final_price.currency,
  sitours.price_from,
  final_price.currency,
  final_price.supplier_price_from,
  final_price.PricingType,
  cutoff.BookingCutoffHours,
  cutoff.BookingCutoffType,
  cutoff.FixedTime,
  --cutoff.flat_CutoffOverridesByTime,
  meta.TimeZone,
  meta.StartTimeType,
  meta.Live,
  meta.RequiresAdult,
  meta.MaxTravellerCount
  --a_min,
  --c_min,
  --i_min,
  --y_min,
  --s_min
  )
  select *EXCEPT(rn) from V2_detail where rn = 1