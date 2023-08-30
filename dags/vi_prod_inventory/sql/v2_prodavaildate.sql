
WITH
  availability AS (
  SELECT
    ProductCode AS product_code,
    TourGradeCode AS tour_grade_code,
    CONCAT(ProductCode,"/",TourGradeCode) AS prod_tour_grade,
    -- combine the product and tour grade code into one field
    SAFE_CAST(TravelDate AS DATE) AS travel_date,
    -- cast the travel date as a date
    CASE
      WHEN REGEXP_CONTAINS(SessionTime, r'\d{4}') THEN REGEXP_REPLACE(SessionTime, r'(\d{2})(\d{2})', r'\1:\2')  -- check if session time contains four digits, if yes, then convert it to time format
    ELSE
    NULL  -- if session time does not contain four digits, set it as NULL
  END
    AS start_time,
    availability,
    TravelDate,
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_availability_history`
  WHERE
    TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) > TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 3 day))  
     ),
  cutoff AS (
  SELECT
    ProductCode AS product_code,
    BookingCutoffType AS booking_cutoff_type,
    BookingCutoffHours AS booking_cutoff_hours,
    FixedTime AS fixed_time,
    cutoff_override   -- select each element in the cutoff overrides by time array as a separate row
  FROM
    `viator-availability-prod.vi_prod_inventory.inventory_metadata_booking_cutoff`,
    -- table to select from
    UNNEST(CutoffOverridesByTime) AS cutoff_override  -- unnest the array column to create multiple rows, one for each element
    )
SELECT
  DISTINCT SAFE_CAST(NULL AS INT64)     AS  detail_id
  ,availability.prod_tour_grade          --AS prodTourGrade,
  ,availability.travel_date              --AS travelDate,
  ,availability.start_time               --AS startTime,
  ,availability.availability             --AS availability,
  ,SAFE_CAST(NULL AS STRING)             AS booking_cutoff
  ,SAFE_CAST(NULL AS STRING)             AS natural_cutoff
  ,SAFE_CAST(NULL AS STRING)             AS last_updated_timestamp
  ,cutoff.booking_cutoff_type
  ,cutoff.booking_cutoff_hours
  ,cutoff.fixed_time
  ,cutoff.cutoff_override
  ,DATE_TRUNC(SAFE_CAST(availability.travel_date AS DATE),MONTH) AS m_partition_traveldate
FROM
  availability
LEFT JOIN
  cutoff
ON
  cutoff.product_code = availability.product_code
WHERE
  travel_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR),MONTH) /* 2 years ago */
