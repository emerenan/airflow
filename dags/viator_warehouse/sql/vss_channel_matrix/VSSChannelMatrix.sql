{% set label_list = labels %}
{% set columns = table_schema %}

{% macro get_ddl(project, dataset, table_name, materialisation, table_exists, partition_field, table_description) %} 
{% set table_or_view = 'TABLE' if materialisation in ('table', 'partition_table') else 'VIEW' %}
{% set command = ({'replace': 'OR REPLACE ','exists': ' IF NOT EXISTS' })%}
{% set ddl_command = table_or_view ~ command['exists'] if table_exists else command['replace'] ~ table_or_view %}
CREATE {{ ddl_command }} {{ project }}.{{ dataset }}.{{ table_name }}  {% if table_or_view != 'VIEW' %}
({% for col, col_type in columns.items() %}
    {{col}} {{col_type}}{% if not loop.last %},{% endif %}{% endfor %}
){% endif %} {% if materialisation == 'partition_table' %}
PARTITION BY {{ partition_field }} {% endif %}
OPTIONS(
    description="{{ table_description }}",
    labels=[{% for label in label_list %}{{label}}{% if not loop.last %}, {% endif %}{% endfor %}]
) {% endmacro %}

/*
Create the table based on the sql below
*/
{{ get_ddl(project_id, dataset_id, table_id, materialisation, table_exists, partition_field, table_description) }} AS
WITH UserPresence AS (  /* time spent on productive aux in SF */
  SELECT CONCAT(format_date("%Y-%m", date(StartDateUTC) ) , '-01' ) as Date
    ,AgentSite as Site
    ,SUM(DurationMinutes) as ProductiveAux
    /*Adding 60% of ChatAndCases to ChatAux and 40% to CasesAux. This is based on the type of activities done while on the ChatAndCasesAux (reviewed in excel, saved in the Channel Matrix folder*/
    ,SUM(CASE WHEN AuxCodes = 'Available - Cases Only' then DurationMinutes else 0 end) 
      + SUM(CASE WHEN AuxCodes = 'Available - Chat and Cases' then (DurationMinutes * 0.4) else 0 end) as CasesOnlyAux
    ,SUM(CASE WHEN AuxCodes = 'Available - Chat Only' then DurationMinutes else 0 end)
      + SUM(CASE WHEN AuxCodes = 'Available - Chat and Cases' then (DurationMinutes * 0.6) else 0 end) as ChatOnlyAux
    ,SUM(CASE WHEN AuxCodes = 'Available - Project Work'  then DurationMinutes else 0 end) as ProjectWorkAux /*minimal volume*/
    /*Adding 60% of Follow Up time. This is based on the fact that the remainder 40% is used while actively working on a distributed item (therefore already captured in the Active Time below) */
    ,SUM(CASE WHEN AuxCodes = 'Follow Up' then (DurationMinutes * 0.6) else 0 end) as FollowUpAux

    FROM `avr-warehouse.SharedCustomerCare.IntervalComplianceExpSST` 
    where IsProductive = 'Productive'
    group by 1,2
    )
,DateRange AS(
  SELECT
  /* Date range for Completed Bookings */
  DATE_SUB(CURRENT_DATE(),INTERVAL 364 day) AS DatesFrom
)
,CreatDate AS(SELECT SupplierCode, 
CreatedDate,
ROW_NUMBER() OVER (PARTITION BY SupplierCode ORDER BY CreatedDate) as CountDate
FROM `avr-warehouse.ATTRData.vwProductAttributeCurrent`
)
,NewVsOld AS (
SELECT DISTINCT SupplierCode
,case when CountDate = 1 then CreatedDate end as CreatedDate
FROM CreatDate
WHERE Countdate = 1)
,AuxAndCost as (
  SELECT up.Date
    ,up.Site
    /* converting to Seconds */
    ,(up.CasesOnlyAux * 60)   as CasesOnlyAux
    ,(up.ChatOnlyAux * 60)    as ChatOnlyAux
    ,(up.ProjectWorkAux * 60) as ProjectWorkAux
    ,(up.FollowUpAux * 60)    as FollowUpAux
    ,((IFNULL(up.CasesOnlyAux,0) 
        + IFNULL(up.ChatOnlyAux,0)
        + IFNULL(up.ProjectWorkAux,0)
        + IFNULL(up.FollowUpAux,0) ) *60 )
        AS TotalCostedAux
    ,safe_cast(c.PeopleCostsUSD as string) as PeopleCostUSD
    ,safe_cast(c.FullyLoadedCostsUSD as string) as FullyLoadedCostUSD    
    FROM UserPresence up
    left join `{{ project_id }}.landing.VSSCallCenterCostTable` c on SAFE_CAST(up.Date AS DATE) = DATE(CONCAT(c.YearMonth, '-01')) 
  )
,AuxCostPerSecond as (
  select Date
  ,Site
  ,PeopleCostUSD
  ,FullyLoadedCostUSD
  ,SAFE_DIVIDE( safe_cast(PeopleCostUSD as float64), TotalCostedAux )       AS PeopleCostPerProductiveSecond
  ,SAFE_DIVIDE( safe_cast(FullyLoadedCostUSD as float64), TotalCostedAux )  AS FullyLoadedCostPerProductiveSecond
  from AuxAndCost
  )
/* This cte pulls the distributed tasks per case reason and adds active time + chat wrap up (calculated in a separate view - this is because active time isn't tracked once the chat ends, so we need to add it on top of chat active time).
Chat wrap up looks at the time elapsed between chat ends to first status change to 'Resolved' or 'Waiting for Response'.
It is capped at 180 seconds if higher than that, and uses true value if lower than 180 seconds.
It also calculates distinct case count */
,Activities as (
  SELECT 
     aw.TimestampUTC
    ,aw.AgentSite as Site
    ,aw.CaseId
    ,aw.WorkItemId    
    ,sfc.CaseNumber
    ,sfc.SupplierCode
    ,sfc.ProductCode
    ,CASE WHEN sfc.productgroup IS NULL
      THEN  sfc.ProductSubCategory
      ELSE  sfc.productgroup 
      END                               AS ProductCategory
    ,aw.CaseRecordType
    ,sfc.Reason1
    ,sfc.Reason2
    ,sfc.Reason3
    ,aw.TaskType
    ,aw.AgentRole
    ,aw.AgentSite
    ,IF( (row_number() over (partition by sfc.casenumber order by sfc.casenumber, aw.TimestampUTC asc)) = 1, 1, 0)   as DistinctCaseCount
    ,IF ( IF( (row_number() over (partition by sfc.casenumber order by sfc.casenumber, aw.TimestampUTC asc)) = 1, 1, 0) = 1, TIMESTAMP_DIFF(sfc.CaseClosedTimestampUTC, sfc.CaseCreatedTimeStampUTC, HOUR), NULL) as  TurnAroundTimeHrs    
    ,aw.Tasks as DistributedTasks
    ,aw.ActiveTimeSec as ActiveTimeSeconds
    ,wup.ChatWrapUpSeconds 
    ,IFNULL(aw.ActiveTimeSec,0) + IFNULL(wup.ChatWrapUpSeconds,0) AS TotalActiveTimeSeconds /* includes chat wrap up capped to 180 seconds */

    from `avr-warehouse.SharedCustomerCare.SalesForceAgentWorkEXP` aw
    left join `avr-warehouse.SharedCustomerCare.SalesForceCaseEXP` sfc on aw.CaseId = sfc.CaseId
    left join `{{ project_id }}.landing.VSSChatWrapUpToFirstCaseClose` wup on aw.WorkItemId = wup.ChatId
    where lower(aw.AgentRole) like '%sst%'
    )
/* This cte pulls ready state at monthly level.
 It basically takes all Costed Aux and deducts the Total Active time calculated above. The remaining time will be classified as Ready State.
 There are months where the Total Active time is higher than the Costed Aux, so when that happens Ready state will be 0.
*/
,ReadyStateTime as ( 
  SELECT CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) as Date
  ,aw.Site
  ,ac.TotalCostedAux
  ,ac.FollowUpAux
  ,SUM(aw.TotalActiveTimeSeconds) AS TotalActiveTimeSeconds
  /* deducting Follow Up time from here because Follow Up time wouldn't be included in the Total Active time */
  ,IF( ( IFNULL(ac.TotalCostedAux,0) - IFNULL(ac.FollowUpAux,0) - SUM(IFNULL(aw.TotalActiveTimeSeconds,0)) ) <0, 0, ( IFNULL(ac.TotalCostedAux,0) - IFNULL(ac.FollowUpAux,0) - SUM(IFNULL(aw.TotalActiveTimeSeconds,0)) )) AS ReadyState

  FROM Activities aw
  left join AuxAndCost ac on CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) = ac.Date and lower(aw.Site) = lower(ac.Site)
  where date(aw.TimestampUTC) >= '2022-01-01' /* HC_DATE: Table must be created from this date */
  GROUP BY 1,2,3,4
  )
/*This cte aggregates the tasks at reason level.
  It also distributes the Follow Up Aux time and Ready State time across the different Demand Driver. 
  The distribution is based on the % of Active time over the total Active time for the Month, then the % is applied to Follow Up.  
*/
,ActivitiesAggr as (
  SELECT CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) as Date
    ,aw.CaseID
    ,aw.AgentSite as Site
    ,aw.Reason1
    ,aw.Reason2
    ,aw.Reason3
    ,aw.Tasktype
    ,aw.CaseRecordType
    ,aw.SupplierCode
    ,SUM(DistinctCaseCount)            as CaseCount
    ,SUM(TurnAroundTimeHrs)            as TurnAroundTimeHrs
    ,SUM(aw.DistributedTasks)          as DistributedTasks
    ,SUM(aw.TotalActiveTimeSeconds)    as TotalActiveTimeSeconds
    ,m.TotalActiveTimeSecondsMonth 
    ,up.FollowUpAux                    as FollowUpAuxSecondsMonth
    ,rr.ReadyState                     as ReadyStateSecondsMonth
    ,SAFE_DIVIDE(SUM(aw.TotalActiveTimeSeconds) , m.TotalActiveTimeSecondsMonth)                         as ActiveTimeRatioToMonthTotal
    ,(SAFE_DIVIDE(SUM(aw.TotalActiveTimeSeconds) , m.TotalActiveTimeSecondsMonth)) * (up.FollowUpAux)    as FollowUpAuxSeconds 
    ,(SAFE_DIVIDE(SUM(aw.TotalActiveTimeSeconds) , m.TotalActiveTimeSecondsMonth)) * (rr.ReadyState)     as ReadyStateSeconds        

    ,sum(case when aw.distinctcasecount = 1 then csat.CESCount else 0 end)         as CESCount
    ,sum(case when aw.distinctcasecount = 1 then csat.CESDetractors else 0 end)    as CESDetractors
    ,sum(case when aw.distinctcasecount = 1 then csat.CESPromoters else 0 end)     as CESPromoters
    ,sum(case when aw.distinctcasecount = 1 then csat.FCRNumerator else 0 end)     as FCRNumerator
    ,sum(case when aw.distinctcasecount = 1 then csat.FCRDenominator else 0 end)   as FCRDenominator
    ,sum(case when aw.distinctcasecount = 1 then csat.ResolutionYes else 0 end)    as ResolutionYes  
    ,sum(case when aw.distinctcasecount = 1 then csat.ResolutionCount else 0 end)  as ResolutionCount
    ,sum(case when aw.distinctcasecount = 1 then csat.AgentPerformance else 0 end) as AgentPerformance

    ,sum(case when aw.distinctcasecount = 1 then csat.EasinessOfAddingProductCount else 0 end)       as EasinessOfAddingProductCount
    ,sum(case when aw.distinctcasecount = 1 then csat.EasinessOfAddingProductDetractors else 0 end)  as EasinessOfAddingProductDetractors
    ,sum(case when aw.distinctcasecount = 1 then csat.EasinessOfAddingProductPromoters else 0 end)   as EasinessOfAddingProductPromoters        

    ,sum(case when aw.distinctcasecount = 1 and  csat.responseid is not null then 1 else 0 end) as SurveyCount
    ,aw.ProductCode
    ,aw.ProductCategory

    FROM Activities aw 
    LEFT JOIN (SELECT CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) as Date
                ,SUM(TotalActiveTimeSeconds) as TotalActiveTimeSecondsMonth 
                FROM Activities aw 
                GROUP BY 1) m on CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) = m.Date
    LEFT JOIN UserPresence up ON  CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) = up.Date and lower(aw.AgentSite) = lower(up.Site)
    LEFT JOIN ReadyStateTime rr on CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) = rr.Date and lower(aw.AgentSite) = lower(rr.Site)
    LEFT JOIN `avr-warehouse.SharedCustomerCare.SSTSatisfactionSurveyEXP` csat on aw.CaseId = csat.CaseId
    LEFT JOIN `avr-warehouse.ATTRData.vwProductAttributeCurrent` s on s.ProductCode = csat.ProductCode


    WHERE LOWER(aw.AgentRole) LIKE '%sst%'
    AND CONCAT(format_date("%Y-%m", date(aw.TimestampUTC) ) , '-01' ) >= '2022-01-01' /* HC_DATE: Table must be created from this date */
    GROUP BY 1,2,3,4,5,6,7,8,9,14,up.FollowUpAux, rr.ReadyState,32,33
  )
/* adds info on whether the cost is Actual or Forecast*/
,CostType as (
  select distinct yearmonth, Site, PeopleCostsForecastOrActuals
  from `{{ project_id }}.landing.VSSCallCenterCostTable`
  )

,SupplierOpenDate as (SELECT ROW_NUMBER () OVER (PARTITION BY suppliercode ORDER BY date(ContractAcceptanceDate)) AS rn, suppliercode, ContractAcceptanceDate FROM `avr-warehouse.ATTRData.SupplierAttributeHistory` 
WHERE ContractAcceptanceDate is not null
group by 2, 3
order by 2)

/* Account manager if Global Sales of Fully managed */
,ManagedSuppliers AS (
  select 
  distinct acc.Supplier_ID__c as SupplierCode
  ,supplier_name__c as SupplierName
  ,u.Name as AccountManager
  from `avr-warehouse.ATTRSalesForce.Account` acc
  left join `avr-warehouse.ATTRSalesForce.User` u on acc.OwnerId = u.Id
  and acc.Supplier_ID__c is not null 
  )
,final as (
  select 
   aa.Date
  ,aa.CaseID
  ,CONCAT('Q', extract(quarter from DATE(aa.Date) )) as Quarter
  ,extract(year from DATE(aa.Date) ) as Year  
  ,aa.Site
  ,aa.Reason1
  ,aa.Reason2
  ,aa.Reason3
  ,aa.Tasktype
  ,aa.CaseRecordType
  ,ms.AccountManager
  ,aa.SupplierCode
  ,supp.SupplierName
  ,supp.SupplierCountry
  ,supp.ConnectedReservationSystem
  ,date(SOD.ContractAcceptanceDate) AS FirstContractAcceptanceDate
  ,supp.ContractAcceptanceDate AS ContractAcceptanceDate
  ,aa.CaseCount
  ,aa.TurnAroundTimeHrs
  ,aa.DistributedTasks
  ,aa.TotalActiveTimeSeconds as ActiveTimeSeconds
  ,aa.FollowUpAuxSeconds
  ,aa.ReadyStateSeconds
  ,cc.PeopleCostPerProductiveSecond
  ,cc.FullyLoadedCostPerProductiveSecond
  ,ct.PeopleCostsForecastOrActuals as CostType

  ,SUM(aa.CESCount)         AS CESCount
  ,SUM(aa.CESDetractors)    AS CESDetractors
  ,SUM(aa.CESPromoters)     AS CESPromoters
  ,SUM(aa.FCRNumerator)     AS FCRNumerator
  ,SUM(aa.FCRDenominator)   AS FCRDenominator
  ,SUM(aa.ResolutionYes  )  AS ResolutionYes
  ,SUM(aa.ResolutionCount)  AS ResolutionCount
  ,SUM(aa.AgentPerformance) AS AgentPerformance 
  ,SUM(aa.EasinessOfAddingProductCount)      as EasinessOfAddingProductCount
  ,sum(aa.EasinessOfAddingProductDetractors) as EasinessOfAddingProductDetractors
  ,sum(aa.EasinessOfAddingProductPromoters)  as EasinessOfAddingProductPromoters
  ,SUM(aa.SurveyCount)                       AS SurveyCount
  ,aa.ProductCode
  ,aa.ProductCategory
  ,v.Createddate


  ,IFNULL(aa.TotalActiveTimeSeconds,0) + IFNULL(SAFE_CAST(aa.FollowUpAuxSeconds AS INT64),0) + IFNULL(SAFE_CAST(aa.ReadyStateSeconds AS INT64),0) AS TotalActiveTimeSeconds

  /* used in final query to add GBV info at Supplier level */
  ,ROW_NUMBER() OVER (PARTITION BY aa.Date, aa.SupplierCode order by aa.Date, aa.SupplierCode) as RowNum
  ,ROW_NUMBER() OVER (PARTITION BY aa.SupplierCode ORDER BY aa.SupplierCode) as RNSC
  ,ROW_NUMBER() OVER (PARTITION BY aa.SupplierCode,aa.ProductCode ORDER BY aa.ProductCode) AS RNPC

  FROM ActivitiesAggr aa
  LEFT JOIN AuxCostPerSecond cc on aa.Date = cc.Date and aa.Site = cc.Site
  LEFT JOIN CostType ct on aa.Date = CONCAT(ct.YearMonth, '-01') and aa.Site = ct.Site
  LEFT JOIN `avr-warehouse.ATTRData.vwSupplierAttributeCurrent` supp on aa.suppliercode = supp.SupplierCode 
  LEFT JOIN ManagedSuppliers ms on aa.suppliercode = ms.SupplierCode
  LEFT JOIN (SELECT SupplierCode, contractacceptancedate FROM SupplierOpenDate WHERE rn=1) SOD on aa.suppliercode = SOD.SupplierCode
  LEFT JOIN NewVsOld V ON aa.supplierCode = V.SupplierCode

  where aa.date >= '2022-01-01' /* HC_DATE: Table must be created from this date */
  and supp.IsTestSupplier IS FALSE
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,39,40,41,42
  )
 select 
  cast(f.Date as date) as partition_date
  ,f.CaseID
  ,f.Quarter
  ,f.Year
  ,f.Site
  ,f.Reason1
  ,f.Reason2
  ,f.Reason3
  ,f.Tasktype
  ,f.CaseRecordType
  ,f.AccountManager
  ,f.SupplierCode
  ,f.SupplierName
  ,f.SupplierCountry
  ,f.ConnectedReservationSystem
  ,f.FirstContractAcceptanceDate
  ,f.ContractAcceptanceDate
  ,f.CaseCount
  ,f.TurnAroundTimeHrs
  ,f.DistributedTasks
  ,f.ActiveTimeSeconds
  ,f.FollowUpAuxSeconds
  ,f.ReadyStateSeconds
  ,f.PeopleCostPerProductiveSecond
  ,f.FullyLoadedCostPerProductiveSecond
  ,f.CostType
  ,f.CESCount
  ,f.CESDetractors
  ,f.CESPromoters
  ,f.FCRNumerator
  ,f.FCRDenominator
  ,f.ResolutionYes
  ,f.ResolutionCount
  ,f.AgentPerformance
  ,f.EasinessOfAddingProductCount
  ,f.EasinessOfAddingProductDetractors
  ,f.EasinessOfAddingProductPromoters
  ,f.SurveyCount
  ,f.ProductCategory
  ,f.Createddate
  ,case when rownum = 1 then supprev.GBV end                  as GBV /*This only records GBV for months with contact*/
  ,case when rownum = 1 then supprev.OperatingIncome end      as OperatingIncome  
  ,case when rownum = 1 then supprev.Bookings end             as Bookings
  ,case when rownum = 1 then supprev.CancelledBookings end    as CancelledBookings
  ,case when rownum = 1 then supprev.CancelledGBV end         as CancelledGBV
  ,case when RNSC = 1 then P.PastBookings ELSE 0 end          AS TotalBookingsPrevious12Months
  ,case when RNSC = 1 then P.pastGBV ELSE 0 end               AS GBVUSDPrevious12Months /*Suppliers true GBV Previous 12 Months*/
  ,case when RNPC = 1 then Pro.PastBookings ELSE NULL end     AS PastBookingsPC
  ,case when RNPC = 1 then Pro.PastGBV ELSE NULL end          AS PastGBVPC

 from final f
 /*GVB and Bookings per Suppler Year to Date*/
LEFT JOIN (SELECT
              Bi.SupplierCode,
              COUNT(DISTINCT Bi.ItineraryItemID) AS PastBookings,
              SUM(Bi.GrossBookingUSD) AS PastGBV,
              FROM `avr-warehouse.ATTRData.vwBookingItem` Bi
              CROSS JOIN DateRange D
              WHERE Bi.OrderDate < date_sub(CURRENT_DATE(), interval 1 day)
              AND Bi.OrderDate >= D.DatesFrom
              GROUP BY 1) P ON P.SupplierCode = f.SupplierCode
LEFT JOIN (SELECT
              Bi.SupplierCode,
              Bi.ProductCode,
              COUNT(DISTINCT Bi.ItineraryItemID)  AS PastBookings,
              SUM(Bi.GrossBookingUSD) AS PastGBV 
              FROM `avr-warehouse.ATTRData.vwBookingItem` Bi
              CROSS JOIN DateRange D
              WHERE Bi.OrderDate < date_sub(CURRENT_DATE(), interval 1 day)
              AND Bi.OrderDate >= D.DatesFrom
              GROUP BY 1,2) Pro ON Pro.ProductCode = f.ProductCode
LEFT JOIN (select distinct bi.suppliercode
              ,CONCAT(format_date("%Y-%m", date(bi.OrderDate) ) , '-01' ) as Date
              ,sum(bi.GrossBookingUSD)          as GBV
              ,sum(bi.OperatingIncomeUSD)       as OperatingIncome              
              ,count(distinct itineraryitemid)  as Bookings
              ,sum(case when iscancelled =1 then 1 else 0 end) as CancelledBookings
              ,sum(case when iscancelled =1 then GrossBookingUSD else 0 end) as CancelledGBV
              from `avr-warehouse.ATTRData.vwBookingItem` bi
              CROSS JOIN DateRange D
              where orderdate >= D.datesfrom
              group by 1,2) supprev on f.Date = supprev.Date and f.suppliercode = supprev.suppliercode
where contractacceptancedate is not null;