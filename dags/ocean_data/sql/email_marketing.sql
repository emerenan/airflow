SELECT
    {year} as year,
    {week_number} as week_number,
    Date,
    cea.AnalyticsType,
    count(distinct case when lastsitevisit >= date_sub(Date, interval 12 month) then cea.SubscriberKey END) as repeatbrowsersends,
    count(distinct case when lastsitevisit is null or lastsitevisit < date_sub(Date, interval 12 month) then cea.SubscriberKey END) as newbrowsersends,
    count(distinct case when lasttransactiondate between date_sub(Date, interval 12 month) and Date then cea.SubscriberKey END) as repeatbookersends,
    count(distinct case when lasttransactiondate is null or lasttransactiondate< date_sub(Date, interval 12 month) then cea.SubscriberKey END) as newbookersends,
    sum(case when lastsitevisit >= date_sub(Date, interval 12 month) then cea.UniqueOpens END) as repeatbrowseropens,
    sum(case when lastsitevisit is null or lastsitevisit < date_sub(Date, interval 12 month) then cea.UniqueOpens END) as newbrowseropens,
    sum(case when lasttransactiondate between date_sub(Date, interval 12 month) and Date then cea.UniqueOpens END) as repeatbookeropens,
    sum(case when lasttransactiondate is null or lasttransactiondate< date_sub(Date, interval 12 month) then cea.UniqueOpens END) as newbookeropens,
    sum(case when lastsitevisit >= date_sub(Date, interval 12 month) then cea.UniqueClicks END) as repeatbrowserclicks,
    sum(case when lastsitevisit is null or lastsitevisit < date_sub(Date, interval 12 month) then cea.UniqueClicks END) as newbrowserclicks,
    sum(case when lasttransactiondate between date_sub(Date, interval 12 month) and Date then cea.UniqueClicks END) as repeatbookerclicks,
    sum(case when lasttransactiondate is null or lasttransactiondate< date_sub(Date, interval 12 month) then cea.UniqueClicks END) as newbookerclicks,
    count(distinct(ms.SubscriberKey)) as totalusers
FROM `avr-warehouse.CRM.vwCRMEmailActivityAll` cea
left join `avr-analysts.mlowe.MemberSnapshot` ms
    on lower(cea.SubscriberKey) = lower(ms.SubscriberKey)
    and date(ms.Date) = date(cea.SendDateEST)
left join `avr-warehouse.ATTRTableau.BookingItemCustomer` bic
    on lower(ms.EmailAddress) = lower(bic.Email)
WHERE Date BETWEEN "{min-event-date}" and "{max-event-date}"
group by Date, cea.AnalyticsType