with firstclosed as (								
    select ch.CaseId								
    , ch.oldvalue								
    , ch.newvalue								
    , TIMESTAMP(REPLACE(ch.CreatedDate,"+0000","")) AS StatusChangeDate								
    , ROW_NUMBER() OVER (PARTITION BY ch.CaseId ORDER BY TIMESTAMP(REPLACE(ch.CreatedDate,"+0000","")) ) as OrderNum								
    from `avr-warehouse.ATTRSalesForce.CaseHistory` ch								
    inner join `avr-warehouse.SharedCustomerCare.ChatMessageEXP` chat on ch.CaseId = chat.CaseId								
    where lower(ch.field) = 'status'
    and lower(ch.newvalue) in ( 'resolved' , 'waiting for response' )
    order by 1,4								
    )								
,chats as (								
    select ch.CaseCreatedTimeStampUTC  as CaseCreatedUTC								
    ,fc.StatusChangeDate               as CaseFirstStatusChangeUTC
    ,ch.CaseClosedTimeStampUTC         as CaseClosedUTC								
    ,ch.ChatCreatedTimeStampUTC        as ChatCreatedUTC								
    ,ch.ChatStartTimeStampUTC          as ChatStartTimeUTC								
    ,ch.ChatEndTimestampUTC            as ChatEndTimeUTC								
    ,ch.ChatId								
    ,ch.CaseId								
    ,ch.CaseNumber								
    ,ch.CaseRecordType								
    ,ch.ChatStatus								
    ,ch.AgentEmail								
    ,ch.AgentRole								
    ,ch.ChatCount								
    ,ch.ChatDurationInSeconds														
    from `avr-warehouse.SharedCustomerCare.ChatMessageEXP` ch								
    left join firstclosed fc on ch.CaseId = fc.CaseId and fc.OrderNum =1								
    where CaseRecordType in (
          'Account Review',
          'Action Alert',
          'Finance',
          'Finance - Supplier',
          'Listing Issue',
          'New Destination',
          'Product Creation',
          'Product Issue',
          'Product Quality Review',
          'Product Review',
          'Special Project',
          'Supplier Connectivity',
          'Supplier Sales',
          'Supplier Support'  )						
    and date(ChatCreatedTimeStampUTC) >= '2021-11-01'
    and agentemail <> 'salesforce-api@viator.com'								
    )
,final as (								
    select ch.* 								
    ,aw.ActiveTimeSec
    ,timestamp_diff(CaseFirstStatusChangeUTC,ChatEndTimeUTC, second) as ChatEndToCaseFirstStatusChange
    from chats ch
    left join ( select distinct aw.WorkItemId, sum(ActiveTimeSec) as ActiveTimeSec
                from `avr-warehouse.SharedCustomerCare.SalesForceAgentWorkEXP` aw
                where activetimesec >0
                and tasktype = 'Chat'
                group by 1 ) aw on ch.ChatId = aw.WorkItemId
    )								
select 
    f.* 		
    ,case 
        when ChatEndToCaseFirstStatusChange between 0 and 180 then ChatEndToCaseFirstStatusChange 
        when ChatEndToCaseFirstStatusChange <0 then 0
        else 180 
    end as ChatWrapUpSeconds
From final f

