with opportunities as (
    select
        o.id as opportunity_id,
        o.AccountId as customer_id,
        o.Title as opportunity_name,
        c_owner.name as opportunity_owner_name,
        c_owner.Email as opportunity_owner_email,
        c_created.name as opportunity_created_name,
        c_created.Email as opportunity_created_email,
        o.Amount as opportunity_amount,
        trans_found.Language0 as opportunity_found_in,
        Country.Alpha2Code as opportunity_country,
        OpportunityStage.name as opportunity_stage,
        trans_close.Language0 as opportunity_close_reason,
        trans_cancel.Language0 as opportunity_cancel_reason,
        stage_losing.name as opportunity_stage_before_losing,
        convert(varchar(100), o.UsrDateLosing) as opportunity_losing_date,
        convert(varchar(100), dateadd(hour, 3, CAST(o.CreatedOn AS datetime2))) as opportunity_created_date,
        convert(varchar(100), dateadd(hour, 3, CAST(o.DueDate AS datetime2))) as opportunity_due_date
    from Opportunity o
    left join Contact c_owner
        on o.OwnerId = c_owner.Id
    left join Contact c_created
        on o.CreatedById = c_created.Id
    left join Country
        on o.UsrCountryLookup1Id = Country.Id
    left join UsrFoundInSources
        on UsrFoundInSources.Id = o.UsrFoundInId
    left join OpportunityStage
        on OpportunityStage.Id = o.StageId
    left join OpportunityCloseReason
        on OpportunityCloseReason.Id = o.CloseReasonId
    left join OpportunityStage stage_losing
        on o.UsrStageBeforeLosingId = stage_losing.Id
    left join UsrPotClientSaleCancelReasom
        on o.UsrCancellationReasonId = UsrPotClientSaleCancelReasom.id
    left join SysTranslation trans_found on
    trans_found.Language1 = UsrFoundInSources.name and trans_found.[key] like '%FoundIN%'
    left join SysTranslation trans_close on
    trans_close.Language1 = OpportunityCloseReason.name and trans_close.[key] like '%CloseReason%'
    left join SysTranslation trans_cancel on
    trans_cancel.Language1 = UsrPotClientSaleCancelReasom.name and trans_cancel.[key] like '%UsrPotClientSaleCancelReasom%'


),
accounts as (
    select
        a.id as account_id,
        a.name as account_name,
        Contact.name as account_owner_name,
        Contact.Email as account_owner_email,
        Country.Alpha2Code as account_country,
        UsrFoundInSources.name as account_found_in,
        UsrReferralOfPotentialClients.name as account_source_type,
        UsrClientSegment.name as account_client_segment,
        a.usrPotentialSalesSites as account_sales_potential
    from Account a
    left join Contact
        on a.OwnerId = Contact.Id
    left join Country
        on a.CountryId = Country.Id
    left join UsrFoundInSources
        on UsrFoundInSources.Id = a.UsrFoundInId
    left join UsrReferralOfPotentialClients
        on UsrReferralOfPotentialClients.Id = a.UsrReferralOfPotentialClientsId
    left join UsrClientSegment
        on UsrClientSegment.Id = a.UsrSalesPotenrialId
),
activities as (
    select
        a.id as activity_id,
        a.opportunityid as opportunity_id,
        a.accountid as account_id,
        trans.Language0 as activity_result,
        iif(res.name is not null, 1, 0) as has_result,
        convert(varchar(100), dateadd(hour, 3, a.startdate), 20) as activity_start_date,
        convert(varchar(100), dateadd(hour, 3, a.DueDate), 20) as activity_due_date
    from activity a
    left join ActivityStatus st
        on a.statusid = st.id
    left join ActivityResult res
        on a.resultid = res.id
    left join SysTranslation trans on
    trans.Language1 = res.name and trans.[key] like '%ActivityResult%'
    where
        a.opportunityid is not null
        and st.code = 'Finished'
),
activities1 as (
    select
        activity_id,
        opportunity_id,
        account_id,
        activity_result,
        has_result,
        activity_start_date,
        activity_due_date,
        row_number() over(partition by opportunity_id order by has_result desc, activity_start_date desc) as row_num
    from activities
)
select
    a.account_id,
    account_name,
    account_owner_name,
    account_owner_email,
    account_country,
    account_found_in,
    account_source_type,
    account_client_segment,
    account_sales_potential,
    o.opportunity_id,
    opportunity_name,
    opportunity_owner_name,
    opportunity_owner_email,
    opportunity_created_name,
    opportunity_created_email,
    opportunity_amount,
    opportunity_found_in,
    opportunity_country,
    opportunity_stage,
    opportunity_close_reason,
    opportunity_cancel_reason,
    opportunity_stage_before_losing,
    opportunity_losing_date,
    opportunity_created_date,
    opportunity_due_date,
    activity_id,
    activity_result,
    activity_start_date,
    activity_due_date
from accounts a
left join opportunities o
    on o.customer_id = a.account_id
left join (select * from activities1 where row_num = 1) ac
    on o.opportunity_id = ac.opportunity_id
