select uct.id                                     as test_id,
       uct.UsrAccountId                           as usr_account_id,
       uct.CreatedOn                              as test_created_date,
       uct.UsrTestStartdate                       as usr_test_startdate,
       uct.UsrTestEnddate                         as usr_test_enddate,
       uct.UsrName                                as test_user_name,
       o.title                                    as opps_name,
       c_created.name                             as test_created_name,
       c_opp_owner.name                           as opp_owner_name,
       OpportunityStage.name                      as opportunity_stage,
       Country.Alpha2Code                         as opportunity_country,
       trans_close.Language0                      as opportunity_close_reason,
       trans_cancel.Language0                     as opportunity_cancel_reason,
       stage_losing.name                          as opportunity_stage_before_losing,
       convert(varchar(100), o.UsrDateLosing, 20) as opportunity_losing_date,
       o.Amount as amount
from UsrCustomerTest uct
         left join opportunity o on o.AccountId = uct.UsrAccountId
         left join Contact c_created
                   on uct.CreatedById = c_created.Id
         left join Contact c_opp_owner
                   on o.OwnerId = c_opp_owner.Id
         left join OpportunityStage
                   on OpportunityStage.Id = o.StageId
         left join Country
                   on o.UsrCountryLookup1Id = Country.Id
         left join OpportunityCloseReason
                   on OpportunityCloseReason.Id = o.CloseReasonId
         left join OpportunityStage stage_losing
                   on o.UsrStageBeforeLosingId = stage_losing.Id
         left join UsrPotClientSaleCancelReasom
                   on o.UsrCancellationReasonId = UsrPotClientSaleCancelReasom.id
         left join SysTranslation trans_close on
    trans_close.Language1 = OpportunityCloseReason.name and trans_close.[key] like '%CloseReason%'
    left join SysTranslation trans_cancel
on
    trans_cancel.Language1 = UsrPotClientSaleCancelReasom.name and trans_cancel.[key] like '%UsrPotClientSaleCancelReasom%';