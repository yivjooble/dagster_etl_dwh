Select UsrStageinCases.UsrCasesId       as case_row_id,
       UsrCases.UsrCaseNumber           as case_number,
       UsrStageinCases.UsrStageInCaseId as stage_id,
       UsrStageInCase.UsrName           as stage_name,
       UsrStageinCases.UsrOwnerId       as owner_id,
       Contact.Name                     as owner_name,
       UsrStageinCases.UsrStartDate     as date_stg_start,
       UsrStageinCases.UsrDueDate       as date_stg_end,
       UsrStageinCases.UsrHistorical    as is_finished
FROM UsrStageinCases
         LEFT JOIN UsrCases ON UsrCases.Id = UsrStageinCases.UsrCasesId
         LEFT JOIN UsrStageInCase ON UsrStageinCases.UsrStageInCaseId = UsrStageInCase.Id
         LEFT JOIN Contact ON UsrStageinCases.UsrOwnerId = Contact.Id
;