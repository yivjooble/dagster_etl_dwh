select
    a.id,
    a.name,
    convert(varchar(100), dateadd(hour, 3, a.CreatedOn), 20) as created_on,
    Contact.name                                             as owner_name,
    Contact.Email                                            as owner_email,
    Country.Alpha2Code                                       as country,
    UsrFoundInSources.name                                   as found_in,
    UsrReferralOfPotentialClients.name                       as source_type,
    UsrClientSegment.name                                    as client_segment,
    UsrSpecialization.name                                   as specialization,
    Usrtargetaction.name                                     as target_action,
    case 
    when a.UsrProblemsMulti = '' then null
    else
    a.UsrProblemsMulti end                                   as problem_segments,
    cast(a.UsrCPAprice2 as int)                              as is_target_cpa,
    a.UsrCPApricebenchmarktargetmetric                       as benchmark_cpa,
    cast(a.UsrClickcount as int)                             as is_target_clicks,
    a.UsrClickcountbenchmarktargetmetric                     as benchmark_clicks,
    cast(a.UsrConversioncount as int)                        as is_target_conversions,
    a.UsrConversioncountbenchmarktargetmetric                as benchmark_conversions,
    cast(a.UsrConversionrate as int)                         as is_target_cr,
    a.UsrConversionratebenchmarktargetmetric                 as benchmark_cr,
    cast(a.UsrBuysAtPja as int)                              as is_buys_at_pja,
    AccountPJA.name                                          as via_pja,
    iif(a.UsrSoskaCabinet != '',
        upper(replace(substring(a.UsrSoskaCabinet, 33, 20), '/', '')),
        null)                                                as soska_user_link,
    iif(a.UsrSoskaJproject != '',
        upper(replace(substring(a.UsrSoskaJproject, 34, 20), '/', '')),
        null)                                                as soska_project_link,
    a.PrimaryContactId                                       as primary_contact_id,
    iif(a.UsrId1C != '', a.UsrId1C, null)                    as number_1c,
    a.UsrClientsUTM as client_utm,
    a.UsrTrafficResources as traffic_resources,
    a.UsrTrafficflow as traffic_flow
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
     left join UsrSpecialization
               on a.UsrSpecializationId = UsrSpecialization.Id
     left join Usrtargetaction
               on Usrtargetaction.Id = a.UsrtargetactionId
     left join Account as AccountPJA
               on a.UsrProgrammaticId = AccountPJA.id
