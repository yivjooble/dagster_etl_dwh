select
    c.id,
    c.name,
    convert(varchar(100), dateadd(hour, 3, c.CreatedOn), 20) as created_on,
    manager.name                                             as owner_name,
    manager.Email                                            as owner_email,
    c.AccountId                                              as account_id,
    iif(c.JobTitle != '', c.JobTitle, null)                  as job_title,
    Department.name                                          as department,
    ContactDecisionRole.name                                 as role_name,
    iif(c.Phone != '', c.Phone, null)                        as business_phone,
    iif(c.MobilePhone != '', c.MobilePhone, null)            as mobile_phone,
    iif(c.Email != '', c.Email, null)                        as email
from
    Contact c
    join Account
              on Account.Id = c.AccountId
    left join ContactDecisionRole
              on ContactDecisionRole.id = c.DecisionRoleId
    left join Department
              on Department.id = c.DepartmentId
    left join Contact as manager
              on c.OwnerId = manager.Id
