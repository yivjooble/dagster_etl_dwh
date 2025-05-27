SET NOCOUNT ON;


select id_test,
       id_account,
       id_group,
       date_diff,
       flags
from dbo.email_account_test
;