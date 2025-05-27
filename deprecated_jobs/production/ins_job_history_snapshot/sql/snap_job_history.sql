SET NOCOUNT ON;

select uid,
       id_similar_group,
       id_region,
       title,
       id_project,
       salary_val1,
       id_currency,
       id_salary_rate,
       company_name,
       date_diff,
       id_category
from dbo.job_history
;