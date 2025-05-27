def generate_aggregation_query_to_run(
        date_from,
        date_to,
        db):
    concatenated_command = f'''
       insert into {db}.pr_tmp_raw(date_created, date_expired, id, id_similar_group, id_region, title, job_text, company_name, salary_val1, salary_val2,
                       id_currency, id_salary_rate, job_type1, job_type2, id_category, city_name, region_name, remote_type,
                       abroad_type, fulltime_type, temporary_type, parttime_type, internship_type)
       Select job.date_created,
              job.date_expired,
              job.id,
              job.id_similar_group,
              job.id_region,
              job.title,
              job.job_text,
              job.company_name,
              job.salary_val1,
              job.salary_val2,
              job.id_currency,
              job.id_salary_rate,
              job.job_type1,
              job.job_type2,
              job.id_category,
              case when ir.is_city = 1 then ir.name end as city_name,
              ir2.name                                  as region_name,
              --
              case when (ir.name = 'Remote'
                         or (ir.is_toplevel = 1 and ir.order_value = 1000)
                         or job.remote_type = 1
                         or lower(job.title) like '%remote%'
                     or lower(job.title) like '%online% '
                     or lower(job.title) like '%work from home%'
                     or lower(job.title) like '%from home%'
                     or lower(job.title) like '%homeoffice%') then 1 else 0 end                       as remote_type,
              case when ir.order_value = -1 then 1 else 0 end                                         as abroad_type,
              case when job.job_type1 = 0 or job.job_type1 = 1 or job.job_type2 = 1 then 1 else 0 end as fulltime_type,
              case when job.job_type1 = 2 or job.job_type2 = 2 then 1 else 0 end                      as temporary_type,
              case when job.job_type1 = 3 or job.job_type2 = 3 then 1 else 0 end                      as parttime_type,
              case when job.job_type1 = 4 or job.job_type2 = 4 then 1 else 0 end                      as internship_type
              --
       FROM
            -- getting job info from history db
            (
                   SELECT after.date_created as date_created,
                          after.date_expired as date_expired,
                          after.id as id,
                          after.id_similar_group as id_similar_group,
                          after.id_region as id_region,
                          after.title as title,
                          null as job_text,
                          after.company_name as company_name,
                          after.salary_val1 as salary_val1,
                          after.salary_val2 as salary_val2,
                          after.id_currency as id_currency,
                          after.id_salary_rate as id_salary_rate,
                          after.job_type1 as job_type1,
                          after.job_type2 as job_type2,
                          after.id_category as id_category,
                          after.remote_type as remote_type
                   FROM {db}_job
                   where toDate(after.date_created) between toDate('{date_from}') and toDate('{date_to}')       
            ) as job 
            -- end of getting job info from history db
                left join info_region ir ON job.id_region = ir.id
                left join info_region_submission irs ON irs.id_region = ir.id
                left join info_region ir2 ON ir2.id = irs.id_region_parent
       
       '''

    return concatenated_command
