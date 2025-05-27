create temp table job_last_month as
  SELECT j.date_created,
         j.date_expired,
         j.id_region,
         j.title,
         j.job_type1,
         j.id_category,
         j.id,
         j.id_similar_group
FROM public.job j (nolock)
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
Group by j.date_created,
         j.date_expired,
         j.id_region,
         j.title,
         j.job_type1,
         j.id_category,
         j.id,
         j.id_similar_group
;

create temp table j_by_yymm as
SELECT  to_char(j.date_created, 'YYYYMM')  as year_month,
        count(distinct j.id_similar_group) as unique_jobs_cnt,
        count(distinct j.id)               as jobs_cnt
FROM job_last_month j
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
        and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
Group by to_char(j.date_created, 'YYYYMM')
;

create temp table j_by_yymm_region_top as
SELECT *, row_number() over ( order by a.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                j.id_region                        as id_region,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
         Group by to_char(j.date_created, 'YYYYMM'), j.id_region
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )a
;

create temp table j_by_yymm_vacancy_top as
SELECT *, row_number() over ( order by a.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                lower(j.title)                     as title,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
         Group by to_char(j.date_created, 'YYYYMM'), lower(j.title)
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )a
;

create temp table j_by_yymm_student_total as
SELECT  to_char(j.date_created, 'YYYYMM')  as year_month,
        count(distinct j.id_similar_group) as unique_jobs_cnt,
        count(distinct j.id) as jobs_cnt
FROM job_last_month j
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
        and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
        and (lower(j.title) like '%%étudiant%%'
                or lower(j.title) like '%%etudiant%%'
                or lower(j.title) like '%%student%%'
                or lower(j.title) like '%%studierend%%'
                or lower(j.title) like '%%diákmunka%%'
                or lower(j.title) like '%%bijbaan%%'
                or lower(j.title) like '%%studenți%%'
                or lower(j.title) like '%%studentski%%'
    )
Group by to_char(j.date_created, 'YYYYMM')
;

create temp table j_by_yymm_student_region_top as
SELECT *, row_number() over ( order by tb.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                j.id_region                        as id_region,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
              and (lower(j.title) like '%%étudiant%%'
                or lower(j.title) like '%%etudiant%%'
                or lower(j.title) like '%%student%%'
                or lower(j.title) like '%%studierend%%'
                or lower(j.title) like '%%diákmunka%%'
                or lower(j.title) like '%%bijbaan%%'
                or lower(j.title) like '%%studenți%%'
                or lower(j.title) like '%%studentski%%'
                      )
         Group by to_char(j.date_created, 'YYYYMM'), j.id_region
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )tb
;

create temp table j_by_yymm_student_vacancy_top as
SELECT *, row_number() over ( order by bbb.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                lower(j.title)                     as title,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
              and (lower(j.title) like '%%étudiant%%'
                or lower(j.title) like '%%etudiant%%'
                or lower(j.title) like '%%student%%'
                or lower(j.title) like '%%studierend%%'
                or lower(j.title) like '%%diákmunka%%'
                or lower(j.title) like '%%bijbaan%%'
                or lower(j.title) like '%%studenți%%'
                or lower(j.title) like '%%studentski%%'
                      )
         Group by to_char(j.date_created, 'YYYYMM'), lower(j.title)
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )bbb
;

create temp table j_by_yymm_season_total as
SELECT  to_char(j.date_created, 'YYYYMM')  as year_month,
        count(distinct j.id_similar_group) as unique_jobs_cnt,
        count(distinct j.id) as jobs_cnt
FROM job_last_month j
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
              and (lower(j.title) like '%%saisonarbeit%%'
                or lower(j.title) like '%%befristete beschäftigung%%'
                or lower(j.title) like '%%saisonjob%%'
                or lower(j.title) like '%%saisonnier%%'
                or lower(j.title) like '%%travail temporaire%%'
                or lower(j.title) like '%%seasonal%%'
                or lower(j.title) like '%%holiday%%'
                or lower(j.title) like '%%summer%%'
                or lower(j.title) like '%%winter%%'
                or lower(j.title) like '%%seizoensarbeid%%'
                or lower(j.title) like '%%szezonális munka%%'
                or lower(j.title) like '%%szezonális%%'
                or lower(j.title) like '%%seizoenswerk%%'
                or lower(j.title) like '%%sezonowa%%'
                or lower(j.title) like '%%sezonieră%%'
                or lower(j.title) like '%%sezonski%%'
                or lower(j.title) like '%%de vacances%%'
                or lower(j.title) like '%%ferienjob%%'
                or lower(j.title) like '%%ferialjob%%'
                     )
Group by to_char(j.date_created, 'YYYYMM')
;

create temp table j_by_yymm_season_region_top as
SELECT *, row_number() over ( order by tb.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                j.id_region                        as id_region,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
              and (lower(j.title) like '%%saisonarbeit%%'
                or lower(j.title) like '%%befristete beschäftigung%%'
                or lower(j.title) like '%%saisonjob%%'
                or lower(j.title) like '%%saisonnier%%'
                or lower(j.title) like '%%travail temporaire%%'
                or lower(j.title) like '%%seasonal%%'
                or lower(j.title) like '%%holiday%%'
                or lower(j.title) like '%%summer%%'
                or lower(j.title) like '%%winter%%'
                or lower(j.title) like '%%seizoensarbeid%%'
                or lower(j.title) like '%%szezonális munka%%'
                or lower(j.title) like '%%szezonális%%'
                or lower(j.title) like '%%seizoenswerk%%'
                or lower(j.title) like '%%sezonowa%%'
                or lower(j.title) like '%%sezonieră%%'
                or lower(j.title) like '%%sezonski%%'
                or lower(j.title) like '%%de vacances%%'
                or lower(j.title) like '%%ferienjob%%'
                or lower(j.title) like '%%ferialjob%%'
                     )
         Group by to_char(j.date_created, 'YYYYMM'), j.id_region
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )tb
;

create temp table j_by_yymm_season_vacancy_top as
SELECT *, row_number() over ( order by bbb.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                lower(j.title)                     as title,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
              and (lower(j.title) like '%%saisonarbeit%%'
                or lower(j.title) like '%%befristete beschäftigung%%'
                or lower(j.title) like '%%saisonjob%%'
                or lower(j.title) like '%%saisonnier%%'
                or lower(j.title) like '%%travail temporaire%%'
                or lower(j.title) like '%%seasonal%%'
                or lower(j.title) like '%%holiday%%'
                or lower(j.title) like '%%summer%%'
                or lower(j.title) like '%%winter%%'
                or lower(j.title) like '%%seizoensarbeid%%'
                or lower(j.title) like '%%szezonális munka%%'
                or lower(j.title) like '%%szezonális%%'
                or lower(j.title) like '%%seizoenswerk%%'
                or lower(j.title) like '%%sezonowa%%'
                or lower(j.title) like '%%sezonieră%%'
                or lower(j.title) like '%%sezonski%%'
                or lower(j.title) like '%%de vacances%%'
                or lower(j.title) like '%%ferienjob%%'
                or lower(j.title) like '%%ferialjob%%'
                     )
         Group by to_char(j.date_created, 'YYYYMM'), lower(j.title)
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )bbb
;

create temp table j_by_yymm_jobtype_total as
SELECT  to_char(j.date_created, 'YYYYMM')   as year_month,
        j.job_type1                         as job_type,
        count(distinct j.id_similar_group)  as unique_jobs_cnt,
        count(distinct j.id)                as jobs_cnt
FROM job_last_month j
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
        and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
        and j.job_type1 in (2,3,4,5)
Group by to_char(j.date_created, 'YYYYMM'),j.job_type1
;

create temp table j_by_yymm_jobtype_parttime_region_top as
SELECT *, row_number() over ( order by bgg.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                j.id_region                        as id_region,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
           and j.job_type1=3
         Group by to_char(j.date_created, 'YYYYMM'), j.id_region
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )bgg
;

create temp table j_by_yymm_jobtype_parttime_vacancy_top as
SELECT *, row_number() over ( order by cc.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                lower(j.title)                     as title,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
            and j.job_type1=3
         Group by to_char(j.date_created, 'YYYYMM'), lower(j.title)
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )cc
;

create temp table j_by_yymm_category_total as
SELECT  to_char(j.date_created, 'YYYYMM')  as year_month,
        j.id_category,
        count(distinct j.id_similar_group) as unique_jobs_cnt,
        count(distinct j.id) as jobs_cnt
FROM job_last_month j
WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
        and (j.date_expired is null or cast(j.date_expired as date)  > %(previous_month_end)s)
Group by to_char(j.date_created, 'YYYYMM'),j.id_category
;

create temp table j_by_yymm_abroad as
SELECT *, row_number() over ( order by a.unique_jobs_cnt desc ) AS TOPNo
FROM (
        SELECT  to_char(j.date_created, 'YYYYMM')  as year_month,
                j.id_region,
                count(distinct j.id_similar_group) as unique_jobs_cnt,
                count(distinct j.id)               as jobs_cnt
        FROM job_last_month j
        WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
                and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
                and ARRAY[j.id_region] && (%(abroad_region)s)
         Group by to_char(j.date_created, 'YYYYMM'),j.id_region
         Order by count(distinct j.id_similar_group) desc
         )a
;

create temp table j_by_yymm_vacancy_top_abroad as
SELECT *, row_number() over ( order by a.unique_jobs_cnt desc ) AS TOPNo
FROM (
         SELECT to_char(j.date_created, 'YYYYMM')  as year_month,
                lower(j.title)                     as title,
                count(distinct j.id)               as jobs_cnt,
                count(distinct j.id_similar_group) as unique_jobs_cnt
         FROM job_last_month j
         WHERE cast(j.date_created as date) between %(previous_month_start)s and %(previous_month_end)s
           and (j.date_expired is null or cast(j.date_expired as date) > %(previous_month_end)s)
           and ARRAY[j.id_region] && (%(abroad_region)s)
         Group by to_char(j.date_created, 'YYYYMM'), lower(j.title)
         Order by count(distinct j.id_similar_group) desc
         Limit 30
         )a
;

-- final union with all segments
create temp table  final_union as
    Select
           j1.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'total'                       as metric_type,
           j1.jobs_cnt,
           j1.unique_jobs_cnt
    FROM j_by_yymm j1
    union all
    Select
           top5.year_month,
           top5.id_region,
           null::text             as title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           top5.TOPNo,
           'jobs_by_region_top'   as metric,
           'total'                as metric_type,
           top5.jobs_cnt,
           top5.unique_jobs_cnt
    FROM j_by_yymm_region_top top5
    union all
    Select
           j25.year_month,
           null::integer           as id_region,
           j25.title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           j25.TOPNo,
           'jobs_by_vacancy_top'   as metric,
           'total'                 as metric_type,
           j25.jobs_cnt,
           j25.unique_jobs_cnt
    FROM j_by_yymm_vacancy_top j25
    union all
	Select
           j34.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'student'                     as metric_type,
           j34.jobs_cnt,
           j34.unique_jobs_cnt
    FROM j_by_yymm_student_total j34
    union all
    Select
           top4.year_month,
           top4.id_region,
           null::text             as title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           top4.TOPNo,
           'jobs_by_region_top'   as metric,
           'student'              as metric_type,
           top4.jobs_cnt,
           top4.unique_jobs_cnt
    FROM j_by_yymm_student_region_top top4
    union all
    Select
           j24.year_month,
           null::integer          as id_region,
           j24.title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           j24.TOPNo,
           'jobs_by_vacancy_top'   as metric,
           'student'               as metric_type,
           j24.jobs_cnt,
           j24.unique_jobs_cnt
    FROM j_by_yymm_student_vacancy_top j24
    union all
	Select
           j34.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'seasonal'                    as metric_type,
           j34.jobs_cnt,
           j34.unique_jobs_cnt
    FROM j_by_yymm_season_total j34
    union all
    Select
           top4.year_month,
           top4.id_region,
           null::text             as title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           top4.TOPNo,
           'jobs_by_region_top'   as metric,
           'seasonal'             as metric_type,
           top4.jobs_cnt,
           top4.unique_jobs_cnt
    FROM j_by_yymm_season_region_top top4
    union all
    Select
           j24.year_month,
           null::integer          as id_region,
           j24.title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           j24.TOPNo,
           'jobs_by_vacancy_top'  as metric,
           'seasonal'             as metric_type,
           j24.jobs_cnt,
           j24.unique_jobs_cnt
    FROM j_by_yymm_season_vacancy_top j24
    union all
	Select
           j33.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'temporary_jobtype'           as metric_type,
           j33.jobs_cnt,
           j33.unique_jobs_cnt
    FROM j_by_yymm_jobtype_total j33
        where j33.job_type=2
    union all
	Select
           j32.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'parttime_jobtype'            as metric_type,
           j32.jobs_cnt,
           j32.unique_jobs_cnt
    FROM j_by_yymm_jobtype_total j32
        where j32.job_type=3
    union all
	Select
           j31.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           null::smallint                as TOPNo,
           'jobs_by_month'               as metric,
           'forUA_jobtype'               as metric_type,
           j31.jobs_cnt,
           j31.unique_jobs_cnt
    FROM j_by_yymm_jobtype_total j31
        where j31.job_type=5
    union all
    Select
           top.year_month,
           top.id_region,
           null::text             as title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           top.TOPNo,
           'jobs_by_region_top'   as metric,
           'parttime_jobtype'     as metric_type,
           top.jobs_cnt,
           top.unique_jobs_cnt
    FROM j_by_yymm_jobtype_parttime_region_top top
    union all
    Select
           j2.year_month,
           null::integer           as id_region,
           j2.title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           j2.TOPNo,
           'jobs_by_vacancy_top'   as metric,
           'parttime_jobtype'      as metric_type,
           j2.jobs_cnt,
           j2.unique_jobs_cnt
    FROM j_by_yymm_jobtype_parttime_vacancy_top j2
    union all
	Select
           j3.year_month,
           null::integer                 as id_region,
           null::text                    as title,
           null::double precision        as salary_val1,
           null::double precision        as salary_val2,
           null::smallint                as id_currency,
           null::smallint                as id_salary_rate,
           j3.id_category                as TOPNo,
           'jobs_by_month'               as metric,
           'category_id'                 as metric_type,
           j3.jobs_cnt,
           j3.unique_jobs_cnt
    FROM j_by_yymm_category_total j3
    union all
    Select
           toptop.year_month,
           toptop.id_region,
           null::text             as title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           toptop.TOPNo,
           'jobs_by_region_top'   as metric,
           'abroad'               as metric_type,
           toptop.jobs_cnt,
           toptop.unique_jobs_cnt
    FROM j_by_yymm_abroad toptop
    union all
    Select
           j21.year_month,
           null::integer           as id_region,
           j21.title,
           null::double precision as salary_val1,
           null::double precision as salary_val2,
           null::smallint         as id_currency,
           null::smallint         as id_salary_rate,
           j21.TOPNo,
           'jobs_by_vacancy_top'   as metric,
           'abroad'                as metric_type,
           j21.jobs_cnt,
           j21.unique_jobs_cnt
    FROM j_by_yymm_vacancy_top_abroad j21
;

Select %(db_name)s  as country_code,
       %(country_id)s  as country_id,
       u.year_month,
       u.id_region as region_id,
       u.title,
       u.salary_val1,
       u.salary_val2,
       u.id_currency as currency_id,
       u.id_salary_rate as salary_rate_id,
       u.TOPNo as top_num,
       u.metric,
       u.metric_type,
       u.jobs_cnt,
       u.unique_jobs_cnt
FROM final_union u;

