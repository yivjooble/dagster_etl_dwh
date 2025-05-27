select 
       (select case when (select current_database()) = 'ua' then 1 when (select current_database()) = 'de'
                then 2 when (select current_database()) = 'uk' then 3 when (select current_database()) = 'fr' then 4 when (select current_database()) = 'ca'
                then 5 when (select current_database()) = 'us' then 6 when (select current_database()) = 'id' then 7 when (select current_database()) = 'ru'
                then 8 when (select current_database()) = 'pl' then 9 when (select current_database()) = 'hu' then 10 when (select current_database()) = 'ro'
                then 11 when (select current_database()) = 'es' then 12 when (select current_database()) = 'at' then 13 when (select current_database()) = 'be'
                then 14 when (select current_database()) = 'br' then 15 when (select current_database()) = 'ch' then 16 when (select current_database()) = 'cz'
                then 17 when (select current_database()) = 'in' then 18 when (select current_database()) = 'it' then 19 when (select current_database()) = 'nl'
                then 20 when (select current_database()) = 'tr' then 21 when (select current_database()) = 'by' then 22 when (select current_database()) = 'cl'
                then 23 when (select current_database()) = 'co' then 24 when (select current_database()) = 'gr' then 25 when (select current_database()) = 'sk'
                then 26 when (select current_database()) = 'th' then 27 when (select current_database()) = 'tw' then 28 when (select current_database()) = 've'
                then 29 when (select current_database()) = 'bg' then 30 when (select current_database()) = 'hr' then 31 when (select current_database()) = 'kz'
                then 32 when (select current_database()) = 'no' then 33 when (select current_database()) = 'rs' then 34 when (select current_database()) = 'se'
                then 35 when (select current_database()) = 'nz' then 36 when (select current_database()) = 'ng' then 37 when (select current_database()) = 'ar'
                then 38 when (select current_database()) = 'mx' then 39 when (select current_database()) = 'pe' then 40 when (select current_database()) = 'cn'
                then 41 when (select current_database()) = 'hk' then 42 when (select current_database()) = 'kr' then 43 when (select current_database()) = 'ph'
                then 44 when (select current_database()) = 'pk' then 45 when (select current_database()) = 'jp' then 46 when (select current_database()) = 'cu'
                then 47 when (select current_database()) = 'pr' then 48 when (select current_database()) = 'sv' then 49 when (select current_database()) = 'cr'
                then 50 when (select current_database()) = 'au' then 51 when (select current_database()) = 'do' then 52 when (select current_database()) = 'uy'
                then 53 when (select current_database()) = 'ec' then 54 when (select current_database()) = 'sg' then 55 when (select current_database()) = 'az'
                then 56 when (select current_database()) = 'fi' then 57 when (select current_database()) = 'ba' then 58 when (select current_database()) = 'pt'
                then 59 when (select current_database()) = 'dk' then 60 when (select current_database()) = 'ie' then 61 when (select current_database()) = 'my'
                then 62 when (select current_database()) = 'za' then 63 when (select current_database()) = 'ae' then 64 when (select current_database()) = 'qa'
                then 65 when (select current_database()) = 'sa' then 66 when (select current_database()) = 'kw' then 67 when (select current_database()) = 'bh'
                then 68 when (select current_database()) = 'eg' then 69 when (select current_database()) = 'ma' then 70 when (select current_database()) = 'uz'
                then 71 end) as country_id,
       impression_datediff,
       j.id   as job_id,
       jr.uid as job_uid,
       j.lang_text,
       j.lang_title
from (select distinct
             si.date    as impression_datediff,
             si.uid_job as job_uid
      from session_impression si
           join session_impression_on_screen sios
           on si.date = sios.date_diff
               and si.id = sios.id_impression
      where si.date = an.fn_get_date_diff(ex_getdate()::date - 1)) as jobs_with_impression
     join link_dbo.job_region jr
     on jobs_with_impression.job_uid = jr.uid
     join link_dbo.job j
     on j.id = jr.id_job;