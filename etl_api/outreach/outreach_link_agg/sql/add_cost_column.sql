with eur_currency_rate as(
      select ich.value_to_usd
      FROM dimension.info_currency_history ich
      Where cast(ich.date as date) = %(date)s
        and ich.country = 6
        and ich.id_currency = 1
)
select report_update_time,
       id_link,
       link_created_date,
       id_service_flags,
       name_service_flags,
       cost,
       currency,
       cost_usd,
       case
           when currency = 'EUR' then cost
           else cost_usd / tcr.value_to_usd
           end as cost_eur
from dwh_test.outreach_link_cost_raw,
     eur_currency_rate tcr
where link_created_date::date = %(date)s
;