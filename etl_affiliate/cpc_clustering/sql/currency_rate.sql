select distinct on (ic.name)
    ic.name, coalesce(ich.value_to_usd, ic.value_to_usd) as value_to_usd
from dimension.info_currency ic
left join dimension.info_currency_history ich
    on ic.country = ich.country
        and ic.id = ich.id_currency
        and cast(ich.date as date) = '{target_date}'
order by ic.name, coalesce(ich.value_to_usd, ic.value_to_usd) desc
