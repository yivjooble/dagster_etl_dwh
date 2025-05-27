SET NOCOUNT ON;


select 
		el.id,
		el.id_account,
		el.date_add,
		el.id_traf_src,
		el.unsub_date,
		el.id_type_unsub,
		iif(charindex(nchar(0x00) collate Latin1_General_BIN, el.search collate Latin1_General_BIN) > 0,
					left(replace(el.search collate Latin1_General_BIN, nchar(0x00) collate Latin1_General_BIN ,''),
								(charindex(nchar(0x00) collate Latin1_General_BIN, el.search collate Latin1_General_BIN)) - 1),
										el.search) as search,
		el.id_region,
		iif(charindex(nchar(0x00) collate Latin1_General_BIN, el.txt_region collate Latin1_General_BIN) > 0,
					left(replace(el.txt_region collate Latin1_General_BIN, nchar(0x00) collate Latin1_General_BIN ,''),
								(charindex(nchar(0x00) collate Latin1_General_BIN, el.txt_region collate Latin1_General_BIN)) - 1),
										el.txt_region) as txt_region,
		el.id_lang,
		el.symbols,
		el.ip_backup,
		el.symbols_hash64,
		el.query_hash64,
		el.usr_id64,
		el.id_alert_type,
		cast(el.is_sent as smallint) as is_sent,
		el.radius,
		el.salary,
		el.radius_km,
		el.token_hash64
from dbo.email_alert el
;