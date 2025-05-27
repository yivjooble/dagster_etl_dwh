create table an.email_account_test as
select *
from link_dbo.email_account_test
where date_diff between 45105 and 45134;

create table an.email_account_test_settings as
select *
from link_dbo.email_account_test_settings;

create table an.email_sent as
select *
from link_dbo.email_sent
where date_diff between 45105 and 45134;

create table an.email_alert as
select *
from link_dbo.email_alert
where date_add::date >= (current_date - 31)::date or unsub_date::date >= (current_date - 31)::date;

create table an.account as
select *
from link_dbo.account;

create table an.account_info as
select *
from link_dbo.account_info;