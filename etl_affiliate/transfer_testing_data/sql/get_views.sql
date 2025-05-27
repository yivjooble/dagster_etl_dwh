with data_views as (
        select pv.schemaname as schema_name,
               pv.viewname   as name,
               pv.definition,
               pc.oid
        from pg_catalog.pg_views as pv
            join affiliate.dev_test_objects as dtb
            on dtb.object_type = 'view' and pv.viewname = dtb.object_name and pv.schemaname = 'affiliate'
            join pg_catalog.pg_class as pc
            on pc.relname = pv.viewname
            join pg_catalog.pg_namespace as pn
            on pn.oid = pc.relnamespace and pn.nspname = 'affiliate'
        ),
    col_views as (
        select
            attname,
            attrelid
        from
            pg_attribute
        where
            attrelid in (select oid from data_views)
            and attnum > 0
        ),
    col_views_agg as (
         select string_agg(attname, ', ') as cols, attrelid
         from col_views
         group by attrelid
         )

select dv.schema_name,
       dv.name,
       'create or replace view ' || dv.schema_name || '.' || dv.name || ' (' || cva.cols || ') ' || CHR(10) ||
            'as ' || dv.definition as definition
from data_views as dv
    join col_views_agg as cva
    on cva.attrelid = dv.oid