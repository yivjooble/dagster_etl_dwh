select
    namespace.nspname            as schema_name,
    proc.proname                 as name,
    proc.prokind                 as routine_type,
    pg_get_functiondef(proc.oid) as definition,
    case
        when proc.prokind = 'f' then split_part(
                replace(lower(pg_get_functiondef(proc.oid))::varchar, 'create or replace function affiliate.', ''),
                ' returns', 1)
        else split_part(
                replace(lower(pg_get_functiondef(proc.oid))::varchar, 'create or replace procedure affiliate.', ''),
                ' language plpgsql', 1)
        end                      as name_arg
from
    pg_catalog.pg_namespace namespace
    join pg_catalog.pg_proc proc
         on proc.pronamespace = namespace.oid
    join affiliate.dev_test_objects dto
         on dto.object_type = 'routine' and
            dto.object_name = proc.proname
where
      proc.prokind in ('function', 'procedure')
  and namespace.nspname = 'affiliate'