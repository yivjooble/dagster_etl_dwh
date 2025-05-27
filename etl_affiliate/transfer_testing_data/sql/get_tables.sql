select
    object_name as table_name,
    is_empty,
    data_filter_string,
    table_constraints
from affiliate.dev_test_objects
where object_type = 'table'