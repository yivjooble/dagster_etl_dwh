select 
    country_id                            ,
    test_group_list                       ,
    is_new_user                           ,
    is_mobile                             ,
    is_local                              ,
    session_create_page_type_id           ,
    first_session_datediff                ,
    first_test_session_datediff           ,
    user_cnt                              ,
    user_retained_day_1_3_cnt             ,
    user_retained_day_1_7_cnt             ,
    user_retained_day_1_14_cnt            ,
    user_managed_retained_day_1_3_cnt     ,
    user_managed_retained_day_1_7_cnt     ,
    user_managed_retained_day_1_14_cnt    ,
    user_unmanaged_retained_day_1_3_cnt   ,
    user_unmanaged_retained_day_1_7_cnt   ,
    user_unmanaged_retained_day_1_14_cnt  ,
    user_artificial_retained_day_1_3_cnt  ,
    user_artificial_retained_day_1_7_cnt  ,
    user_artificial_retained_day_1_14_cnt 
from an.rpl_retention_test_agg
where first_test_session_datediff >= %(datediff)s;