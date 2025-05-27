def generate_aggregation_query_to_run(
        date_from,
        date_to,
        db):
    concatenated_command = f'''
        insert into {db}.search_cnt_pre_agg(date_diff, id, title, title_processed, id_region, ir_region_name, 
                                            pre_filtered_city_name, raw_city_name, region_name, remote_type, 
                                            abroad_type)
        select pre_agg.date_diff,
               pre_agg.id,
               pre_agg.title,
               pre_agg.title_processed,
               pre_agg.id_region,
               pre_agg.ir_region_name,
               pre_agg.pre_filtered_city_name,
               pre_agg.raw_city_name,
               pre_agg.region_name,
               pre_agg.remote_type,
               pre_agg.abroad_type
        from (
                 SELECT ss.date_diff                                         AS date_diff,
                        ss.id                                                AS id,
                        ss.q_kw                                              AS title,
                        ss.q_kw_processed                                    AS title_processed,
                        ss.q_id_region                                       AS id_region,
                        ir.name                                              AS ir_region_name,
                        multiIf((ir. is_city = 1) AND (ss. q_id_region != -1) AND (ir. order_value > 100000) AND
                                (ir. order_value != -1),
                                ir. name, 'other')                           AS pre_filtered_city_name,
                        ir. name                                             AS raw_city_name,
                        ir2.name                                             AS region_name,
                        multiIf((ir. name = 'Remote') OR ((ir. is_toplevel = 1) AND (ir. order_value = 1000)) OR
                                ((sfa. action IN (0, 5)) AND (sfa. filter_type = 2) AND
                                 (sfa. filter_value = 2) AND
                                 (sfa. id_search_prev IS NOT NULL)) OR (lower(ss. q_kw) LIKE '%remote%') OR
                                (lower(ss. q_kw) LIKE '%online% ') OR (lower(ss. q_kw) LIKE '%from home%') OR
                                (lower(ss. q_kw) LIKE '%homeoffice%'), 1, 0) AS remote_type,
                        multiIf(ir. order_value = -1, 1, 0)                  AS abroad_type
                 FROM {db}.session_search AS ss
                          INNER JOIN {db}.session AS s ON (s. date_diff = ss. date_diff) AND (s. id = ss. id_session)
                          LEFT JOIN {db}.info_region AS ir ON ss. q_id_region = ir. id
                          LEFT JOIN {db}.info_region_submission AS irs ON irs. id_region = ir. id
                          LEFT JOIN {db}.info_region AS ir2 ON ir2.id = irs. id_region_parent
                          LEFT JOIN {db}.session_filter_action AS sfa ON (s. date_diff = sfa. date_diff) AND (ss. id = sfa. id_search_prev)
                 WHERE bitAnd(s. flags, 1) = 0
                   and date_diff between (dateDiff('day', toDate('1970-01-01'), toDate('{date_from}')) + 25567) and (
                     dateDiff('day', toDate('1970-01-01'), toDate('{date_to}')) + 25567)
                 ) as pre_agg
                 SETTINGS distributed_product_mode = 'global'
        ;
       '''

    return concatenated_command
