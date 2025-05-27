q_delete_tickets_referenced = 'delete from {schema}.{table} where ticket_id in ({tickets});'
q_delete_ids = 'delete from {schema}.{table} where id in ({id_list});'
q_select_by_id = 'select * from {schema}.tickets where id in ({id_list})'
q_select_2_cols = 'select {col1}, {col2} from {schema}.{table_name}'
q_upd_to_deleted = 'update {schema}.{table_name} set is_deleted = True where {id_col} in ({ids})'

q_select_update_time = 'select max(update_time) from {schema}.update_history where is_successful = 1 and type = {type}'
q_insert_update_time = '''
    insert into {schema}.update_history(update_time, status_code, type, is_successful)
    values ('{ut}', {status}, {type}, {is_successful})
'''

q_select_parent_dic = 'select * from {schema}.v_parent_dictionaries'
q_select_class_dic = 'select * from {schema}.v_classification_dictionary'
q_select_class_tree = '''
    select 
        c.id as client_id, c.name as client, 
        ct.id as category_id, ct.name as category,
        r.id as request_id, r.name as request
    from {schema}.client_types c 
    left join {schema}.category_types ct 
        on ct.client_id = c.id
    left join {schema}.request_types r 
        on r.category_id = ct.id
'''
q_select_ticket_types = 'select name from {schema}.ticket_types'
q_select_job_complaint_actions = 'select name from {schema}.job_complaint_actions'

q_max_reply = '''
    select ticket_id, max(created_at) as last_reply_at 
    from {schema}.replies 
    where ticket_id in ({id_list}) 
    group by ticket_id
'''
q_update_ticket = '''
    update {schema}.tickets
    set {column} = {value}
    where id = {ticket_id}
'''
