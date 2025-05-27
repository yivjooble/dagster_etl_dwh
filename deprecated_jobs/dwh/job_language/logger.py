import datetime
import traceback

from utils import Session
from models import LogEntry


def log_execution(func):
    def wrapper(*args, **kwargs):   
        log_id = log_to_db('start', func.__name__, f"{datetime.datetime.now()}: {func.__name__}: started.")
        
        try:
            result = func(*args, **kwargs)
            update_log_entry(log_id, 'end', f"{datetime.datetime.now()}: {func.__name__}: executed successfully.")
            return result
        except Exception as e:
            update_log_entry(log_id, 'error', f"{datetime.datetime.now()}: {func.__name__}: failed with error: {traceback.format_exc()}")
            raise
    
    return wrapper


def log_to_db(status, module_name, log_field):
    session = Session()
    log_entry = LogEntry(start_date=datetime.datetime.now(), 
                         status=status, 
                         module_name=module_name, 
                         log_field=log_field,
                         launch_time=datetime.datetime.now())

    session.add(log_entry)
    session.commit()

    log_id = log_entry.id
    session.close()

    return log_id


def update_log_entry(log_id, status, log_field):
    session = Session()
    log_entry = session.query(LogEntry).filter(LogEntry.id == log_id).one()

    log_entry.update_date = datetime.datetime.now()
    log_entry.status = status
    log_entry.log_field = log_entry.log_field + '\n' + log_field

    session.commit()
    session.close()
