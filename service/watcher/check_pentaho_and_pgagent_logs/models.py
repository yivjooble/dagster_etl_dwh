from sqlalchemy import Column, Integer, String, Numeric, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class PentahoTransformationsLog(Base):
    __tablename__ = 'pentaho_transformations_log'
    __table_args__ = {'schema': 'logs'}

    id_batch = Column(Integer, primary_key = True)
    channel_id = Column(String)       
    transname = Column(String)        
    status = Column(String)           
    lines_read = Column(Numeric)       
    lines_written = Column(Numeric)    
    lines_updated = Column(Numeric)    
    lines_input = Column(Numeric)      
    lines_output = Column(Numeric)     
    lines_rejected = Column(Numeric)   
    errors = Column(Numeric)           
    startdate = Column(DateTime)        
    enddate = Column(DateTime)          
    logdate = Column(DateTime)          
    depdate = Column(DateTime)          
    replaydate = Column(DateTime)       
    log_field = Column(Text)        
    executing_server = Column(String) 
    executing_user = Column(String)   
    client = Column(String)           


class CheckPgAdminFailed(Base):
    __tablename__ = 'mat_check_pg_admin_failed'
    __table_args__ = {'schema': 'dwh_test'}

    job_step_start = Column(DateTime)
    jobname = Column(String)
    job_step_name = Column(String)
    job_step_id = Column(Integer, primary_key=True)
    job_step_status = Column(String)
    time_from_start = Column(DateTime)
    job_step_duration = Column(String)
    job_step_output = Column(String)


class PythonETLLogs(Base):
    __tablename__ = 'etl_logs_v2'
    __table_args__ = {'schema': 'dwh_system'}

    module_name = Column(String, primary_key = True)
    status = Column(String)       
    log_field = Column(String)        
    start_date = Column(DateTime)           
    update_date = Column(DateTime)       
    recurrence = Column(String)    
    launch_time = Column(DateTime)    
    flag = Column(String)         