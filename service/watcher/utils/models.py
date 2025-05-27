from sqlalchemy import Column, Integer, String, DateTime, SmallInteger
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LogEntry(Base):
    __tablename__ = 'etl_logs_v2'
    __table_args__ = {'schema': 'dwh_system'}

    id = Column(Integer, primary_key=True)
    module_name = Column(String(100), nullable=False)
    status = Column(String(10), nullable=False)
    log_field = Column(String, nullable=False)
    start_date = Column(DateTime, nullable=False)
    update_date = Column(DateTime, nullable=False)
    launch_time = Column(DateTime, nullable=False)
    flag = Column(String(255), nullable=False)


class LogWatcher(Base):
    __tablename__ = 'watcher_logs'
    __table_args__ = {'schema': 'dwh_system'}

    id = Column(Integer, primary_key=True)
    module_name = Column(String(100), nullable=False)
    module_check_count = Column(SmallInteger, nullable=False)
    update_date = Column(DateTime, nullable=False)