from sqlalchemy import Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PentahoSchedules(Base):
    __tablename__ = 'pentaho_schedules'
    __table_args__ = {'schema': 'dc'}

    job_name = Column(String, primary_key=True)
    job_directory = Column(String)
    cron_string = Column(String)
    recurrence = Column(String)
    last_run_datetime = Column(DateTime)
    next_run_datetime = Column(DateTime)