from sqlalchemy import Column, Integer, String, SmallInteger, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class VacancyCollars(Base):
    __tablename__ = 'vacancy_collars'
    __table_args__ = {'schema': 'aggregation'}

    country_id = Column(SmallInteger, primary_key=True)
    action_datediff = Column(Integer)
    cluster_cnt = Column(Integer)
    local_cluster_id = Column(SmallInteger)
    local_cluster_name = Column(String)
    is_paid_uid = Column(SmallInteger)
    active_id_cnt = Column(Integer)
    active_uid_cnt = Column(Integer)
    active_simgroup_cnt = Column(Integer)
    avg_uid_cpc = Column(Numeric)


class JobsStatDaily(Base):
    __tablename__ = 'jobs_stat_daily'
    __table_args__ = {'schema': 'aggregation'}

    id = Column(Integer, primary_key=True)
    id_country = Column(SmallInteger)
    date = Column(String)
    id_project = Column(Integer)
    id_campaign = Column(Integer)
    paid_job_count = Column(Integer)
    organic_job_count = Column(Integer)
    avg_cpc_for_paid_job_count_usd = Column(Numeric)
    flag_32_jobs_count = Column(Integer)
    campaign_budget = Column(Numeric)
    campaign_daily_budget = Column(Numeric)
    is_price_per_job = Column(String)
    campaign_flags = Column(Integer)
    campaign_status = Column(Integer)
    min_cpc_job_count = Column(Numeric)
    max_cpc_job_count = Column(Numeric)


class SentinelStatistic(Base):
    __tablename__ = 'sentinel_statistic'
    __table_args__ = {'schema': 'aggregation'}

    country_id = Column(SmallInteger, primary_key=True)
    action_date = Column(Date)
    sentinel_name = Column(String)
    revenue_usd = Column(Numeric)
    job_cnt = Column(Integer)
    job_with_away_cnt = Column(Integer)
    away_cnt = Column(Integer)
    away_conversion_cnt = Column(Integer)
    conversion_cnt = Column(Integer)
    sentinel_type = Column(Integer)
    country_name = Column(String)
    job_with_revenue_cnt = Column(Integer)
    median_cpc_usd = Column(Numeric)


class SessionAbtestAgg(Base):
    __tablename__ = 'session_abtest_agg'
    __table_args__ = {'schema': 'aggregation'}

    country_id = Column(SmallInteger, primary_key=True)
    group_num = Column(String(255))
    action_datediff = Column(Integer)
    is_returned = Column(Integer)
    device_type_id = Column(Integer)
    is_local = Column(Integer)
    session_create_page_type = Column(Integer)
    traffic_source_id = Column(Integer)
    current_traffic_source_id = Column(Integer)
    is_anomalistic = Column(Integer)
    attribute_name = Column(String(255))
    attribute_value = Column(Integer)
    metric_name = Column(String(255))
    metric_value = Column(Numeric(18, 9))
