from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime


from history.db_engine import BASE
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict


class ScheduleEventHistory(BASE):
    __tablename__ = "schedule_event_history"

    id = Column(Integer, primary_key=True)
    event = Column(String)
    name = Column(String)
    key = Column(String)
    group = Column(String)
    type = Column(String)
    schedule = Column(String)
    task_type = Column(String)
    task_connection = Column(MutableDict.as_mutable(JSONB))
    task_data = Column(MutableDict.as_mutable(JSONB))
    created_at = Column("created_at", DateTime, default=datetime.now)
    updated_at = Column("updated_at", DateTime, onupdate=datetime.now)
