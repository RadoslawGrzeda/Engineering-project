from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class DeadLetter(Base):
    __tablename__ = "dead_letter"
    __table_args__ = {"schema": "client"}

    id = Column(Integer, primary_key=True, index=True)
    inserted_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    error_code = Column(String(100))
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    status = Column(String(20), default="NEW")
    person_id = Column(String(50))
    correlation_id = Column(String(50))
    source_application = Column(String(50))
    raw_payload = Column(JSONB)
