from sqlalchemy import Column, String, DateTime
from datetime import datetime
from app.core.database import Base

class IngestionState(Base):
    __tablename__ = "ingestion_state"

    source = Column(String, primary_key=True)  # "ted"
    iteration_token = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)