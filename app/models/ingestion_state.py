from sqlalchemy import Column, String, DateTime, Boolean,  Integer

from datetime import datetime
from app.core.database import Base
from sqlalchemy import ForeignKey


class IngestionState(Base):
    __tablename__ = "ingestion_state"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    config_id = Column(
        ForeignKey("ingestion_config.id"),
        unique=True,
        nullable=False
    )

    iteration_token = Column(String, nullable=True)

    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )


