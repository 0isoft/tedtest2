from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from app.core.database import Base
from sqlalchemy import Boolean,  Integer


class RawNotice(Base):
    __tablename__ = "raw_notices"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    external_source = Column(String, nullable=False)  # "TED"
    external_id = Column(String, nullable=False, index=True)

    country = Column(String, nullable=True)

    raw_payload = Column(JSON, nullable=False)

    ingested_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False, index=True)

    retry_count = Column(Integer, default=0)
    last_error = Column(String, nullable=True)