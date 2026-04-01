from sqlalchemy import Column, String, Text, DateTime, Numeric, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from app.core.database import Base

class Opportunity(Base):
    __tablename__ = "opportunities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    external_source = Column(String, nullable=False)
    external_id = Column(String, nullable=True)

    title = Column(Text, nullable=False)
    description = Column(Text, nullable=True)

    buyer_name = Column(String, nullable=True)
    buyer_country = Column(String, nullable=True)

    publication_date = Column(DateTime, nullable=True)
    deadline = Column(DateTime, nullable=True)

    estimated_value = Column(Numeric, nullable=True)
    currency = Column(String, nullable=True)

    status = Column(String, default="new")

    source_url = Column(Text, nullable=True) #original notice link
    documents_url = Column(Text, nullable=True) #tender docs page

    requirements = relationship("Requirement", back_populates="opportunity")
    updates = relationship("OpportunityUpdate", back_populates="opportunity")

    raw_payload = Column(JSON, nullable=False)

    created_at = Column(DateTime)
    updated_at = Column(DateTime)