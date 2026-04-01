from sqlalchemy import Column, String, Text, DateTime, Numeric, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm  import relationship
from sqlalchemy import ForeignKey
import uuid
from app.core.database import Base

class OpportunityUpdate(Base):
    __tablename__ = "opportunity_updates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.id"))

    update_type = Column(String)
    description = Column(Text)

    source_url = Column(Text)
    raw_payload = Column(JSON)

    opportunity = relationship("Opportunity", back_populates="updates")