from sqlalchemy import Column, String, Text, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from app.core.database import Base

class Requirement(Base):
    __tablename__ = "requirements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.id"))

    type = Column(String)
    text = Column(Text)

    source = Column(String)  # ted/pdf/manual
    extra_data = Column(JSON)

    opportunity = relationship("Opportunity", back_populates="requirements")