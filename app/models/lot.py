from sqlalchemy import Column, String, Text, DateTime, Numeric, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from app.core.database import Base


class Lot(Base):
    __tablename__ = "lots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.id"))

    external_lot_id = Column(String,nullable=True)
    title = Column(Text)
    description = Column(Text)

    estimated_value = Column(Numeric)
    currency = Column(String)

    opportunity = relationship("Opportunity", back_populates="lots")
    requirements = relationship("Requirement", back_populates="lot")