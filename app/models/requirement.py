from sqlalchemy import Column, String, Text, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from app.core.database import Base
from sqlalchemy import CheckConstraint

class Requirement(Base):
    __tablename__ = "requirements"

    #a requirement belongsto either an opportunity or a lot, but never to both, and never to neither
    __table_args__ = (
    CheckConstraint(
        "(opportunity_id IS NOT NULL AND lot_id IS NULL) OR "
        "(opportunity_id IS NULL AND lot_id IS NOT NULL)",
        name="requirement_scope_check"
    ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.id"))
    lot_id = Column(UUID(as_uuid=True), ForeignKey("lots.id"), nullable=True)

    type = Column(String)
    text = Column(Text)

    source = Column(String)  # ted/pdf/manual
    extra_data = Column(JSON)

    opportunity = relationship("Opportunity", back_populates="requirements")
    lot = relationship("Lot", back_populates="requirements")