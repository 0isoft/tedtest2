from pydantic import BaseModel
from typing import Optional, List
from uuid import UUID
from datetime import datetime


class RequirementOut(BaseModel):
    text: str

class OpportunityUpdateOut(BaseModel):
    update_type: Optional[str]
    description: Optional[str]


class OpportunityOut(BaseModel):
    id: UUID
    external_id: Optional[str]
    title: str
    description: Optional[str]
    buyer_country: Optional[str]
    source_url: Optional[str]

    estimated_value: Optional[float]
    currency: Optional[str]
    deadline: datetime | None

    requirements: List[RequirementOut] = []
    updates: List[OpportunityUpdateOut] = []

    class Config:
        from_attributes = True