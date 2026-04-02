from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.ingestion import fetch_and_store_notices

from app.models.opportunity import Opportunity
from app.schemas.opportunity import OpportunityOut

router = APIRouter()



@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/test/ted")
def test_ted(db: Session = Depends(get_db)):
    country = "ROU"

    count = fetch_and_store_notices(db, country)

    return {"inserted": count}

@router.get("/opportunities", response_model=list[OpportunityOut])
def get_opportunities(
    db: Session = Depends(get_db),
    limit: int = 10000,
    offset: int = 0
):
    return db.query(Opportunity).offset(offset).limit(limit).all()

@router.get("/opportunities/{id}", response_model=OpportunityOut)
def get_opportunity(id: str, db: Session = Depends(get_db)):
    return db.query(Opportunity).filter_by(id=id).first()

