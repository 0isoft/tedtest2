from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.ingestion import fetch_and_store_notices
router = APIRouter()

@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/test/ted")
def test_ted(db: Session = Depends(get_db)):
    country = "ROU"

    count = fetch_and_store_notices(db, country)

    return {"inserted": count}