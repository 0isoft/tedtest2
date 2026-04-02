from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.ingestion import fetch_and_store_notices
import time
router = APIRouter()

def run_ingestion(db, country):
    while True:
        fetch_and_store_notices(db, country)
        print("Sleeping 60s...")
        time.sleep(60)

@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/test/ted")
def test_ted(db: Session = Depends(get_db)):
    country = "ROU"

    count = run_ingestion(db, country)

    return {"inserted": count}

