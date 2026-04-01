from fastapi import APIRouter
from app.services.ingestion import fetch_sample_notices
router = APIRouter()

@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/test/ted")
def test_ted():
    notices = fetch_sample_notices()
    return {"count": len(notices), "data": notices}