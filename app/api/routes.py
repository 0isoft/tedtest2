from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db, Base
from app.services.ingestion import fetch_and_store_notices
from app.models.ingestion_config import IngestionConfig,  IngestionControl

from app.models.opportunity import Opportunity
from app.schemas.opportunity import OpportunityOut
from sqlalchemy import Column, String, DateTime

from sqlalchemy import Boolean,  Integer
from app.core.dependencies import get_queue
from app.core.queue import RedisQueue

router = APIRouter()



@router.get("/ping")
def ping():
    return {"message": "pong"}

@router.get("/ingestion/status")
def ingestion_status(db: Session = Depends(get_db)):
    ctrl = db.query(IngestionControl).first()

    return {
        "paused": ctrl.is_paused if ctrl else False
    }

@router.get("/opportunities", response_model=list[OpportunityOut])
def get_opportunities(
    db: Session = Depends(get_db),
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    return (
        db.query(Opportunity)
        .order_by(Opportunity.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

@router.get("/opportunities/{id}", response_model=OpportunityOut)
def get_opportunity(id: int, db: Session = Depends(get_db)):
    obj = db.query(Opportunity).filter_by(id=id).first()

    if not obj:
        raise HTTPException(404, "Opportunity not found")

    return obj


@router.get("/opportunities/external/{external_id}", response_model=OpportunityOut)
def get_opportunity_external(external_id: str, db: Session = Depends(get_db)):
    obj = db.query(Opportunity).filter_by(external_id=external_id).first()

    if not obj:
        raise HTTPException(404, "Opportunity not found")

    return obj


@router.post("/configs")
def create_config(
    country: str,
    cpv_code: str | None = None,
    db: Session = Depends(get_db),
):
    config = IngestionConfig(
        country=country,
        cpv_code=cpv_code,
        is_active=True
    )

    db.add(config)
    db.commit()
    db.refresh(config)

    return config

@router.get("/configs")
def list_configs(db: Session = Depends(get_db)):
    return db.query(IngestionConfig).all()


@router.patch("/configs/{config_id}/toggle")
def toggle_config(config_id: int, db: Session = Depends(get_db)):
    config = db.get(IngestionConfig, config_id)

    if not config:
        raise HTTPException(404, "Config not found")

    config.is_active = not config.is_active
    db.commit()

    return {"id": config.id, "is_active": config.is_active}


@router.delete("/configs/{config_id}")
def delete_config(config_id: int, db: Session = Depends(get_db)):
    config = db.get(IngestionConfig, config_id)

    if not config:
        raise HTTPException(404, "Config not found")

    db.delete(config)
    db.commit()

    return {"status": "deleted"}


@router.post("/ingestion/pause")
def pause_ingestion(db: Session = Depends(get_db)):
    ctrl = db.query(IngestionControl).first()
    if not ctrl:
        ctrl = IngestionControl(is_paused=True)
        db.add(ctrl)
    else:
        ctrl.is_paused = True

    db.commit()
    return {"status": "paused"}


@router.post("/ingestion/resume")
def resume_ingestion(db: Session = Depends(get_db)):
    ctrl = db.query(IngestionControl).first()
    if ctrl:
        ctrl.is_paused = False
        db.commit()

    return {"status": "running"}

@router.get("/debug/queue-size")
def queue_size(queue: RedisQueue = Depends(get_queue)):
    return {"size": queue.length()}

@router.get("/debug/queue-peek")
def queue_peek(queue: RedisQueue = Depends(get_queue)):
    return {"items": queue.peek()}


@router.get("/debug/queue-obliterate")
def queue_peek(queue: RedisQueue = Depends(get_queue)):
    return {"items": queue.clear()}