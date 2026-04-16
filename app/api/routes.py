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
from app.models.raw_notice import RawNotice

from app.models.lot import Lot
from app.models.requirement import Requirement

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

@router.get("/debug/configs-health")
def configs_health(db: Session = Depends(get_db)):
    configs = db.query(IngestionConfig).all()

    return [
        {
            "id": c.id,
            "country": c.country,
            "cpv_code": c.cpv_code,
            "active": c.is_active,
            "failures": c.failure_count,
            "last_error": c.last_error,
            "last_run_at": c.last_run_at
        }
        for c in configs
    ]


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

@router.get("/debug/raw-notices-stats")
def raw_notices_stats(db: Session = Depends(get_db)):
    total = db.query(RawNotice).count()

    processed = db.query(RawNotice)\
        .filter(RawNotice.processed == True).count()

    pending = db.query(RawNotice)\
        .filter(RawNotice.processed == False).count()

    failed = db.query(RawNotice)\
        .filter(RawNotice.retry_count >= 5).count()

    return {
        "total": total,
        "processed": processed,
        "pending": pending,
        "failed": failed
    }


@router.get("/debug/pipeline-health")
def pipeline_health(
    db: Session = Depends(get_db),
    queue: RedisQueue = Depends(get_queue)
):
    pending = db.query(RawNotice)\
        .filter(RawNotice.processed == False)\
        .filter(RawNotice.retry_count < 5)\
        .count()

    queue_size = queue.length()

    return {
        "pending_in_db": pending,
        "queue_size": queue_size,
        "status": (
            "OK" if queue_size > 0 or pending == 0
            else "WARNING: notices are pending but queue empty"
        )
    }


@router.get("/debug/recent-raw-notices")
def recent_raw_notices(db: Session = Depends(get_db)):
    notices = (
        db.query(RawNotice)
        .order_by(RawNotice.ingested_at.desc())
        .limit(20)
        .all()
    )

    return [
        {
            "id": str(n.id),
            "external_id": n.external_id,
            "processed": n.processed,
            "retry_count": n.retry_count,
            "last_error": n.last_error
        }
        for n in notices
    ]


@router.get("/debug/processing-rate")
def processing_rate(db: Session = Depends(get_db)):
    from datetime import datetime, timedelta

    last_minute = datetime.utcnow() - timedelta(minutes=1)

    processed_last_min = db.query(RawNotice)\
        .filter(RawNotice.processed == True)\
        .filter(RawNotice.ingested_at >= last_minute)\
        .count()

    return {
        "processed_last_minute": processed_last_min
    }



@router.get("/debug/opportunities")
def debug_opportunities(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
):
    opportunities = (
        db.query(Opportunity)
        .order_by(Opportunity.created_at.desc().nullslast(), Opportunity.id.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": str(o.id),
            "external_id": o.external_id,
            "external_source": o.external_source,
            "title": o.title,
            "description_preview": o.description[:200] if o.description else None,
            "buyer_name": o.buyer_name,
            "buyer_country": o.buyer_country,
            "publication_date": o.publication_date,
            "deadline": o.deadline,
            "estimated_value": float(o.estimated_value) if o.estimated_value is not None else None,
            "currency": o.currency,
            "status": o.status,
            "source_url": o.source_url,
            "documents_url": o.documents_url,
            "requirements_count": len(o.requirements) if o.requirements else 0,
            "updates_count": len(o.updates) if o.updates else 0,
            "raw_missing": {
                "title": o.title is None,
                "description": o.description is None,
                "buyer_name": o.buyer_name is None,
                "buyer_country": o.buyer_country is None,
                "publication_date": o.publication_date is None,
                "deadline": o.deadline is None,
                "estimated_value": o.estimated_value is None,
                "currency": o.currency is None,
                "source_url": o.source_url is None,
                "documents_url": o.documents_url is None,
            },
        }
        for o in opportunities
    ]

@router.get("/debug/opportunities-quality")
def opportunities_quality(db: Session = Depends(get_db)):
    total = db.query(Opportunity).count()

    def pct(count: int) -> float:
        return round(100 * count / total, 2) if total else 0.0

    missing_title = db.query(Opportunity).filter(Opportunity.title.is_(None)).count()
    missing_description = db.query(Opportunity).filter(Opportunity.description.is_(None)).count()
    missing_buyer_name = db.query(Opportunity).filter(Opportunity.buyer_name.is_(None)).count()
    missing_buyer_country = db.query(Opportunity).filter(Opportunity.buyer_country.is_(None)).count()
    missing_publication_date = db.query(Opportunity).filter(Opportunity.publication_date.is_(None)).count()
    missing_deadline = db.query(Opportunity).filter(Opportunity.deadline.is_(None)).count()
    missing_estimated_value = db.query(Opportunity).filter(Opportunity.estimated_value.is_(None)).count()
    missing_currency = db.query(Opportunity).filter(Opportunity.currency.is_(None)).count()
    missing_source_url = db.query(Opportunity).filter(Opportunity.source_url.is_(None)).count()
    missing_documents_url = db.query(Opportunity).filter(Opportunity.documents_url.is_(None)).count()

    return {
        "total": total,
        "missing_title": {"count": missing_title, "pct": pct(missing_title)},
        "missing_description": {"count": missing_description, "pct": pct(missing_description)},
        "missing_buyer_name": {"count": missing_buyer_name, "pct": pct(missing_buyer_name)},
        "missing_buyer_country": {"count": missing_buyer_country, "pct": pct(missing_buyer_country)},
        "missing_publication_date": {"count": missing_publication_date, "pct": pct(missing_publication_date)},
        "missing_deadline": {"count": missing_deadline, "pct": pct(missing_deadline)},
        "missing_estimated_value": {"count": missing_estimated_value, "pct": pct(missing_estimated_value)},
        "missing_currency": {"count": missing_currency, "pct": pct(missing_currency)},
        "missing_source_url": {"count": missing_source_url, "pct": pct(missing_source_url)},
        "missing_documents_url": {"count": missing_documents_url, "pct": pct(missing_documents_url)},
    }


@router.get("/debug/opportunity/{id}/raw")
def get_raw_for_opportunity(id: str, db: Session = Depends(get_db)):
    opp = db.query(Opportunity).filter_by(id=id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    raw = db.query(RawNotice).filter_by(external_id=opp.external_id).first()

    return {
        "opportunity": {
            "id": str(opp.id),
            "external_id": opp.external_id,
            "title": opp.title,
            "description": opp.description,
            "buyer_name": opp.buyer_name,
            "buyer_country": opp.buyer_country,
            "publication_date": opp.publication_date,
            "deadline": opp.deadline,
            "estimated_value": float(opp.estimated_value) if opp.estimated_value is not None else None,
            "currency": opp.currency,
            "status": opp.status,
            "source_url": opp.source_url,
            "documents_url": opp.documents_url,
            "created_at": opp.created_at,
            "updated_at": opp.updated_at,
        },
        "raw_notice": {
            "id": str(raw.id),
            "external_id": raw.external_id,
            "country": raw.country,
            "processed": raw.processed,
            "retry_count": raw.retry_count,
            "last_error": raw.last_error,
            "ingested_at": raw.ingested_at,
            "raw_payload": raw.raw_payload,
        } if raw else None,
    }

@router.get("/debug/opportunities-value-distribution")
def value_distribution(db: Session = Depends(get_db)):
    values = (
        db.query(Opportunity.estimated_value)
        .filter(Opportunity.estimated_value.is_not(None))
        .all()
    )

    numeric_values = [float(v[0]) for v in values]

    return {
        "count_with_estimated_value": len(numeric_values),
        "avg_estimated_value": round(sum(numeric_values) / len(numeric_values), 2) if numeric_values else 0,
        "max_estimated_value": max(numeric_values) if numeric_values else 0,
        "min_estimated_value": min(numeric_values) if numeric_values else 0,
    }


@router.get("/debug/lots-quality")
def lots_quality(db: Session = Depends(get_db)):
    total = db.query(Lot).count()

    def pct(count: int) -> float:
        return round(100 * count / total, 2) if total else 0.0

    missing_external_lot_id = db.query(Lot).filter(Lot.external_lot_id.is_(None)).count()
    missing_title = db.query(Lot).filter(Lot.title.is_(None)).count()
    missing_description = db.query(Lot).filter(Lot.description.is_(None)).count()
    missing_estimated_value = db.query(Lot).filter(Lot.estimated_value.is_(None)).count()
    missing_currency = db.query(Lot).filter(Lot.currency.is_(None)).count()
    missing_opportunity_id = db.query(Lot).filter(Lot.opportunity_id.is_(None)).count()

    return {
        "total": total,
        "missing_external_lot_id": {"count": missing_external_lot_id, "pct": pct(missing_external_lot_id)},
        "missing_title": {"count": missing_title, "pct": pct(missing_title)},
        "missing_description": {"count": missing_description, "pct": pct(missing_description)},
        "missing_estimated_value": {"count": missing_estimated_value, "pct": pct(missing_estimated_value)},
        "missing_currency": {"count": missing_currency, "pct": pct(missing_currency)},
        "missing_opportunity_id": {"count": missing_opportunity_id, "pct": pct(missing_opportunity_id)},
    }



@router.get("/debug/lots-quality")
def lots_quality(db: Session = Depends(get_db)):
    total = db.query(Lot).count()

    def pct(count: int) -> float:
        return round(100 * count / total, 2) if total else 0.0

    missing_external_lot_id = db.query(Lot).filter(Lot.external_lot_id.is_(None)).count()
    missing_title = db.query(Lot).filter(Lot.title.is_(None)).count()
    missing_description = db.query(Lot).filter(Lot.description.is_(None)).count()
    missing_estimated_value = db.query(Lot).filter(Lot.estimated_value.is_(None)).count()
    missing_currency = db.query(Lot).filter(Lot.currency.is_(None)).count()
    missing_opportunity_id = db.query(Lot).filter(Lot.opportunity_id.is_(None)).count()

    return {
        "total": total,
        "missing_external_lot_id": {"count": missing_external_lot_id, "pct": pct(missing_external_lot_id)},
        "missing_title": {"count": missing_title, "pct": pct(missing_title)},
        "missing_description": {"count": missing_description, "pct": pct(missing_description)},
        "missing_estimated_value": {"count": missing_estimated_value, "pct": pct(missing_estimated_value)},
        "missing_currency": {"count": missing_currency, "pct": pct(missing_currency)},
        "missing_opportunity_id": {"count": missing_opportunity_id, "pct": pct(missing_opportunity_id)},
    }


@router.get("/debug/requirements-scope")
def requirements_scope(db: Session = Depends(get_db)):
    total = db.query(Requirement).count()

    opp_only = db.query(Requirement).filter(
        Requirement.opportunity_id.is_not(None),
        Requirement.lot_id.is_(None)
    ).count()

    lot_only = db.query(Requirement).filter(
        Requirement.opportunity_id.is_(None),
        Requirement.lot_id.is_not(None)
    ).count()

    both_set = db.query(Requirement).filter(
        Requirement.opportunity_id.is_not(None),
        Requirement.lot_id.is_not(None)
    ).count()

    neither_set = db.query(Requirement).filter(
        Requirement.opportunity_id.is_(None),
        Requirement.lot_id.is_(None)
    ).count()

    return {
        "total": total,
        "opportunity_level": opp_only,
        "lot_level": lot_only,
        "invalid_both_set": both_set,
        "invalid_neither_set": neither_set,
    }

@router.get("/debug/opportunities-lots-summary")
def opportunities_lots_summary(db: Session = Depends(get_db)):
    total_opportunities = db.query(Opportunity).count()

    opportunities_with_lots = (
        db.query(Opportunity.id)
        .join(Lot, Lot.opportunity_id == Opportunity.id)
        .distinct()
        .count()
    )

    opportunities_without_lots = total_opportunities - opportunities_with_lots
    total_lots = db.query(Lot).count()

    avg_lots_per_opportunity = round(total_lots / total_opportunities, 2) if total_opportunities else 0
    avg_lots_per_opportunity_with_lots = round(total_lots / opportunities_with_lots, 2) if opportunities_with_lots else 0

    return {
        "total_opportunities": total_opportunities,
        "opportunities_with_lots": opportunities_with_lots,
        "opportunities_without_lots": opportunities_without_lots,
        "total_lots": total_lots,
        "avg_lots_per_opportunity": avg_lots_per_opportunity,
        "avg_lots_per_opportunity_with_lots": avg_lots_per_opportunity_with_lots,
    }

@router.get("/debug/opportunity/{id}/structure")
def get_opportunity_structure(id: str, db: Session = Depends(get_db)):
    opp = db.query(Opportunity).filter_by(id=id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    return {
        "opportunity": {
            "id": str(opp.id),
            "external_id": opp.external_id,
            "title": opp.title,
            "buyer_name": opp.buyer_name,
            "buyer_country": opp.buyer_country,
            "publication_date": opp.publication_date,
            "deadline": opp.deadline,
            "estimated_value": float(opp.estimated_value) if opp.estimated_value is not None else None,
            "currency": opp.currency,
            "documents_url": opp.documents_url,
            "requirements_count": len(opp.requirements),
            "lots_count": len(opp.lots),
            "requirements": [
                {
                    "id": str(r.id),
                    "type": r.type,
                    "text": r.text,
                    "source": r.source,
                    "scope": "opportunity",
                }
                for r in opp.requirements
            ],
        },
        "lots": [
            {
                "id": str(lot.id),
                "external_lot_id": lot.external_lot_id,
                "title": lot.title,
                "description": lot.description,
                "estimated_value": float(lot.estimated_value) if lot.estimated_value is not None else None,
                "currency": lot.currency,
                "requirements_count": len(lot.requirements),
                "requirements": [
                    {
                        "id": str(r.id),
                        "type": r.type,
                        "text": r.text,
                        "source": r.source,
                        "scope": "lot",
                    }
                    for r in lot.requirements
                ],
            }
            for lot in opp.lots
        ]
    }

@router.get("/debug/lots")
def debug_lots(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
):
    lots = (
        db.query(Lot)
        .order_by(Lot.id.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": str(l.id),
            "opportunity_id": str(l.opportunity_id) if l.opportunity_id else None,
            "external_lot_id": l.external_lot_id,
            "title": l.title,
            "description_preview": l.description[:200] if l.description else None,
            "estimated_value": float(l.estimated_value) if l.estimated_value is not None else None,
            "currency": l.currency,
            "requirements_count": len(l.requirements) if l.requirements else 0,
            "raw_missing": {
                "external_lot_id": l.external_lot_id is None,
                "title": l.title is None,
                "description": l.description is None,
                "estimated_value": l.estimated_value is None,
                "currency": l.currency is None,
                "opportunity_id": l.opportunity_id is None,
            }
        }
        for l in lots
    ]


@router.get("/debug/requirements-distribution")
def requirements_distribution(db: Session = Depends(get_db)):
    requirements = db.query(Requirement).all()

    total = len(requirements)
    eligibility = sum(1 for r in requirements if r.type == "eligibility")
    award = sum(1 for r in requirements if r.type == "award")
    other = total - eligibility - award

    return {
        "total": total,
        "eligibility": eligibility,
        "award": award,
        "other": other,
    }

