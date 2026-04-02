import time
from app.core.queue import dequeue_notice
from app.core.database import SessionLocal
from app.models.raw_notice import RawNotice
from app.models.opportunity import Opportunity
from app.models.opportunity_update import OpportunityUpdate
from app.models.requirement import Requirement

def extract_requirements(description: str):
    if not description:
        return []

    sentences = description.split(".")

    return [
        s.strip()
        for s in sentences
        if len(s.strip()) > 30
    ]

def extract_language(field: dict) -> str:
    if not isinstance(field, dict):
        return ""

    for lang in ["eng", "ron", "fra", "deu"]:
        if lang in field:
            return field[lang]

    if field:
        return list(field.values())[0]

    return ""


def extract_description(raw: dict) -> str:
    candidates = [
        raw.get("BT-24-Procedure"),
    ]

    for c in candidates:
        text = extract_language(c)
        if text:
            return text

    return ""


def extract_title(raw: dict) -> str:
    # fallback: first 100 chars of description
    desc = extract_description(raw)
    return desc[:100] if desc else "Untitled opportunity"


def extract_source_url(raw: dict) -> str:
    links = raw.get("links", {})
    pdf = links.get("pdf", {})

    # try english first
    if "ENG" in pdf:
        return pdf["ENG"]

    # fallback: any language
    if pdf:
        return list(pdf.values())[0]

    return None


def extract_country(raw: dict) -> str:
    # you already stored country separately, fallback here
    return raw.get("buyer-country", None)


def process_notice(db, notice_id):
    notice = db.query(RawNotice).filter_by(id=notice_id).first()

    if not notice:
        return

    if notice.processed:
        return

    raw = notice.raw_payload
    external_id = notice.external_id

    print(f"Processing {external_id}", flush=True)

    #  Extract basic fields
    description = extract_description(raw)
    title = extract_title(raw)
    source_url = extract_source_url(raw)

    #  UPSERT Opportunity
    opportunity = db.query(Opportunity).filter_by(
        external_id=external_id
    ).first()

    if not opportunity:
        opportunity = Opportunity(
            external_source="TED",
            external_id=external_id,
            title=title,
            description=description,
            buyer_country=notice.country,
            source_url=source_url,
            raw_payload=raw,
            status="new",
        )

        db.add(opportunity)
        db.flush()  # get ID

        #  Extract requirements (ONLY on creation)
        reqs = extract_requirements(description)

        for r in reqs:
            db.add(Requirement(
                opportunity_id=opportunity.id,
                text=r,
                type="text",
                source="ted",
            ))

    else:
        #  Treat as update (VERY SIMPLE VERSION)
        update = OpportunityUpdate(
            opportunity_id=opportunity.id,
            update_type="notice_update",
            description=description[:500],
            source_url=source_url,
            raw_payload=raw,
        )

        db.add(update)

    #  Mark processed
    notice.processed = True

    db.commit()



def worker_loop():
    print("Worker started...")

    while True:
        notice_id = dequeue_notice()

        if not notice_id:
            time.sleep(1)
            continue
        print(f"Dequeued: {notice_id}", flush=True)
        db = SessionLocal()

        try:
            process_notice(db, notice_id)
        finally:
            db.close()

if __name__ == "__main__":
    worker_loop()