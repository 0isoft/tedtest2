from app.services.ted_client import TedClient
from app.models.raw_notice import RawNotice
from sqlalchemy.orm import Session
from datetime import date, timedelta


def build_query(country: str):
    today = date.today() 
    week_ago = today - timedelta(days=7)
    return ( f"buyer-country={country} " f"AND publication-date>={week_ago.strftime('%Y%m%d')}" )

def fetch_and_store_notices(db: Session, country: str):
    client = TedClient()

    query = build_query(country)

    token = None
    total_inserted = 0

    while True:
        data = client.search_notices(
            query=query,
            limit=50,
            iteration_token=token
        )

        notices = data.get("notices", [])

        if not notices:
            break

        for notice in notices:
            external_id = notice.get("publication-number")

            # deduplication
            exists = db.query(RawNotice).filter_by(
                external_id=external_id
            ).first()

            if exists:
                continue

            raw_notice = RawNotice(
                external_source="TED",
                external_id=external_id,
                country=country,
                raw_payload=notice,
            )

            db.add(raw_notice)
            total_inserted += 1

        db.commit()  # commit per batch (important)

        token = data.get("iterationNextToken")

        if not token:
            break

    return total_inserted