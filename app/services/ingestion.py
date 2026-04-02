from app.services.ted_client import TedClient
from app.models.raw_notice import RawNotice
from sqlalchemy.orm import Session
from datetime import date, timedelta
from app.core.queue import enqueue_notice
from app.models.ingestion_state import IngestionState
import time

def build_query(country: str):
    #query = return all notifications from given country, posted within last week
    today = date.today() 
    week_ago = today - timedelta(days=10)
    return ( f"buyer-country={country} " f"AND publication-date>={week_ago.strftime('%Y%m%d')}" )

#this for rate limiting
class TokenBucket:
    def __init__(self, rate, capacity):
        """
        rate: tokens per second
        capacity: max burst
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()

    def wait_for_token(self):
        #todo: use celery beat to schedule  this
        while True:
            now = time.time()
            elapsed = now - self.last_refill

            # refill tokens
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate
            )
            self.last_refill = now

            if self.tokens >= 1:
                self.tokens -= 1
                return

            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time)


#this for iteration token

# loop = fetch one page of ted notifications, save them to db to be further fed into the redis queue
# after fetching a page, store the iteration token, then sleep
# the iteration token is stored in the db
def load_token(db: Session):
    state = db.query(IngestionState).filter_by(source="ted").first()
    return state.iteration_token if state else None


def save_token(db: Session, token: str):
    state = db.query(IngestionState).filter_by(source="ted").first()

    if not state:
        state = IngestionState(source="ted", iteration_token=token)
        db.add(state)
    else:
        state.iteration_token = token

    db.commit()



#bucket = TokenBucket(rate=1/60, capacity=1)  # 1 req/min

def fetch_and_store_notices(db: Session, country: str):
    print("step 1: bucket", flush=True)
    bucket = TokenBucket(rate=1/60, capacity=1)


    print("step 2: client", flush=True)
    client = TedClient(bucket)

    print("step 3: query", flush=True)
    query = build_query(country)

    print("step 4: load token", flush=True)
    token = load_token(db)
    print("step 5: token loaded", token, flush=True)
    total_inserted = 0

    print(f"Fetching ONE batch with token={token}")

    data = client.search_notices(
        query=query,
        limit=50,
        iteration_token=token
    )

    if not data:
        print("No data returned (rate limited or empty)")
        return 0

    notices = data.get("notices", [])

    for notice in notices:
        external_id = notice.get("publication-number")
        print("EXTERNAL ID:", external_id)
        print("EXISTS:", db.query(RawNotice).filter_by(external_id=external_id).first())

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
        db.flush()

        enqueue_notice(str(raw_notice.id))
        total_inserted += 1
        total_seen = len(notices)
        total_skipped = total_seen - total_inserted
        print(f"Seen: {total_seen}, Inserted: {total_inserted}, Skipped: {total_skipped}")

    db.commit()

    next_token = data.get("iterationNextToken")    # persist NEXT iterationtoken

    if next_token:
        save_token(db, next_token)

    print(f"Inserted {total_inserted} notices")

    return total_inserted
