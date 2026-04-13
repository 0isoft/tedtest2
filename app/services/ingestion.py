from app.services.ted_client import TedClient
from app.core.config import settings
from app.models.raw_notice import RawNotice
from sqlalchemy.orm import Session
from datetime import date, timedelta
from app.models.ingestion_state import IngestionState
from app.repositories.ingestion_repository import (
    RawNoticeRepository,
    IngestionStateRepository,
)
import time

def build_query(country: str, cpv_code: str | None = None):
    #query = return all notifications from given country, posted within last week
    today = date.today() 
    week_ago = today - timedelta(days=10)
    parts = [
        f"buyer-country={country}",
        f"publication-date>={week_ago.strftime('%Y%m%d')}"
    ]

    if cpv_code:
        parts.append(f"classification-cpv={cpv_code}")

    return " AND ".join(parts)

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
# after fetching a page, store the iteration token for each config, then sleep
# the iteration token is stored in the db



#bucket = TokenBucket(rate=1/60, capacity=1)  # 1 req/min

def fetch_and_store_notices(
    client,
    raw_repo: RawNoticeRepository,
    state_repo: IngestionStateRepository,
    queue,
    config_id,
    country: str,
    cpv_code: str | None = None, 
):
    query = build_query(country, cpv_code)
    token = state_repo.load_token(config_id)

    print(f"Calling TED with query: {query}", flush=True)
    data = client.search_notices(
        query=query,
        limit=50,
        iteration_token=token
    )
    print("Calling TED...", flush=True)

    if data is None:# rate limited=retry later
        return 0

    if "notices" not in data:
        raise ValueError("Malformed TED response")

    notices = data["notices"]
    if not notices:
        return 0

    total_inserted = 0

    next_token = data.get("iterationNextToken")
    if next_token:
        state_repo.save_token(config_id, next_token)

    for notice in notices:
        external_id = notice.get("publication-number")

        if raw_repo.exists(external_id):
            continue

        raw_notice = raw_repo.create(notice, country)

        queue.enqueue(str(raw_notice.id))

        total_inserted += 1
    print("NOTICES:", len(notices), flush=True)
    raw_repo.commit()
    
    if total_inserted == 0:
        time.sleep(30)
    return total_inserted
