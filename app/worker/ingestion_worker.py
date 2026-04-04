# app/worker/ingestion_worker.py

from app.core.database import SessionLocal
from app.services.ingestion import fetch_and_store_notices
import time
import traceback
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from app.core.database import Base, engine
from app.services.ted_client import TedClient
from app.services.ingestion import TokenBucket
import httpx
from app.core.config import settings
from app.core.queue import RedisQueue
from app.repositories.ingestion_repository import (
    RawNoticeRepository,
    IngestionStateRepository,
)



#sometimes service would start before postgres can accept connections, sothis is needed
def wait_for_db():
    while True:
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            db.close()
            print("DB is ready", flush=True)
            return
        except OperationalError:
            print("Waiting for DB...", flush=True)
            time.sleep(2)

def wait_for_tables():
    from sqlalchemy import text

    while True:
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1 FROM ingestion_state LIMIT 1"))
            db.close()
            print("Tables ready", flush=True)
            return
        except Exception:
            print("Waiting for tables...", flush=True)
            time.sleep(2)

def run_ingestion_worker():
    print("Ingestion worker started", flush=True)

    wait_for_db() 
    wait_for_tables()
    bucket = TokenBucket(rate=1/60, capacity=1)

    client = TedClient(
        settings.TED_BASE_URL,
        settings.TED_API_KEY,
        bucket,
        httpx
    )

    queue = RedisQueue(settings.REDIS_URL, "raw_notice_queue")

    while True:
        db = SessionLocal()

        try:
            raw_repo = RawNoticeRepository(db)
            state_repo = IngestionStateRepository(db)

            fetch_and_store_notices(
                client,
                raw_repo,
                state_repo,
                queue,
                "ROU"
            )

        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    run_ingestion_worker()