# app/worker/ingestion_worker.py

from app.core.database import SessionLocal
from app.services.ingestion import fetch_and_store_notices
import time
import traceback
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from app.models.ingestion_config import IngestionConfig,  IngestionControl
from app.services.ted_client import TedClient
from app.services.ingestion import TokenBucket
import httpx
from app.core.config import settings
from app.core.queue import RedisQueue
from app.repositories.ingestion_repository import (
    RawNoticeRepository,
    IngestionStateRepository,
)
from datetime import datetime




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
        control = db.query(IngestionControl).first()

        if control and control.is_paused:
            print("Ingestion paused", flush=True)
            db.close()
            time.sleep(5)
            continue
        

        try:
            #pull from the db the configs and picks the highest priority, amonst the ones that is active
            configs = db.query(IngestionConfig)\
            .filter_by(is_active=True)\
            .order_by(IngestionConfig.priority.desc())\
            .all()

            raw_repo = RawNoticeRepository(db) #this to check if notices exist, create new notices, commit inserts
            state_repo = IngestionStateRepository(db) #this to handle the ingestion state and iteration token
            # note that the token changes in between workers swithcing configurations, its not like you can reuse 
            # it, so we need one ingestionstate per config to hold token for romania, token for romania+cpv, token for belgium
            
            # there may be multiple active configs, for several countries and several cpv codes to go through
            now = datetime.utcnow()
            for config in configs:
                
                #skip configs which were ran recently
                if config.last_run_at and (
                    now - config.last_run_at
                ).total_seconds() < config.interval_seconds:
                    continue
                
                print(f"[INGESTION] Running config {config.id}", flush=True)

                try:
                    fetch_and_store_notices(
                        client=client,
                        raw_repo=raw_repo,
                        state_repo=state_repo,
                        queue=queue,
                        config_id=config.id,
                        country=config.country,
                        cpv_code=config.cpv_code,
                    )

                    config.last_run_at = now
                    config.failure_count = 0
                    config.last_error = None
                except Exception as e:
                    config.failure_count += 1
                    config.last_error = str(e)

                    print(f"[CONFIG ERROR] id={config.id} failure={config.failure_count}")

                    if config.failure_count > 3:
                        config.is_active = False
                        print(f"[DISABLED CONFIG] {config.id}")
                finally:
                    db.commit()

        except Exception:
            traceback.print_exc()
            db.rollback()
        finally:
            db.close()

        time.sleep(5) 


if __name__ == "__main__":
    run_ingestion_worker()