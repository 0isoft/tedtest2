# app/worker/ingestion_worker.py

from app.core.database import SessionLocal
from app.services.ingestion import fetch_and_store_notices
import time
import traceback
from sqlalchemy.exc import OperationalError
from sqlalchemy import text

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

def run_ingestion_worker():
    print("Ingestion worker started", flush=True)
    wait_for_db() 
    while True:
        db = SessionLocal()

        try:
            fetch_and_store_notices(db, "ROU")
        except Exception:
            traceback.print_exc()
        finally:
            db.close()

        print("Sleeping 60s...", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    run_ingestion_worker()