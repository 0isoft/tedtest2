from fastapi import FastAPI
from app.api.routes import router
from app.core.database import Base, engine
import app.models
from app.core.database import SessionLocal
from sqlalchemy.exc import OperationalError
import time
from sqlalchemy import text


#Base.metadata.create_all(bind=engine)

app = FastAPI(title="TED Ingestion Service")

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


app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.on_event("startup")
def startup():
    wait_for_db()
    Base.metadata.create_all(bind=engine)
    wait_for_tables()