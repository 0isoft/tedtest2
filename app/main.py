from fastapi import FastAPI
from app.api.routes import router
from app.core.database import Base, engine
import app.models

Base.metadata.create_all(bind=engine)

app = FastAPI(title="TED Ingestion Service")

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok"}