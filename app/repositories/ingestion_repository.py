from app.models.raw_notice import RawNotice
from sqlalchemy.orm import Session
from app.models.ingestion_state import IngestionState


class RawNoticeRepository:
    def __init__(self, db: Session):
        self.db = db

    def exists(self, external_id: str) -> bool:
        return self.db.query(RawNotice).filter_by(
            external_id=external_id
        ).first() is not None

    def create(self, notice: dict, country: str) -> RawNotice:
        obj = RawNotice(
            external_source="TED",
            external_id=notice.get("publication-number"),
            country=country,
            raw_payload=notice,
        )
        self.db.add(obj)
        self.db.flush()
        return obj

    def commit(self):
        self.db.commit()

class IngestionStateRepository:
    def __init__(self, db: Session):
        self.db = db

    def load_token(self, source: str = "ted") -> str | None:
        state = self.db.query(IngestionState).filter_by(source=source).first()
        return state.iteration_token if state else None

    def save_token(self, token: str, source: str = "ted"):
        state = self.db.query(IngestionState).filter_by(source=source).first()

        if not state:
            state = IngestionState(source=source, iteration_token=token)
            self.db.add(state)
        else:
            state.iteration_token = token

        self.db.commit()