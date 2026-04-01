from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import settings

engine = create_engine(settings.DATABASE_URL) #a factory of connections and pool manager

SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine) #a factory of entityManager
#autocommit=false=>nothing commits unless db.commit(),enables rollback
#autoflush=false=>no sql is sent before commit, so no unexpected queries sent

Base = declarative_base() #base class for  ORM model (opportunities)


#open session per request (@Transactional)
def get_db():
    db = SessionLocal() #create a session instance (entityManager)
    try:
        yield db
    finally:
        db.close()