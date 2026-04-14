from sqlalchemy import Column, String, DateTime
from datetime import datetime
from app.core.database import Base
from sqlalchemy import Boolean,  Integer

#reason for introducing this = in its absence the workers
#  which perform fetching and notice ingestion dont receive instructions 
# as to when to stop fetching, start fetching, and change criteria for 
# fetching aka applying the filters, 
# instead of running in while loop and with hardcoded country query 
#  and no other filters
# 
# 
# what must happen is instead of  while True: fetch(country="ROU")
# have a while True: read instructions then act accordingly
#  instructions coming from api calls and then get stored in db

# and then the workers reads the insturctions and fetches notices accordingly
# (see worker/ingestion_worker.py)


class IngestionConfig(Base):
    __tablename__ = "ingestion_config"

    id = Column(Integer, primary_key=True)

    source = Column(String, default="ted")

    country = Column(String)
    cpv_code = Column(String, nullable=True)

    is_active = Column(Boolean, default=True)

    # control
    last_run_at = Column(DateTime, nullable=True)
    interval_seconds = Column(Integer, default=60)

    # optional
    priority = Column(Integer, default=0)

    # properties of the 'query execution'
    failure_count = Column(Integer, default=0)
    last_error = Column(String, nullable=True)
    last_failure_at = Column(DateTime, nullable=True)





#this just to toggle worker on/off manually

class IngestionControl(Base):
    __tablename__ = "ingestion_control"

    id = Column(Integer, primary_key=True)
    is_paused = Column(Boolean, default=False)