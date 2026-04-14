from app.core.queue import RedisQueue
from app.core.config import settings

def get_queue():
    return RedisQueue(settings.REDIS_URL, "raw_notice_queue")