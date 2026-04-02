import redis

r = redis.Redis(host="redis", port=6379, decode_responses=True)

QUEUE_NAME = "raw_notice_queue"


def enqueue_notice(notice_id: str):
    r.lpush(QUEUE_NAME, notice_id)


def dequeue_notice():
    return r.rpop(QUEUE_NAME)