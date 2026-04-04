import redis

class RedisQueue:
    def __init__(self, url: str, queue_name: str):
        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.queue_name = queue_name

    def enqueue(self, notice_id: str):
        self.client.lpush(self.queue_name, notice_id)

    def dequeue(self):
        return self.client.rpop(self.queue_name)