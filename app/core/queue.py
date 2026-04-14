import redis

class RedisQueue:
    def __init__(self, url: str, queue_name: str):
        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.queue_name = queue_name

    def enqueue(self, notice_id: str):
        self.client.lpush(self.queue_name, notice_id)

    def dequeue(self):
        return self.client.rpop(self.queue_name)
    
    def length(self) -> int:
        return self.client.llen(self.queue_name)
    
    def peek(self, n=5):
        return self.client.lrange(self.queue_name, 0, n-1)
    
    def clear(self):
        self.client.delete(self.queue_name)