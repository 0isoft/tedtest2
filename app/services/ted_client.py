import httpx
from app.core.config import settings
import time

#idea: we can use that iteration token to fetch a small amount of stuff every once in a while.
#  this will naturally allow for the db to be populated cold-start, and if service runs for long enough, 
# to be more or less in-sync with ted and when notifications get pushed, to also be fetched. = eventual consistency with ted
# instead of hammering the ted api with constant requests = rate limited
# but this process needs to be slow. maybe one batch per minute or smth
# solution = use token bucket rate limiter  + persist the iteration-token in db table

# iteration token can never be lost, bcs then ingestion starts from beginning and introduces duplicate risk,
# so ingestion  needs to be idempotent



class TedClient:
    def __init__(self, rate_limiter):
        self.base_url = settings.TED_BASE_URL
        self.api_key = settings.TED_API_KEY
        self.rate_limiter = rate_limiter

    def search_notices(self, query, limit=10, iteration_token=None):
        self.rate_limiter.wait_for_token()

        url = f"{self.base_url}/notices/search"

        payload = {
            "query": query,
            "limit": limit,
            "paginationMode": "ITERATION",
            "fields": ["publication-number", "BT-24-Procedure"]
        }

        if iteration_token:
            payload["iterationNextToken"] = iteration_token

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        response = httpx.post(url, json=payload, headers=headers)
        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text[:1000]) 
        
        # handle rate limit ONCE (not spam loop)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", 60)
            print(f"429 hit. Sleeping {retry_after}s")
            time.sleep(float(retry_after))
            return None

        response.raise_for_status()
        return response.json()
        