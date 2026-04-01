import httpx
from app.core.config import settings


class TedClient:
    def __init__(self):
        self.base_url = settings.TED_BASE_URL  
        self.api_key = settings.TED_API_KEY

    def search_notices(self, query, limit=10, iteration_token=None):
        url = f"{self.base_url}/notices/search"

        payload = {
            "query": query,
            "limit": limit,
            "paginationMode": "ITERATION",
            "fields": ["publication-number",
                       "BT-24-Procedure"] 
        }

        if iteration_token:
            payload["iterationNextToken"] = iteration_token

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}", 
        }

        response = httpx.post(url, json=payload, headers=headers)

        print("STATUS:", response.status_code)
        print("BODY:", response.text[:500])

        response.raise_for_status()

        return response.json()