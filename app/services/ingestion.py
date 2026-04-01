from app.services.ted_client import TedClient


def fetch_sample_notices():
    client = TedClient()

    query = "buyer-country=BEL AND publication-date>=20260101" #needs query optimization

    all_notices = []
    token = None

    for _ in range(5):  # 5pages =? exactly
        data = client.search_notices(
            query=query,
            limit=5,
            iteration_token=token
        )

        notices = data.get("notices", [])
        all_notices.extend(notices)

        token = data.get("iterationNextToken")

        if not token:
            break

    return all_notices