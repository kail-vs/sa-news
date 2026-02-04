import logging
import requests
from requests.exceptions import RequestException

from shared.state import load_state, save_state
from shared.utils import extract_ids, overlap_ratio
from shared.parquet import records_to_parquet_bytes
from shared.blob import upload_bytes
from shared.merge import merge_hour
from shared.config import NEWSAPI_KEY

BASE_URL = "https://newsapi.org/v2/everything"
DAILY_LIMIT = 100

def run_newsapi(now):
    state = load_state("newsapi")

    if state["calls_today"] >= DAILY_LIMIT:
        logging.info("NewsAPI daily limit reached")
        return

    try:
        params = {
            "q": "news",
            "sortBy": "publishedAt",
            "page": state["current_page"],
            "pageSize": 100,
            "apiKey": NEWSAPI_KEY
        }

        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        articles = payload.get("articles", [])
        if not articles:
            logging.info("NewsAPI returned no articles")
            return

        ids = extract_ids(articles)
        overlap = overlap_ratio(ids, state["last_ids"])

        if overlap > 0.7:
            state["stagnation_count"] += 1
            state["current_page"] = (
                state["current_page"] % state["max_page"]
            ) + 1
        else:
            state["stagnation_count"] = 0
            state["last_ids"] = ids[:50]

        parquet_bytes = records_to_parquet_bytes(articles)
        if not parquet_bytes:
            return

        path = f"newsapi/{now:%Y-%m-%d}/{now:%H-%M}.parquet"
        upload_bytes("staging", path, parquet_bytes)

        state["calls_today"] += 1
        save_state("newsapi", state)

        if now.minute == 0:
            try:
                merge_hour("newsapi", now)
            except Exception as e:
                logging.warning(f"NewsAPI merge failed: {e}")

    except RequestException as e:
        logging.error(f"NewsAPI request failed: {e}")
    except ValueError as e:
        logging.error(f"NewsAPI JSON parsing failed: {e}")
    except Exception as e:
        logging.exception(f"Unexpected NewsAPI error: {e}")
