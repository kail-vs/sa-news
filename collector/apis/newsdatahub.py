import logging
import requests
from requests.exceptions import RequestException

from shared.state import load_state, save_state
from shared.utils import extract_ids, overlap_ratio
from shared.parquet import records_to_parquet_bytes
from shared.blob import upload_bytes
from shared.merge import merge_hour
from shared.config import NEWSDATAHUB_KEY

BASE_URL = "https://api.newsdatahub.com/v1/news"
DAILY_LIMIT = 100
PER_PAGE = 20


def run_newsdatahub(now):
    state = load_state("newsdatahub")

    if state["calls_today"] >= DAILY_LIMIT:
        logging.info("NewsDataHub daily limit reached")
        return

    try:
        headers = {"x-api-key": NEWSDATAHUB_KEY}

        params = {
            "per_page": PER_PAGE
        }

        if state.get("current_page"):
            params["cursor"] = state["current_page"]

        response = requests.get(
            BASE_URL,
            headers=headers,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        payload = response.json()

        articles = payload.get("data", [])
        if not articles:
            logging.info("NewsDataHub returned no data")
            return

        next_cursor = payload.get("pagination", {}).get("next_cursor")
        if next_cursor:
            state["current_page"] = next_cursor

        ids = extract_ids(articles)
        overlap = overlap_ratio(ids, state["last_ids"])

        if overlap <= 0.7:
            state["last_ids"] = ids[:50]

        parquet_bytes = records_to_parquet_bytes(articles)
        if not parquet_bytes:
            logging.warning("NewsDataHub parquet conversion returned empty bytes")
            return

        path = f"newsdatahub/{now:%Y-%m-%d}/{now:%H-%M}.parquet"
        upload_bytes("staging", path, parquet_bytes)

        state["calls_today"] += 1
        save_state("newsdatahub", state)

        if now.minute == 0:
            try:
                merge_hour("newsdatahub", now)
            except Exception as e:
                logging.warning(f"NewsDataHub merge failed: {e}")

    except RequestException as e:
        logging.error(f"NewsDataHub request failed: {e}")
    except ValueError as e:
        logging.error(f"NewsDataHub JSON parsing failed: {e}")
    except Exception as e:
        logging.exception(f"Unexpected NewsDataHub error: {e}")
