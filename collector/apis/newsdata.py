import logging
import requests
from requests.exceptions import RequestException

from shared.state import load_state, save_state
from shared.utils import extract_ids, overlap_ratio
from shared.parquet import records_to_parquet_bytes
from shared.blob import upload_bytes
from shared.merge import merge_hour
from shared.config import NEWSDATA_KEY

BASE_URL = "https://newsdata.io/api/1/latest"
DAILY_LIMIT = 200

def run_newsdata(now):
    state = load_state("newsdata")

    if state["calls_today"] >= DAILY_LIMIT:
        logging.info("NewsData daily limit reached")
        return

    try:
        params = {
            "apikey": NEWSDATA_KEY,
            "removeduplicate": 1
        }

        if state.get("next_page"):
            params["page"] = state["next_page"]

        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        articles = payload.get("results", [])
        if not articles:
            logging.info("NewsData returned no results")
            return

        state["next_page"] = payload.get("nextPage")

        ids = extract_ids(articles)
        overlap = overlap_ratio(ids, state.get("last_ids", []))

        if overlap <= 0.7:
            state["last_ids"] = ids[:50]

        parquet_bytes = records_to_parquet_bytes(articles)
        if not parquet_bytes:
            return

        path = f"newsdata/{now:%Y-%m-%d}/{now:%H-%M}.parquet"
        upload_bytes("staging", path, parquet_bytes)

        state["calls_today"] += 1
        save_state("newsdata", state)

        if now.minute == 0:
            try:
                merge_hour("newsdata", now)
            except Exception as e:
                logging.warning(f"NewsData merge failed: {e}")

    except RequestException as e:
        logging.error(f"NewsData request failed: {e}")
    except ValueError as e:
        logging.error(f"NewsData JSON parsing failed: {e}")
    except Exception as e:
        logging.exception(f"Unexpected NewsData error: {e}")
