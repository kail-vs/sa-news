import logging
import azure.functions as func
from news_collector.scheduler import get_due_apis, update_last_run
from news_collector.api_clients import fetch_articles
from news_collector.storage_manager import append_to_staging, flush_hourly_to_parquet

def main(mytimer: func.TimerRequest) -> None:
    logging.info("News collector triggered.")

    due_apis = get_due_apis()
    logging.info(f"APIs due to run: {due_apis}")

    for api in due_apis:
        try:
            logging.info(f"Running collection for {api}")
            articles = fetch_articles(api)
            append_to_staging(api, articles)
            update_last_run(api)
            flush_hourly_to_parquet(api)
        except Exception as e:
            logging.warning(f"Error processing {api}: {e}")

    logging.info("News collector run completed.")
