import os
import io
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient


def get_blob_service():
    conn = os.getenv("AZURE_BLOB_CONNECTION")
    if not conn:
        raise RuntimeError("AZURE_BLOB_CONNECTION is not set")
    return BlobServiceClient.from_connection_string(conn)


def get_container(name, default):
    return os.getenv(name, default)


def append_to_staging(api_name, articles):
    if not articles:
        return

    blob_service = get_blob_service()
    staging_container = get_container("STAGING_CONTAINER", "staging")

    now = datetime.utcnow()
    hour_key = now.strftime("%Y%m%d%H")
    staging_blob_name = f"{api_name}/hourly_{hour_key}.json"

    blob_client = blob_service.get_blob_client(
        container=staging_container,
        blob=staging_blob_name
    )

    try:
        existing = blob_client.download_blob().readall()
        existing_df = pd.read_json(existing)
        new_df = pd.DataFrame(articles)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    except Exception:
        combined = pd.DataFrame(articles)

    blob_client.upload_blob(
        combined.to_json(orient="records"),
        overwrite=True
    )


def flush_hourly_to_parquet(api_name):
    blob_service = get_blob_service()
    staging_container = get_container("STAGING_CONTAINER", "staging")
    data_container = get_container("DATA_CONTAINER", "news-data")

    now = datetime.utcnow()
    hour_key = now.strftime("%Y%m%d%H")
    staging_blob_name = f"{api_name}/hourly_{hour_key}.json"

    staging_blob = blob_service.get_blob_client(
        container=staging_container,
        blob=staging_blob_name
    )

    try:
        data = staging_blob.download_blob().readall()
        df = pd.read_json(data)

        if df.empty:
            return

        path = (
            f"{api_name}/"
            f"year={now.year}/"
            f"month={now.month:02}/"
            f"day={now.day:02}/"
            f"hour={now.hour:02}/"
            f"batch_{hour_key}.parquet"
        )

        blob_client = blob_service.get_blob_client(
            container=data_container,
            blob=path
        )

        out = io.BytesIO()
        df.to_parquet(
            out,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        out.seek(0)

        blob_client.upload_blob(out, overwrite=True)
        staging_blob.delete_blob()

    except Exception as e:
        print(f"[WARN] Flush failed for {api_name}: {e}")
