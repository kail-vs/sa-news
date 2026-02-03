import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient


def get_blob_service():
    conn = os.getenv("AZURE_BLOB_CONNECTION")
    if not conn:
        raise RuntimeError("AZURE_BLOB_CONNECTION is not set")
    return BlobServiceClient.from_connection_string(conn)


def get_state_container():
    return os.getenv("STATE_CONTAINER", "state")


def load_state(api_name):
    try:
        blob_service = get_blob_service()
        container = get_state_container()

        blob_client = blob_service.get_blob_client(
            container=container,
            blob=f"{api_name}_state.json"
        )

        data = blob_client.download_blob().readall()
        return json.loads(data)

    except Exception:
        # First run or blob not found
        return {
            "last_run_at": None,
            "last_seen_published_at": None
        }


def save_state(api_name, state_dict):
    blob_service = get_blob_service()
    container = get_state_container()

    blob_client = blob_service.get_blob_client(
        container=container,
        blob=f"{api_name}_state.json"
    )

    blob_client.upload_blob(
        json.dumps(state_dict),
        overwrite=True
    )


def parse_time(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", ""))
