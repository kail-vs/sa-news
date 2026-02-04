from datetime import timedelta
from shared.blob import list_blobs, upload_bytes, delete_blob
from shared.parquet import records_to_parquet_bytes
from shared.config import AZURE_STORAGE_CONNECTION

from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import io
import logging

blob_service = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION
)

def merge_hour(api_name, now):
    merge_time = now - timedelta(hours=1)

    hour_prefix = (
        f"{api_name}/"
        f"{merge_time:%Y-%m-%d}/"
        f"{merge_time:%H}"
    )

    blobs = list_blobs("staging", hour_prefix)

    all_records = []
    blob_paths = []

    for blob in blobs:
        try:
            blob_client = blob_service.get_blob_client(
                container="staging",
                blob=blob.name
            )

            data = blob_client.download_blob().readall()
            table = pq.read_table(io.BytesIO(data))

            all_records.extend(
                table.to_pandas().to_dict("records")
            )
            blob_paths.append(blob.name)

        except Exception as e:
            logging.warning(
                f"Failed to read staging blob {blob.name}: {e}"
            )

    if not all_records:
        logging.info(
            f"No records found for merge "
            f"{api_name} {merge_time:%Y-%m-%d %H}"
        )
        return

    parquet_bytes = records_to_parquet_bytes(all_records)
    if not parquet_bytes:
        return

    main_path = (
        f"{api_name}/"
        f"{merge_time:%Y}/"
        f"{merge_time:%m}/"
        f"{merge_time:%d}/"
        f"{merge_time:%H}.parquet"
    )

    upload_bytes("main", main_path, parquet_bytes)

    for path in blob_paths:
        try:
            delete_blob("staging", path)
        except Exception as e:
            logging.warning(
                f"Failed to delete staging blob {path}: {e}"
            )
