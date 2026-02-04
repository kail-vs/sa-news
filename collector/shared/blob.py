import json
from azure.storage.blob import BlobServiceClient
from shared.config import AZURE_STORAGE_CONNECTION

blob_service = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION
)

def upload_bytes(container, path, data: bytes):
    client = blob_service.get_blob_client(container, path)
    client.upload_blob(data, overwrite=True)

def upload_json(container, path, data):
    upload_bytes(container, path, json.dumps(data).encode("utf-8"))

def download_json(container, path):
    try:
        client = blob_service.get_blob_client(container, path)
        return json.loads(client.download_blob().readall())
    except Exception:
        return None

def list_blobs(container, prefix):
    container_client = blob_service.get_container_client(container)
    return container_client.list_blobs(name_starts_with=prefix)

def delete_blob(container, path):
    client = blob_service.get_blob_client(container, path)
    client.delete_blob()
