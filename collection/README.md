# Collection Function

This is a **Azure Function** that collects news articles from free APIs (`NewsAPI`, `Newsdata.io`, `NewsDataHub`) and stores them in **Azure Blob Storage**.

* Collects articles at configurable intervals for each API.
* Stores raw JSON temporarily in a **staging container**.
* Flushes hourly batches to **Parquet** files for efficient storage.
* Maintains **state in Blob Storage** to avoid duplicates and track last run time.

---

## Folder Structure

```
news_collector_project/
├── NewsCollectorFunction/    # Azure Function
│   ├── __init__.py           # Function entrypoint
│   └── function.json         # Timer trigger config
├── news_collector/           # Core code for collection
│   ├── config.py
│   ├── api_clients.py
│   ├── state_manager.py
│   ├── scheduler.py
│   └── storage_manager.py
├── host.json
├── requirements.txt
└── local.settings.json
```

---

## Setup

1. **Azure Storage**: Create a storage account and 3 containers:

   * `staging` → temporary JSON storage
   * `news-data` → hourly Parquet storage
   * `state` → store per-API state

2. **Environment Variables** (Azure Function App Settings):

```
AZURE_BLOB_CONNECTION = <storage connection string>
STAGING_CONTAINER = <staging container>
DATA_CONTAINER = <news-data container>
STATE_CONTAINER = <state container>
NEWSAPI_KEY = <your key>
NEWSDATA_KEY = <your key>
NEWSDATAHUB_KEY = <your key>
```

3. **Dependencies**:

```bash
pip install -r requirements.txt
```

---

## Usage

* Function is triggered every 5 minutes (timer trigger).
* Scheduler decides which APIs need to run.
* Articles are collected, staged, and flushed hourly to Parquet.
* Downstream pipelines can process the stored Parquet files.

---
