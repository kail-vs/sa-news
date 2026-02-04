# Collection Function

This is a **Azure Function** that collects news articles from free APIs (`NewsAPI`, `Newsdata.io`, `NewsDataHub`) and stores them in **Azure Blob Storage**.

* Collects articles at configurable intervals for each API.
* Stores raw JSON temporarily in a **staging container**.
* Flushes hourly batches to **Parquet** files for efficient storage.
* Maintains **state in Blob Storage** to avoid duplicates and track last run time.

---

## Folder Structure

```
storage-account
│
├── state/
│   ├── newsapi_state.json
│   ├── newsdata_state.json
│   └── newsdatahub_state.json
│
├── staging/
│   ├── newsapi/
│   │   └── 2026-02-04/
│   │       └── 10-15.json
│   ├── newsdata/
│   └── newsdatahub/
│
└── main/
    ├── newsapi/
    │   └── 2026-02-04/
    │       └── 10.json
    ├── newsdata/
    └── newsdatahub/
```

---

## Setup

1. **Azure Storage**: Create a storage account and 3 containers:

   * `staging` → temporary JSON storage
   * `main` → hourly Parquet storage
   * `state` → store per-API state

2. **Environment Variables** (Azure Function App Settings):

```
NEWSAPI_KEY = <your key>
NEWSDATA_KEY = <your key>
NEWSDATAHUB_KEY = <your key>
```

3. **Dependencies**:

```bash
pip install -r requirements.txt
```

4. **Deployment**:

```bash
REM Login into your Azure account and select subscription
az login 

func azure functionapp publish fn-news-collector --python
```

---

## Usage

* Function is triggered every 5 minutes (timer trigger).
* Scheduler decides which APIs need to run.
* Articles are collected, staged, and flushed hourly to Parquet.
* Downstream pipelines can process the stored Parquet files.

---