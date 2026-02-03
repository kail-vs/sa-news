import os
import requests
from datetime import datetime


API_ENDPOINTS = {
    "newsapi": "https://newsapi.org/v2/everything",
    "newsdata_io": "https://newsdata.io/api/1/latest",
    "newsdatahub": "https://api.newsdatahub.com/v1/news"
}


def get_api_key(name):
    key = os.getenv(name)
    if not key:
        raise RuntimeError(f"{name} is not set")
    return key


def fetch_articles(api_name, last_seen_time=None):
    try:
        if api_name == "newsapi":
            return fetch_newsapi(last_seen_time)
        elif api_name == "newsdata_io":
            return fetch_newsdata(last_seen_time)
        elif api_name == "newsdatahub":
            return fetch_newsdatahub(last_seen_time)
        return []
    except Exception as e:
        print(f"[WARN] {api_name} fetch failed: {e}")
        return []


def fetch_newsapi(last_seen_time):
    params = {
        "q": "tesla",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "page": 1,
        "apiKey": get_api_key("NEWSAPI_KEY")
    }
    return fetch_simple(API_ENDPOINTS["newsapi"], params, None, last_seen_time, "newsapi")


def fetch_newsdata(last_seen_time):
    params = {
        "apikey": get_api_key("NEWSDATA_KEY"),
        "removeduplicate": 1,
        "language": "en"
    }
    return fetch_simple(API_ENDPOINTS["newsdata_io"], params, None, last_seen_time, "newsdata_io")


def fetch_newsdatahub(last_seen_time):
    headers = {
        "x-api-key": get_api_key("NEWSDATAHUB_KEY")
    }
    params = {
        "topic": "technology",
        "per_page": 100
    }
    return fetch_simple(API_ENDPOINTS["newsdatahub"], params, headers, last_seen_time, "newsdatahub")


def fetch_simple(url, params, headers, last_seen_time, api_name):
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    articles = extract_articles(api_name, data)

    if last_seen_time:
        articles = [
            a for a in articles
            if parse_time(a["publishedAt"]) and parse_time(a["publishedAt"]) > last_seen_time
        ]

    return articles


def extract_articles(api_name, data):
    if api_name == "newsapi":
        return [{
            "title": a["title"],
            "description": a["description"],
            "url": a["url"],
            "publishedAt": a["publishedAt"],
            "source": a["source"]["name"],
            "api": api_name
        } for a in data.get("articles", [])]

    if api_name == "newsdata_io":
        return [{
            "title": a["title"],
            "description": a.get("description"),
            "url": a["link"],
            "publishedAt": a["pubDate"],
            "source": a["source_id"],
            "api": api_name
        } for a in data.get("results", [])]

    if api_name == "newsdatahub":
        return [{
            "title": a["title"],
            "description": a.get("summary"),
            "url": a["url"],
            "publishedAt": a["published_at"],
            "source": a["source"],
            "api": api_name
        } for a in data.get("data", [])]

    return []


def parse_time(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", ""))
