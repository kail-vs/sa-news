from datetime import datetime
from shared.blob import download_json, upload_json

def load_state(api_name):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    state = download_json("state", f"{api_name}_state.json")

    if not state or state.get("date") != today:
        return {
            "date": today,
            "calls_today": 0,
            "current_page": 1,
            "max_page": 5,
            "last_ids": [],
            "stagnation_count": 0
        }

    return state

def save_state(api_name, state):
    upload_json("state", f"{api_name}_state.json", state)
