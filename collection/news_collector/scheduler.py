from datetime import datetime, timedelta
from news_collector.config import API_CONFIG
from news_collector.state_manager import load_state, save_state

def get_due_apis():
    now = datetime.utcnow()
    due_apis = []

    for api_name, cfg in API_CONFIG.items():
        state = load_state(api_name)
        last_run = state.get("last_run_at")
        if last_run:
            last_run = datetime.fromisoformat(last_run.replace("Z", ""))
        interval = timedelta(minutes=cfg["interval_minutes"])

        if not last_run or now - last_run >= interval:
            due_apis.append(api_name)
    return due_apis

def update_last_run(api_name):
    state = load_state(api_name)
    from datetime import datetime
    state["last_run_at"] = datetime.utcnow().isoformat() + "Z"
    save_state(api_name, state)
