import os
import sys
import time
import json
import requests
import datetime as dt
from supabase import create_client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# === Параметры ===
TABLE_NAME            = "wb_paid_storage"
POLL_EVERY_SECONDS    = 5
TIMEOUT_SECONDS       = 100   # ждём максимум 100 сек на одну задачу
POST_DOWNLOAD_SLEEP   = 65    # WB лимит download 1/мин

# === Клиенты ===
supabase = create_client(SB_URL, SB_KEY)
_auth_header = {"Authorization": WB_TOKEN}

# === API Wildberries ===
def create_task(d_from: dt.date, d_to: dt.date):
    r = requests.post(f"{WB_BASE}/api/v1/paid_storage/tasks", headers=_auth_header, json={
        "dateFrom": d_from.isoformat(),
        "dateTo": d_to.isoformat()
    })
    r.raise_for_status()
    return r.json()["taskId"]

def task_status(task_id: str):
    r = requests.get(f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status", headers=_auth_header)
    r.raise_for_status()
    return r.json()["status"]

def download_report(task_id: str):
    r = requests.get(f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download", headers=_auth_header)
    r.raise_for_status()
    return r.json()

# === Хелперы ===
def wait_done(task_id: str):
    start = time.time()
    last_log = 0
    while True:
        s = task_status(task_id)
        now = time.time()
        if s == "done":
            return
        if s in ("error", "failed"):
            raise RuntimeError(f"Task {task_id} failed: {s}")
        if now - start > TIMEOUT_SECONDS:
            raise TimeoutError(f"Task {task_id} stuck (status={s}) > {TIMEOUT_SECONDS}s")
        if now - last_log >= 60:
            print(f"waiting WB task {task_id}, status={s}, elapsed={int(now-start)}s")
            last_log = now
        time.sleep(POLL_EVERY_SECONDS)

def normalize_row(row, task_id):
    row["_task_id"] = task_id
    return row

def upsert(rows):
    if not rows:
        return
    supabase.table(TABLE_NAME).upsert(rows).execute()

# === Логика скачивания ===
def fetch_window(d_from: dt.date, d_to: dt.date):
    for attempt in range(3):
        task_id = create_task(d_from, d_to)
        try:
            time.sleep(2)
            wait_done(task_id)
            data = download_report(task_id)
            print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
            rows = [normalize_row(r, task_id) for r in data]
            upsert(rows)
            time.sleep(POST_DOWNLOAD_SLEEP)
            return
        except TimeoutError as e:
            print(f"{e}; recreate task ({attempt+1}/3)")
            time.sleep(10)
            continue
        except Exception as e:
            print("Retry fetch_window error:", e)
            time.sleep(30)
            continue
    raise RuntimeError(f"Failed to fetch window {d_from}..{d_to} after retries")

# === Режимы ===
def mode_sync(days_back: int):
    today = dt.date.today()
    d_from = today - dt.timedelta(days=days_back)
    d_to = today
    print(f"[SYNC] {d_from}..{d_to}")
    fetch_window(d_from, d_to)

def mode_range(start_str: str, end_str: str):
    d_from = dt.date.fromisoformat(start_str)
    d_to = dt.date.fromisoformat(end_str)
    print(f"[RANGE] {d_from}..{d_to}")
    fetch_window(d_from, d_to)

def mode_backfill(year: int):
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    cur = start
    step = dt.timedelta(days=7)
    while cur <= end:
        wnd_to = min(cur + step - dt.timedelta(days=1), end)
        print(f"[BACKFILL] {cur}..{wnd_to}")
        fetch_window(cur, wnd_to)
        cur += step

# === main ===
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: sync DAYS | range YYYY-MM-DD YYYY-MM-DD | backfill YEAR")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == "sync":
        days_back = int(sys.argv[2])
        mode_sync(days_back)
    elif mode == "range":
        mode_range(sys.argv[2], sys.argv[3])
    elif mode == "backfill":
        mode_backfill(int(sys.argv[2]))
    else:
        print("Unknown mode:", mode)
        sys.exit(1)
