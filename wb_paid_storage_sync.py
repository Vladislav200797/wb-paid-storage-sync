import os
import sys
import time
import uuid
import json
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Максимальное ожидание выполнения задачи WB (20 минут)
STATUS_MAX_WAIT = 1200
STATUS_INTERVAL = 60

# Название таблицы в Supabase
TABLE_NAME = "wb_paid_storage"

# === Клиент Supabase ===
sb: Client = create_client(SB_URL, SB_KEY)

# === Функции для работы с WB API ===
def wb_headers():
    return {"Authorization": WB_TOKEN}

def wb_create_task(date_from, date_to):
    url = f"{WB_BASE}/api/v1/paid_storage/tasks"
    payload = {"dateFrom": date_from, "dateTo": date_to}
    r = requests.post(url, headers=wb_headers(), json=payload)
    r.raise_for_status()
    return r.json().get("taskId")

def wb_check_task(task_id):
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}"
    r = requests.get(url, headers=wb_headers())
    r.raise_for_status()
    return r.json()

def wb_download_task(task_id):
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    r = requests.get(url, headers=wb_headers())
    r.raise_for_status()
    return r.json()

# === Основная логика ===
def wait_for_task(task_id):
    elapsed = 0
    while elapsed < STATUS_MAX_WAIT:
        status_data = wb_check_task(task_id)
        status = status_data.get("status")
        if status == "done":
            return True
        elif status == "error":
            raise RuntimeError(f"WB task {task_id} failed: {status_data}")
        print(f"waiting WB task {task_id}, status={status}, elapsed={elapsed}s")
        time.sleep(STATUS_INTERVAL)
        elapsed += STATUS_INTERVAL
    print(f"Task {task_id} not ready (timeout). Will retry on next run.")
    return False

def insert_data(rows):
    if not rows:
        return
    # Добавляем _hash для upsert
    for r in rows:
        r["_hash"] = uuid.uuid5(uuid.NAMESPACE_OID, json.dumps(r, sort_keys=True)).hex
    sb.table(TABLE_NAME).upsert(rows, on_conflict=["date", "nm_id", "chrt_id", "office_id"]).execute()

def sync_range(date_from, date_to):
    print(f"[RANGE] {date_from}..{date_to}")
    task_id = wb_create_task(date_from, date_to)
    if wait_for_task(task_id):
        rows = wb_download_task(task_id)
        insert_data(rows)

def sync_last(days):
    date_to = datetime.today().date()
    date_from = date_to - timedelta(days=days)
    sync_range(str(date_from), str(date_to))

def backfill(year):
    start_date = datetime(year, 1, 1).date()
    end_date = datetime(year, 12, 31).date()
    cur = start_date
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=7), end_date)
        print(f"[BACKFILL] {cur}..{chunk_end}")
        sync_range(str(cur), str(chunk_end))
        cur = chunk_end + timedelta(days=1)

# === Точка входа ===
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python wb_paid_storage_sync.py [sync days|range from to|backfill year]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "sync":
        days = int(sys.argv[2])
        sync_last(days)
    elif mode == "range":
        sync_range(sys.argv[2], sys.argv[3])
    elif mode == "backfill":
        backfill(int(sys.argv[2]))
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
