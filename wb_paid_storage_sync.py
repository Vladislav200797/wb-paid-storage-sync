#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, time, json, hashlib, datetime as dt, random
from typing import List, Dict, Any, Optional
import requests
from supabase import create_client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = {
    "Authorization": f"Bearer {WB_TOKEN}",
    "Accept": "application/json",
    "User-Agent": "wb-paid-storage-sync/1.1"
}

# Параметры
MAX_DAYS = 8
BATCH_SIZE = 1000
REQ_TIMEOUT = 61
STATUS_MAX_WAIT = int(os.getenv("WB_STATUS_MAX_WAIT", "900"))
STATUS_POLL_STEP_MIN = 5
STATUS_POLL_STEP_MAX = 20
DOWNLOAD_RETRIES = 6    # попытки скачать отчёт (учитывая лимит 1/мин)

# === Вспомогательные ===
def supa():
    return create_client(SB_URL, SB_KEY)

def chunks_8days(d1: dt.date, d2: dt.date):
    cur = d1
    while cur <= d2:
        end = min(cur + dt.timedelta(days=MAX_DAYS-1), d2)
        yield cur, end
        cur = end + dt.timedelta(days=1)

def http_get(url: str, params: Dict[str, Any], timeout: int = REQ_TIMEOUT) -> Any:
    delay = 2
    for attempt in range(1, 7):
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        if r.status_code == 401:
            r.raise_for_status()
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            sleep_for = delay + random.uniform(0, 1.0)
            print(f"WB {r.status_code}, retry in {sleep_for:.1f}s (attempt {attempt}/6)")
            time.sleep(sleep_for)
            delay = min(delay*2, 30)
            continue
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            raise RuntimeError(f"WB returned non-JSON (HTTP {r.status_code})")
    raise RuntimeError("WB request failed after retries")

def norm_date(v: Any) -> Optional[str]:
    return None if not v else str(v)[:10]

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "date":               norm_date(row.get("date")),
        "log_warehouse_coef": row.get("logWarehouseCoef"),
        "office_id":          row.get("officeId"),
        "warehouse":          row.get("warehouse"),
        "warehouse_coef":     row.get("warehouseCoef"),
        "gi_id":              row.get("giId"),
        "chrt_id":            row.get("chrtId"),
        "size":               row.get("size"),
        "barcode":            row.get("barcode"),
        "subject":            row.get("subject"),
        "brand":              row.get("brand"),
        "vendor_code":        row.get("vendorCode"),
        "nm_id":              row.get("nmId"),
        "volume":             row.get("volume"),
        "calc_type":          row.get("calcType"),
        "warehouse_price":    row.get("warehousePrice"),
        "barcodes_count":     row.get("barcodesCount"),
        "pallet_place_code":  row.get("palletPlaceCode"),
        "pallet_count":       row.get("palletCount"),
        "original_date":      norm_date(row.get("originalDate")),
        "loyalty_discount":   row.get("loyaltyDiscount"),
        "tariff_fix_date":    norm_date(row.get("tariffFixDate")),
        "tariff_lower_date":  row.get("tariffLowerDate"),
    }
    out["_hash"] = hashlib.sha256(
        json.dumps(out, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return out

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    # локальная дедупликация
    dedup: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        dedup[key] = r
    clean_rows = list(dedup.values())

    client = supa()
    for i in range(0, len(clean_rows), BATCH_SIZE):
        chunk = clean_rows[i:i+BATCH_SIZE]
        client.table("wb_paid_storage_x").upsert(
            chunk,
            on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        v = payload.get("data", payload)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for k in ("rows", "items", "elements", "list", "result"):
                if isinstance(v.get(k), list):
                    return v[k]
            # если это старт таска — там бывает только taskId
            if "taskId" in v and isinstance(v["taskId"], str):
                return []  # пусть верхний уровень распознает как "путь через таск"
    return []

# === Работа через таск WB (если API вернул taskId) ===
def poll_status(task_id: str) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    waited = 0
    step = STATUS_POLL_STEP_MIN
    while waited < STATUS_MAX_WAIT:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code == 401:
            r.raise_for_status()
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(step)
            waited += step
            step = min(step + 2, STATUS_POLL_STEP_MAX)
            continue
        r.raise_for_status()
        js = r.json()
        status = js.get("data", {}).get("status") or js.get("status")
        if status in ("done", "error", "failed"):
            return status or "unknown"
        # “пульс” примерно раз в минуту
        if waited % 60 == 0:
            print(f"waiting WB task {task_id}, status={status or 'unknown'}, elapsed={waited}s")
        time.sleep(step)
        waited += step
        step = min(step + 2, STATUS_POLL_STEP_MAX)
    return "timeout"

def download_report(task_id: str) -> List[Dict[str, Any]]:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    delay = 65
    for attempt in range(1, DOWNLOAD_RETRIES+1):
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code == 401:
            r.raise_for_status()
        if r.status_code == 429:
            print(f"429 on download, sleep {delay}s (attempt {attempt}/{DOWNLOAD_RETRIES})")
            time.sleep(delay)
            delay = min(delay + 15, 120)
            continue
        r.raise_for_status()
        payload = r.json()
        data = extract_rows(payload)
        if isinstance(data, list):
            return data
        # если пришёл не список — покажем фрагмент и попробуем ещё раз
        print("Unexpected download payload, retrying...",
              json.dumps(payload, ensure_ascii=False)[:200])
        time.sleep(10)
    raise RuntimeError("WB download failed after retries")

# === Основное окно ===
def fetch_window(d_from: dt.date, d_to: dt.date):
    url = f"{WB_BASE}/api/v1/paid_storage"
    params = {"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}
    payload = http_get(url, params)

    # 1) попытка вытащить сразу строки
    data = extract_rows(payload)
    # 2) если строк нет, но это taskId — идём по таску
    task_id: Optional[str] = None
    if isinstance(payload, dict):
        v = payload.get("data", payload)
        if isinstance(v, dict):
            task_id = v.get("taskId")

    if not data and task_id:
        status = poll_status(task_id)
        if status == "done":
            data = download_report(task_id)
        elif status in ("error", "failed"):
            raise RuntimeError(f"Task {task_id} failed: {status}")
        else:
            # new/timeout — логируем и выходим мягко: следующий запуск доберёт
            print(f"Task {task_id} not ready ({status}). Will retry on next run.")
            return

    if not isinstance(data, list):
        snippet = json.dumps(payload, ensure_ascii=False)[:500]
        raise RuntimeError(f"Unexpected WB response (not list). First 500 chars: {snippet}")

    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
    rows = [normalize_row(r) for r in data]
    upsert(rows)
    time.sleep(2)  # лёгкая пауза между окнами

# === Режимы ===
def backfill(year: int):
    start, end = dt.date(year,1,1), dt.date(year,12,31)
    for a,b in chunks_8days(start, end):
        print(f"[BACKFILL] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a,b); break
            except Exception as e:
                print("Retry:", e); time.sleep(10)

def sync(days_back: int = 8):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back-1)
    for a,b in chunks_8days(start, today):
        print(f"[SYNC] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a,b); break
            except Exception as e:
                print("Retry:", e); time.sleep(10)

def mode_range(date_from: str, date_to: str):
    a = dt.date.fromisoformat(date_from)
    b = dt.date.fromisoformat(date_to)
    for df, dt_ in chunks_8days(a, b):
        print(f"[RANGE] {df}..{dt_}")
        for attempt in range(3):
            try:
                fetch_window(df, dt_); break
            except Exception as e:
                print("Retry:", e); time.sleep(10)

def since(date_from: str):
    a = dt.date.fromisoformat(date_from)
    b = dt.date.today()
    mode_range(a.isoformat(), b.isoformat())

# === Entry point ===
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: backfill <year> | sync [days_back] | range <YYYY-MM-DD> <YYYY-MM-DD> | since <YYYY-MM-DD>")
        sys.exit(1)

    mode = sys.argv[1]
    try:
        if mode == "backfill":
            backfill(int(sys.argv[2]))
        elif mode == "sync":
            sync(int(sys.argv[2]) if len(sys.argv) > 2 else 8)
        elif mode == "range":
            mode_range(sys.argv[2], sys.argv[3])
        elif mode == "since":
            since(sys.argv[2])
        else:
            print("Unknown mode")
            sys.exit(1)
    except KeyError as ke:
        missing_env = str(ke).strip("'")
        print(f"Missing required environment variable: {missing_env}")
        sys.exit(2)

