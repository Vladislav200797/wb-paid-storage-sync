import os, sys, time, json, hashlib, datetime as dt
from typing import List, Dict, Any
import requests
from supabase import create_client

# ========= Конфиг =========
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = {
    "Authorization": f"Bearer {WB_TOKEN}",
    "Accept": "application/json",
}

TABLE_NAME = "wb_paid_storage_x"
MAX_DAYS = 8

# ожидание статуса
STATUS_INTERVAL = 60           # сек
STATUS_MAX_WAIT = 20 * 60      # 20 мин

def supa():
    return create_client(SB_URL, SB_KEY)

# ========= Утилиты =========
def chunks_8days(d1: dt.date, d2: dt.date):
    cur = d1
    while cur <= d2:
        end = min(cur + dt.timedelta(days=MAX_DAYS-1), d2)
        yield cur, end
        cur = end + dt.timedelta(days=1)

def _get(url, *, params=None, headers=None, timeout=60, max_retries=6, base_delay=3):
    """GET с ретраями на 429/5xx (экспоненциальная пауза)."""
    headers = headers or HEADERS
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue
        r.raise_for_status()
        return r

def _download_with_rate_limit(url, *, headers=None, timeout=120):
    """download endpoint у WB лимитируется ~1/мин — аккуратно работаем с 429."""
    headers = headers or HEADERS
    delay = 65
    for attempt in range(1, 10):
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 429:
            time.sleep(delay)
            delay = min(delay + 15, 120)
            continue
        r.raise_for_status()
        return r
    # если сюда попали — крайний случай
    r.raise_for_status()

# ========= WB API =========
def create_task(d_from: dt.date, d_to: dt.date) -> str:
    # ВАЖНО: именно GET /api/v1/paid_storage, а не POST /tasks
    url = f"{WB_BASE}/api/v1/paid_storage"
    params = {"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}
    r = _get(url, params=params, timeout=60)
    js = r.json()
    # ожидаем {"data":{"taskId":"..."}}
    task_id = (js.get("data") or {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"Unexpected create_task response: {js}")
    return task_id

def task_status(task_id: str) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    r = _get(url, timeout=30)
    # ожидаем {"data":{"status":"new|done|error"}}
    js = r.json()
    data = js.get("data") or {}
    status = data.get("status")
    if not status:
        raise RuntimeError(f"Unexpected task_status response: {js}")
    return status

def wait_done(task_id: str) -> bool:
    elapsed = 0
    while elapsed < STATUS_MAX_WAIT:
        s = task_status(task_id)
        if s == "done":
            return True
        if s in ("error", "failed"):
            raise RuntimeError(f"WB task {task_id} failed with status={s}")
        print(f"waiting WB task {task_id}, status={s}, elapsed={elapsed}s")
        time.sleep(STATUS_INTERVAL)
        elapsed += STATUS_INTERVAL
    print(f"Task {task_id} not ready (timeout). Will retry on next run.")
    return False

def download_report(task_id: str) -> List[Dict[str, Any]]:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    r = _download_with_rate_limit(url)
    js = r.json()
    if isinstance(js, list):
        return js
    # иногда API заворачивает лист в {"data":[...]}
    if isinstance(js, dict):
        if isinstance(js.get("data"), list):
            return js["data"]
        # отладочный вывод, если WB внезапно вернул не список
        snippet = json.dumps(js, ensure_ascii=False)[:500]
        print("WB returned non-list payload. First 500 chars:\n" + snippet)
        return []
    return []

# ========= Нормализация/запись =========
def norm_date(v): 
    return None if not v else v[:10]

def normalize_row(row: Dict[str, Any], task_id: str) -> Dict[str, Any]:
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
        "tariff_lower_date":  norm_date(row.get("tariffLowerDate")),
        "_source_task_id":    task_id,
    }
    # детерминированный хэш для контроля изменений
    out["_hash"] = hashlib.sha256(
        json.dumps(out, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return out

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    # убираем дубли в одном батче по PK
    dedup = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        dedup[key] = r
    clean_rows = list(dedup.values())

    client = supa()
    for i in range(0, len(clean_rows), 1000):
        chunk = clean_rows[i:i+1000]
        client.table(TABLE_NAME).upsert(
            chunk,
            on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

def fetch_window(d_from: dt.date, d_to: dt.date):
    task_id = create_task(d_from, d_to)
    # небольшой лаг перед опросом
    time.sleep(2)
    if not wait_done(task_id):
        return
    data = download_report(task_id)
    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
    rows = [normalize_row(r, task_id) for r in data]
    upsert(rows)
    # после успешного download выдержим паузу, чтобы не словить 429 в следующем окне
    time.sleep(65)

# ========= Режимы =========
def backfill(year: int):
    start, end = dt.date(year,1,1), dt.date(year,12,31)
    for a,b in chunks_8days(start, end):
        print(f"[BACKFILL] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a,b); break
            except Exception as e:
                print("Retry:", e); time.sleep(30)

def sync(days_back: int = 8):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back-1)
    for a,b in chunks_8days(start, today):
        print(f"[SYNC] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a,b); break
            except Exception as e:
                print("Retry:", e); time.sleep(30)

def mode_range(date_from: str, date_to: str):
    a = dt.date.fromisoformat(date_from)
    b = dt.date.fromisoformat(date_to)
    for df, dt_ in chunks_8days(a, b):
        print(f"[RANGE] {df}..{dt_}")
        for attempt in range(3):
            try:
                fetch_window(df, dt_); break
            except Exception as e:
                print("Retry:", e); time.sleep(30)

def since(date_from: str):
    a = dt.date.fromisoformat(date_from)
    b = dt.date.today()
    mode_range(a.isoformat(), b.isoformat())

# ========= main =========
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: backfill <year> | sync [days_back] | range <YYYY-MM-DD> <YYYY-MM-DD> | since <YYYY-MM-DD>")
        sys.exit(1)
    mode = sys.argv[1]
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
