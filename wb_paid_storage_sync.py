import os, sys, time, json, hashlib, datetime as dt, uuid
from typing import List, Dict, Any
import requests
from supabase import create_client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = { "Authorization": f"Bearer {WB_TOKEN}", "Accept": "application/json" }

MAX_DAYS = 8
POLL_EVERY_SECONDS = 10
TIMEOUT_SECONDS = 1800

# === Вспомогательные ===
def supa():
    return create_client(SB_URL, SB_KEY)

def chunks_8days(d1: dt.date, d2: dt.date):
    cur = d1
    while cur <= d2:
        end = min(cur + dt.timedelta(days=MAX_DAYS-1), d2)
        yield cur, end
        cur = end + dt.timedelta(days=1)

def create_task(d_from: dt.date, d_to: dt.date) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage"
    r = requests.get(url, params={"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["data"]["taskId"]

def task_status(task_id: str) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    delay = 5
    for attempt in range(10):
        r = requests.get(url, headers=_auth_header(True), timeout=60)
        if r.status_code == 401:
            r = requests.get(url, headers=_auth_header(False), timeout=60)
        if r.status_code == 429 or 500 <= r.status_code < 600:
            # перегрузка/спайк — подождём и попробуем снова
            time.sleep(delay)
            delay = min(delay + 5, 30)
            continue
        r.raise_for_status()
        return r.json()["data"]["status"]
    # если 10 попыток так и не прочитали статус — считаем ошибкой
    raise RuntimeError("WB status polling failed after retries")

def wait_done(task_id: str):
    start = time.time()
    last_print = 0
    while True:
        s = task_status(task_id)
        now = time.time()
        if s == "done":
            return
        if s in ("error", "failed"):
            raise RuntimeError(f"Task {task_id} failed: {s}")
        if now - start > TIMEOUT_SECONDS:
            raise TimeoutError(f"Task {task_id} timeout (> {TIMEOUT_SECONDS}s)")
        # раз в минуту печатаем «живой» пульс
        if now - last_print > 60:
            print(f"waiting WB task {task_id}, status={s}, elapsed={int(now-start)}s")
            last_print = now
        time.sleep(POLL_EVERY_SECONDS)

def download_report(task_id: str) -> List[Dict[str, Any]]:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    delay = 65  # сек, лимит download = 1/мин
    for attempt in range(8):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if getattr(e.response, "status_code", None) == 429:
                print(f"429 on download, sleep {delay}s (attempt {attempt+1}/8)")
                time.sleep(delay)
                delay = min(delay + 15, 120)  # лёгкий рост паузы
                continue
            raise


def norm_date(v): 
    return None if not v else v[:10]

def normalize_row(row: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    # WB -> snake_case для нашей таблицы
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
    h = hashlib.sha256(json.dumps(out, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    out["_hash"] = h
    return out

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return

    # 1) убираем дубликаты по PK внутри одного батча (оставляем последнее встреченное)
    dedup = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        dedup[key] = r
    clean_rows = list(dedup.values())

    # 2) upsert пачками
    client = supa()
    for i in range(0, len(clean_rows), 1000):
        chunk = clean_rows[i:i+1000]
        client.table("wb_paid_storage_x").upsert(
            chunk,
            on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

def fetch_window(d_from: dt.date, d_to: dt.date):
    task_id = create_task(d_from, d_to)
    # небольшой лаг перед опросом статуса
    time.sleep(2)
    wait_done(task_id)
    data = download_report(task_id)
    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
    rows = [normalize_row(r, task_id) for r in data]
    upsert(rows)
    # критично: пауза после успешного download, чтобы не ловить 429 на следующем окне
    time.sleep(65)


# === Режимы ===
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

