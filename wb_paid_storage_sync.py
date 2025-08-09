# wb_paid_storage_sync.py
import os, sys, time, json, hashlib, datetime as dt
from typing import List, Dict, Any, Tuple
import requests
from supabase import create_client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]                 # секрет WB_API_TOKEN
SB_URL    = os.environ["SUPABASE_URL"]                 # секрет SUPABASE_URL
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]    # секрет SUPABASE_SERVICE_ROLE_KEY

HEADERS = {
    "Authorization": f"Bearer {WB_TOKEN}",
    "Accept": "application/json",
}

MAX_DAYS_PER_CALL = 8
RETRY_MAX = 6
RETRY_BASE_SLEEP = 2.0   # сек
HTTP_TIMEOUT = 60        # сек
UPSERT_CHUNK = 1000

# ===== helpers =====
def supa():
    return create_client(SB_URL, SB_KEY)

def chunks_8days(d1: dt.date, d2: dt.date) -> List[Tuple[dt.date, dt.date]]:
    cur = d1
    while cur <= d2:
        end = min(cur + dt.timedelta(days=MAX_DAYS_PER_CALL-1), d2)
        yield cur, end
        cur = end + dt.timedelta(days=1)

def get_with_retries(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    Делает GET с экспоненциальным ретраем на 401/429/5xx.
    """
    delay = RETRY_BASE_SLEEP
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=HTTP_TIMEOUT)
            # мягкая обработка перегрузок
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} Retriable", response=r)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            # на 401 у WB бывает редкий глюк — пробуем ещё раз
            if code in (401, 429) or (code is not None and 500 <= code < 600):
                if attempt < RETRY_MAX:
                    time.sleep(delay)
                    delay = min(delay * 1.8, 20)
                    continue
            raise
        except requests.RequestException:
            if attempt < RETRY_MAX:
                time.sleep(delay)
                delay = min(delay * 1.8, 20)
                continue
            raise

def norm_date(v: Any):
    return None if not v else str(v)[:10]

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
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
    }
    # детерминированный хэш для контроля изменений
    out["_hash"] = hashlib.sha256(
        json.dumps(out, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return out

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    # внутри батча удалим дубликаты PK (date, nm_id, chrt_id, office_id)
    dedup = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        dedup[key] = r
    clean = list(dedup.values())

    client = supa()
    for i in range(0, len(clean), UPSERT_CHUNK):
        chunk = clean[i:i+UPSERT_CHUNK]
        client.table("wb_paid_storage_x").upsert(
            chunk,
            on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

def fetch_window(d_from: dt.date, d_to: dt.date):
    """
    Прямой запрос без тасков:
    GET /api/v1/paid_storage?dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD
    """
    url = f"{WB_BASE}/api/v1/paid_storage"
    params = {"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}
    payload = get_with_retries(url, params)

    # WB может вернуть {"data":[...]} или сразу список — поддержим оба
    data = payload.get("data", payload)

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected WB response: {type(data)}")

    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")

    rows = [normalize_row(r) for r in data]
    upsert(rows)

    # бережная пауза, чтобы не словить 429 при последовательных окнах
    time.sleep(2)

# ===== режимы =====
def backfill(year: int):
    start, end = dt.date(year, 1, 1), dt.date(year, 12, 31)
    for a, b in chunks_8days(start, end):
        print(f"[BACKFILL] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a, b)
                break
            except Exception as e:
                print("Retry:", e); time.sleep(5)

def sync(days_back: int = 8):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back-1)
    for a, b in chunks_8days(start, today):
        print(f"[SYNC] {a}..{b}")
        for attempt in range(3):
            try:
                fetch_window(a, b)
                break
            except Exception as e:
                print("Retry:", e); time.sleep(5)

def mode_range(date_from: str, date_to: str):
    a = dt.date.fromisoformat(date_from)
    b = dt.date.fromisoformat(date_to)
    for df, dt_ in chunks_8days(a, b):
        print(f"[RANGE] {df}..{dt_}")
        for attempt in range(3):
            try:
                fetch_window(df, dt_)
                break
            except Exception as e:
                print("Retry:", e); time.sleep(5)

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
