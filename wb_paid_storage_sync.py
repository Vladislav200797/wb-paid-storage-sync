import os
import sys
import time
import json
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
from supabase import create_client

# ================== Конфиг ==================
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# до 8 дней за одно окно
MAX_DAYS = 8

# лимиты/паузы
POLL_EVERY_SECONDS = 10            # опрос статуса
PRINT_HEARTBEAT_EVERY = 60         # раз в минуту писать «живой» статус
OVERALL_WAIT_SECONDS = 240         # сколько максимум ждём готовности task за один запуск
DOWNLOAD_TIMEOUT = 120             # таймаут HTTP при download
AFTER_DOWNLOAD_COOLDOWN = 65       # пауза, чтобы не ловить 429 на следующем окне

# таблица в Supabase (PK: date,nm_id,chrt_id,office_id)
TABLE_NAME = "wb_paid_storage_x"

HEADERS = {
    "Authorization": f"Bearer {WB_TOKEN}",
    "Accept": "application/json"
}

# ================== Помощники ==================
def supa():
    return create_client(SB_URL, SB_KEY)

def clamp_days_back(n: int) -> int:
    if n < 1: return 1
    if n > MAX_DAYS: return MAX_DAYS
    return n

def dates_window(days_back: int) -> tuple[dt.date, dt.date]:
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back - 1)
    return start, today

def _safe_json(r: requests.Response) -> Any:
    try:
        return r.json()
    except Exception:
        text = (r.text or "")[:500]
        raise RuntimeError(f"WB returned non-JSON (status {r.status_code}): {text}")

# ================== WB API ==================
def wb_create_task(date_from: dt.date, date_to: dt.date) -> str:
    """
    Создаёт task через GET /api/v1/paid_storage?dateFrom&dateTo
    Возвращает taskId.
    """
    url = f"{WB_BASE}/api/v1/paid_storage"
    r = requests.get(
        url,
        params={"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()},
        headers=HEADERS,
        timeout=60,
    )
    r.raise_for_status()
    payload = _safe_json(r)
    # ожидаем {"data":{"taskId":"..."}} от WB
    try:
        task_id = payload["data"]["taskId"]
        if not task_id or not isinstance(task_id, str):
            raise KeyError
        return task_id
    except Exception:
        snippet = json.dumps(payload, ensure_ascii=False)[:500]
        raise RuntimeError(f"Unexpected WB response (create task): {snippet}")

def wb_task_status(task_id: str) -> str:
    """
    GET /api/v1/paid_storage/tasks/{taskId}/status -> {"data":{"status":"new|processing|done|error"}}
    С экспоненциальным бэкоффом на 429/5xx — но не более пары секунд на вызов.
    """
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    delay = 1
    for _ in range(6):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code in (429,) or (500 <= r.status_code < 600):
            time.sleep(delay)
            delay = min(delay * 2, 6)
            continue
        r.raise_for_status()
        payload = _safe_json(r)
        try:
            return payload["data"]["status"]
        except Exception:
            snippet = json.dumps(payload, ensure_ascii=False)[:500]
            raise RuntimeError(f"Unexpected WB response (status): {snippet}")
    # если так и не вышли из 429/5xx
    r.raise_for_status()
    return "new"  # теоретически недостижимо

def wb_wait_done(task_id: str, overall_seconds: int = OVERALL_WAIT_SECONDS) -> Optional[str]:
    """
    Ожидает статусы 'done'/'error' до overall_seconds.
    Возвращает финальный статус или None, если не дождались.
    """
    start = time.time()
    last_print = 0.0
    while True:
        s = wb_task_status(task_id)
        now = time.time()

        if s == "done":
            return "done"
        if s in ("error", "failed"):
            return s

        if (now - start) > overall_seconds:
            return None

        if (now - last_print) > PRINT_HEARTBEAT_EVERY:
            print(f"waiting WB task {task_id}, status={s}, elapsed={int(now - start)}s")
            last_print = now

        time.sleep(POLL_EVERY_SECONDS)

def wb_download(task_id: str) -> List[Dict[str, Any]]:
    """
    GET /api/v1/paid_storage/tasks/{taskId}/download
    Ловим 429 — ждём 65/80/… секунд.
    """
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    delay = 65
    for attempt in range(6):
        r = requests.get(url, headers=HEADERS, timeout=DOWNLOAD_TIMEOUT)
        if r.status_code == 429:
            print(f"WB 429 on download, sleep {delay}s (attempt {attempt+1}/6)")
            time.sleep(delay)
            delay = min(delay + 15, 120)
            continue
        r.raise_for_status()
        data = _safe_json(r)
        # Некоторые окружения WB возвращают сразу массив, иногда {"data":[...]}
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "result", "items", "rows"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # если это был только ответ с taskId (вдруг скачали слишком рано)
            snippet = json.dumps(data, ensure_ascii=False)[:500]
            print("WB returned dict payload, but no list found in typical keys. First 500 chars:")
            print(snippet)
            return []
        # совсем неожиданно
        return []
    # если после 6 попыток всё равно не получилось — считаем пусто
    return []

# ================== Трансформация + Supabase ==================
def _d10(s: Optional[str]) -> Optional[str]:
    return None if not s else s[:10]

def normalize_row(row: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    out = {
        "date":               _d10(row.get("date")),
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
        "original_date":      _d10(row.get("originalDate")),
        "loyalty_discount":   row.get("loyaltyDiscount"),
        "tariff_fix_date":    _d10(row.get("tariffFixDate")),
        "tariff_lower_date":  _d10(row.get("tariffLowerDate")),
        "_source_task_id":    task_id,
    }
    out["_hash"] = hashlib.sha256(
        json.dumps(out, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return out

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return
    # дедуп внутри батча по PK (date, nm_id, chrt_id, office_id)
    seen = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        seen[key] = r
    rows = list(seen.values())

    client = supa()
    for i in range(0, len(rows), 1000):
        chunk = rows[i:i+1000]
        client.table(TABLE_NAME).upsert(
            chunk, on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

# ================== Основной сценарий ==================
def process_window(date_from: dt.date, date_to: dt.date) -> str:
    print(f"[SYNC] {date_from}..{date_to}")
    task_id = wb_create_task(date_from, date_to)
    # небольшой лаг перед первым статусом
    time.sleep(2)

    final_status = wb_wait_done(task_id)
    if final_status is None:
        print(f"Task {task_id} not ready (timeout). Will retry on next run.")
        return "timeout"
    if final_status != "done":
        print(f"Task {task_id} ended with status={final_status}. Skipping.")
        return "error"

    data = wb_download(task_id)
    print(f"rows downloaded: {len(data)} for {date_from}..{date_to}")

    rows = [normalize_row(r, task_id) for r in data]
    upsert_rows(rows)

    # пауза после успешного download — на случай следующего окна в будущем
    time.sleep(AFTER_DOWNLOAD_COOLDOWN)
    return "ok"

def cmd_sync(days_back: int = 8) -> int:
    n = clamp_days_back(days_back)
    d_from, d_to = dates_window(n)
    status = process_window(d_from, d_to)
    # НИКОГДА не фейлим job — пусть следующий запуск дожмёт.
    return 0

# ================== CLI ==================
def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: sync [days_back<=8]")
        return 1
    if argv[1] == "sync":
        days = 8
        if len(argv) > 2:
            try:
                days = int(argv[2])
            except ValueError:
                pass
        return cmd_sync(days)
    print("Unknown command")
    return 1

if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except KeyError as ke:
        # Никаких backslash внутри f-string выражения
        missing = str(ke).strip("'")
        print(f"Missing required environment variable: {missing}")
        sys.exit(1)
    except Exception as e:
        # на всякий — не роняем job, чтобы расписание продолжало работать
        print(f"Unexpected error: {e}")
        sys.exit(0)
