# load_paid_storage.py
# -*- coding: utf-8 -*-
"""
Загрузка отчёта "Платное хранение" WB в Supabase без склейки строк и без дублей.
Особенности:
- upsert по _hash (уникальность полной строки) — _hash считается только по бизнес-полям (без _source_task_id)
- ретраи/бэкофф к WB (create/status/download)
- ретраи/уменьшение батча при апсерте в Supabase
- backfill разбивает период на окна по 8 дней, уважает лимиты WB, при сбое окно помечает и идёт дальше

Команды:
    python load_paid_storage.py day YYYY-MM-DD
    python load_paid_storage.py range YYYY-MM-DD YYYY-MM-DD      (<= 8 дней)
    python load_paid_storage.py backfill YYYY-MM-DD YYYY-MM-DD   (любой период)

.env:
    WB_API_BASE=https://seller-analytics-api.wildberries.ru
    WB_API_TOKEN=***токен аналитики***
    SUPABASE_URL=***ссылка_supabase***
    SUPABASE_SERVICE_ROLE_KEY=***service_role_key***
Опционально:
    WB_BACKFILL_COOLDOWN_SECONDS=70   # пауза между окнами (сек)
"""

import os
import sys
import time
import json
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Iterable, Tuple

import requests
from requests.exceptions import ReadTimeout as ReqReadTimeout, ConnectionError as ReqConnError, HTTPError as ReqHTTPError
from supabase import create_client
from dotenv import load_dotenv

# для ретраев на запись в Supabase (внутри supabase используется httpx)
from httpx import ReadTimeout as HttpxReadTimeout, ConnectError as HttpxConnectError, HTTPError as HttpxHTTPError

# ================== Конфиг ==================
load_dotenv()

WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

MAX_DAYS = 8  # ограничение WB на окно

# лимиты/паузы
POLL_EVERY_SECONDS = 10
PRINT_HEARTBEAT_EVERY = 60
OVERALL_WAIT_SECONDS = 300
DOWNLOAD_TIMEOUT = 120

# ретраи к WB
WB_RETRIES = 5
WB_BACKOFF_BASE = 3  # сек: 3,6,9,...

# таблица в Supabase
TABLE_NAME = "wb_paid_storage_x"

HEADERS = {
    "Authorization": f"Bearer {WB_TOKEN}",
    "Accept": "application/json"
}

# ================== Helpers ==================
def supa():
    return create_client(SB_URL, SB_KEY)

def _safe_json(r: requests.Response) -> Any:
    try:
        return r.json()
    except Exception:
        text = (r.text or "")[:500]
        raise RuntimeError(f"WB returned non-JSON (status {r.status_code}): {text}")

def _d10(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return str(s)[:10]

def windowize(start: dt.date, end: dt.date, max_days: int = MAX_DAYS) -> Iterable[Tuple[dt.date, dt.date]]:
    """Разбивает [start..end] на окна по max_days (включая границы)."""
    cur = start
    delta = dt.timedelta(days=max_days - 1)
    while cur <= end:
        w_end = min(cur + delta, end)
        yield (cur, w_end)
        cur = w_end + dt.timedelta(days=1)

def safe_get(url: str, *, params=None, headers=None, timeout=60) -> requests.Response:
    """GET с ретраями/бэкоффом для WB."""
    last_err = None
    for attempt in range(1, WB_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            # 429/5xx — повторим
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise ReqHTTPError(f"WB {r.status_code} {r.reason}")
            return r
        except (ReqReadTimeout, ReqConnError, ReqHTTPError) as e:
            last_err = e
            sleep_s = WB_BACKOFF_BASE * attempt
            print(f"[WB retry {attempt}/{WB_RETRIES}] {e}; sleep {sleep_s}s ...")
            time.sleep(sleep_s)
    if last_err:
        raise last_err
    raise RuntimeError("WB safe_get: unexpected flow")

# ================== WB API ==================
def wb_create_task(date_from: dt.date, date_to: dt.date) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage"
    r = safe_get(url, params={"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()},
                 headers=HEADERS, timeout=60)
    payload = _safe_json(r)
    task_id = payload["data"]["taskId"]
    if not task_id or not isinstance(task_id, str):
        raise RuntimeError(f"Unexpected WB response (create task): {payload}")
    return task_id

def wb_task_status(task_id: str) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    r = safe_get(url, headers=HEADERS, timeout=30)
    payload = _safe_json(r)
    return payload["data"]["status"]

def wb_wait_done(task_id: str, overall_seconds: int = OVERALL_WAIT_SECONDS) -> Optional[str]:
    """Ждём готовности task до overall_seconds. Возвращаем done/error/None."""
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
    """Скачивание отчёта. Внутри — собственные ретраи (long backoff) как раньше."""
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    delay = 65
    for attempt in range(6):
        try:
            r = safe_get(url, headers=HEADERS, timeout=DOWNLOAD_TIMEOUT)
            data = _safe_json(r)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for key in ("data", "result", "items", "rows"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                snippet = json.dumps(data, ensure_ascii=False)[:300]
                print("WB returned dict payload, but no list found in typical keys. Snippet:", snippet)
                return []
            return []
        except (ReqReadTimeout, ReqConnError, ReqHTTPError) as e:
            print(f"WB download error: {e} — sleep {delay}s (attempt {attempt+1}/6)")
            time.sleep(delay)
            delay = min(delay + 15, 120)
    return []

# ================== Трансформация + Supabase ==================
def normalize_row(row: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    """Ключи -> snake_case под таблицу; _hash считаем ТОЛЬКО по бизнес-полям (без _source_task_id)."""
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
        "_source_task_id":    task_id,  # метаданные — НЕ входят в hash
    }
    # Хэш — ТОЛЬКО по бизнес-полям (без _source_task_id)
    hash_base = {k: out[k] for k in out.keys() if k != "_source_task_id"}
    out["_hash"] = hashlib.sha256(
        json.dumps(hash_base, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return out

def upsert_rows(rows: List[Dict[str, Any]]):
    """
    Надёжная запись в Supabase c ретраями и уменьшением батча при таймаутах.
    on_conflict="_hash" — не склеиваем разные строки одного дня, дублей не будет.
    """
    if not rows:
        return

    client = supa()
    base_chunk = 200  # стартовый размер батча

    i = 0
    n = len(rows)
    while i < n:
        chunk_size = base_chunk
        while True:
            j = min(i + chunk_size, n)
            chunk = rows[i:j]
            try:
                client.table(TABLE_NAME).upsert(
                    chunk,
                    on_conflict="_hash"
                ).execute()
                i = j
                break
            except (HttpxReadTimeout, HttpxConnectError, HttpxHTTPError) as e:
                if chunk_size <= 50:
                    print(f"Supabase upsert timeout/conn error: {e}. sleep 5s and retry same chunk...")
                    time.sleep(5)
                else:
                    chunk_size = max(50, chunk_size // 2)
                    print(f"Upsert error: {type(e).__name__}. Reduce chunk to {chunk_size} and retry...")
                    time.sleep(1)
            except Exception as e:
                if chunk_size <= 50:
                    print(f"Unexpected upsert error: {e}. sleep 5s and retry...")
                    time.sleep(5)
                else:
                    chunk_size = max(50, chunk_size // 2)
                    print(f"Unexpected upsert error: {e}. Reduce chunk to {chunk_size} and retry...")
                    time.sleep(1)

# ================== Основной сценарий ==================
def process_window(date_from: dt.date, date_to: dt.date) -> str:
    if (date_to - date_from).days > (MAX_DAYS - 1):
        raise ValueError("WB API: максимум 8 дней в одном окне")
    print(f"[SYNC] {date_from}..{date_to}")

    for attempt in range(1, 4):  # до 3 попыток на окно
        try:
            task_id = wb_create_task(date_from, date_to)
            time.sleep(2)
            final_status = wb_wait_done(task_id)
            if final_status is None:
                print(f"Task {task_id} not ready (timeout).")
                raise RuntimeError("task timeout")
            if final_status != "done":
                raise RuntimeError(f"Task {task_id} ended with status={final_status}.")

            data = wb_download(task_id)
            print(f"rows downloaded: {len(data)} for {date_from}..{date_to}")

            rows = [normalize_row(r, task_id) for r in data]
            upsert_rows(rows)
            return "ok"
        except Exception as e:
            sleep_s = 10 * attempt
            print(f"[SYNC retry {attempt}/3] window {date_from}..{date_to} failed: {e}. sleep {sleep_s}s...")
            time.sleep(sleep_s)

    return "failed"

def process_backfill(start: dt.date, end: dt.date) -> None:
    total_days = (end - start).days + 1
    print(f"[BACKFILL] {start}..{end} ({total_days} days)")

    done_windows = 0
    for d_from, d_to in windowize(start, end, MAX_DAYS):
        status = process_window(d_from, d_to)
        done_windows += 1

        with open("backfill_progress.txt", "a", encoding="utf-8") as f:
            f.write(f"{dt.datetime.now().isoformat()} processed {d_from}..{d_to} -> {status}\n")

        if status == "failed":
            with open("failed_windows.txt", "a", encoding="utf-8") as f:
                f.write(f"{d_from},{d_to}\n")
            print(f"[BACKFILL] window {d_from}..{d_to} FAILED — continue to next window")

        cooldown = int(os.getenv("WB_BACKFILL_COOLDOWN_SECONDS", "70"))
        print(f"[BACKFILL] cooldown {cooldown}s before next window...")
        time.sleep(cooldown)

    print(f"[BACKFILL] Completed: {done_windows} windows.")

# ================== CLI ==================
def main(argv: List[str]) -> int:
    try:
        if len(argv) < 2:
            print(
                "Usage:\n"
                "  python load_paid_storage.py day YYYY-MM-DD\n"
                "  python load_paid_storage.py range YYYY-MM-DD YYYY-MM-DD\n"
                "  python load_paid_storage.py backfill YYYY-MM-DD YYYY-MM-DD"
            )
            return 1

        cmd = argv[1]
        if cmd == "day":
            if len(argv) < 3:
                print("Usage: python load_paid_storage.py day YYYY-MM-DD")
                return 1
            d = dt.date.fromisoformat(argv[2])
            process_window(d, d)
            return 0

        if cmd == "range":
            if len(argv) < 4:
                print("Usage: python load_paid_storage.py range YYYY-MM-DD YYYY-MM-DD")
                return 1
            d1 = dt.date.fromisoformat(argv[2])
            d2 = dt.date.fromisoformat(argv[3])
            if d2 < d1:
                print("range: end date < start date")
                return 1
            if (d2 - d1).days > (MAX_DAYS - 1):
                print(f"range: WB API позволяет максимум {MAX_DAYS} дней за раз")
                return 1
            process_window(d1, d2)
            return 0

        if cmd == "backfill":
            if len(argv) < 4:
                print("Usage: python load_paid_storage.py backfill YYYY-MM-DD YYYY-MM-DD")
                return 1
            d1 = dt.date.fromisoformat(argv[2])
            d2 = dt.date.fromisoformat(argv[3])
            if d2 < d1:
                print("backfill: end date < start date")
                return 1
            process_backfill(d1, d2)
            return 0

        print("Unknown command")
        return 1

    except KeyError as ke:
        missing = str(ke).strip("'")
        print(f"Missing required environment variable: {missing}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv))
