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
    "User-Agent": "wb-paid-storage-sync/1.0"
}

# Предел окна (в днях) и прочие параметры
MAX_DAYS = 8
BATCH_SIZE = 1000
BASE_TIMEOUT = 60  # сек

# === Вспомогательные ===
def supa():
    return create_client(SB_URL, SB_KEY)

def chunks_8days(d1: dt.date, d2: dt.date):
    cur = d1
    while cur <= d2:
        end = min(cur + dt.timedelta(days=MAX_DAYS-1), d2)
        yield cur, end
        cur = end + dt.timedelta(days=1)

def get_with_retries(url: str, params: Dict[str, Any], max_tries: int = 6, timeout: int = BASE_TIMEOUT):
    """
    Аккуратный GET с автоматическими ретраями на 429/5xx.
    На 401 — сразу ошибка (ключ неверный).
    Бэк-офф экспоненциальный + немного джиттера.
    """
    delay = 2
    last_err: Optional[Exception] = None
    for attempt in range(1, max_tries+1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 401:
                # неавторизован — нет смысла ретраить
                r.raise_for_status()
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                # перегрузка/rate-limit — подождём и попробуем снова
                last_err = requests.HTTPError(f"{r.status_code} for {url}", response=r)
                sleep_for = delay + random.uniform(0, 1.0)
                print(f"WB {r.status_code}, retry in {sleep_for:.1f}s (attempt {attempt}/{max_tries})")
                time.sleep(sleep_for)
                delay = min(delay * 2, 30)
                continue
            r.raise_for_status()
            # Пытаемся распарсить JSON, иначе кидаем осмысленную ошибку
            try:
                return r.json()
            except ValueError:
                raise RuntimeError(f"WB returned non-JSON response (HTTP {r.status_code})")
        except Exception as e:
            last_err = e
            if attempt == max_tries:
                break
            sleep_for = delay + random.uniform(0, 1.0)
            print(f"Retry: {e}, wait {sleep_for:.1f}s (attempt {attempt}/{max_tries})")
            time.sleep(sleep_for)
            delay = min(delay * 2, 30)
    # если здесь — значит не смогли получить валидный ответ
    raise RuntimeError(f"WB request failed after {max_tries} attempts: {last_err}")

def norm_date(v: Any) -> Optional[str]:
    return None if not v else str(v)[:10]

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализация WB -> наши поля (snake_case).
    """
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
    # Детерминированный хэш для контроля изменений
    out["_hash"] = hashlib.sha256(
        json.dumps(out, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return out

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    # 1) убрать локальные дубли (на всякий случай)
    dedup: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        key = (r.get("date"), r.get("nm_id"), r.get("chrt_id"), r.get("office_id"))
        dedup[key] = r
    clean_rows = list(dedup.values())

    # 2) upsert пачками
    client = supa()
    for i in range(0, len(clean_rows), BATCH_SIZE):
        chunk = clean_rows[i:i+BATCH_SIZE]
        client.table("wb_paid_storage_x").upsert(
            chunk,
            on_conflict="date,nm_id,chrt_id,office_id"
        ).execute()

def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    WB иногда отдаёт:
      - сразу list,
      - dict с ключом data -> list,
      - dict с data -> dict и там уже rows/items/list/...
    Приводим всё к list.
    """
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

    # неизвестная форма — вернём пустой список, а наверху решим, что делать
    return []

def fetch_window(d_from: dt.date, d_to: dt.date):
    """
    Быстрый путь: прямой вызов отчёта без тасков.
    """
    url = f"{WB_BASE}/api/v1/paid_storage"
    params = {"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}
    payload = get_with_retries(url, params)

    data = extract_rows(payload)
    if not isinstance(data, list):
        snippet = json.dumps(payload, ensure_ascii=False)[:500]
        raise RuntimeError(f"Unexpected WB response (not list). First 500 chars: {snippet}")
    if len(data) == 0 and isinstance(payload, dict) and payload:
        # Поможем себе в логах при пустом извлечении
        print("WB returned dict payload, but no list found in typical keys. First 500 chars:")
        print(json.dumps(payload, ensure_ascii=False)[:500])

    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
    rows = [normalize_row(r) for r in data]
    upsert(rows)

    # Небольшая пауза, чтобы не долбить WB подряд окнами
    time.sleep(2)

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
        # дружелюбная ошибка, если не выставлены переменные окружения
        miss = str(ke).strip("'")
        print(f"Missing required environment variable: {miss}")
        sys.exit(2)
