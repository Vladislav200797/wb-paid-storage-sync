#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime as dt
from typing import List, Dict, Any, Iterable, Tuple

import requests
from supabase import create_client, Client


# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]                # имя секрета: WB_API_TOKEN
SB_URL    = os.environ["SUPABASE_URL"]                # имя секрета: SUPABASE_URL
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]   # имя секрета: SUPABASE_SERVICE_ROLE_KEY

HEADERS = { "Authorization": f"Bearer {WB_TOKEN}", "Accept": "application/json" }

# === Настройки ожидания WB ===
POLL_EVERY_SECONDS = 5     # как часто опрашивать статус
TIMEOUT_SECONDS    = 180   # максимум ждём 3 минуты, если WB «висит» — попробуем другое окно/повтор

# === Лимиты WB ===
# download: 1 запрос в минуту -> после успешного скачивания ждём 65с перед следующим окном
POST_DOWNLOAD_SLEEP = 65

# === Таблица и ключ апсерта ===
TABLE_NAME = "wb_paid_storage_x"
ON_CONFLICT = "date,nm_id,chrt_id,office_id"


# ---------- Утилиты дат ----------

def chunks_8days(a: dt.date, b: dt.date) -> Iterable[Tuple[dt.date, dt.date]]:
    """
    Разбивает [a..b] на окна по 8 дней включительно:
    [1..8], [9..16], ... Последнее окно может быть короче.
    """
    cur = a
    one_day = dt.timedelta(days=1)
    while cur <= b:
        end = cur + dt.timedelta(days=7)
        if end > b:
            end = b
        yield (cur, end)
        cur = end + one_day


def parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def today_local() -> dt.date:
    # просто дата «сейчас» (на раннере это UTC, но для логики окон не критично)
    return dt.date.today()


# ---------- WB API ----------

def create_task(d_from: dt.date, d_to: dt.date) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage"
    params = {"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()}
    r = requests.get(url, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["data"]["taskId"]


def task_status(task_id: str) -> str:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status"
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["data"]["status"]


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
            raise TimeoutError(f"Task {task_id} timeout (> {TIMEOUT_SECONDS}s), status={s}")
        if now - last_print > 60:
            print(f"waiting WB task {task_id}, status={s}, elapsed={int(now-start)}s")
            last_print = now
        time.sleep(POLL_EVERY_SECONDS)


def download_report(task_id: str) -> List[Dict[str, Any]]:
    url = f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
    delay = POST_DOWNLOAD_SLEEP  # лимит download = 1/мин
    for attempt in range(8):
        r = requests.get(url, headers=HEADERS, timeout=120)
        if r.status_code == 429:
            print(f"429 on download, sleep {delay}s (attempt {attempt+1}/8)")
            time.sleep(delay)
            delay = min(delay + 15, 120)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("download_report: exceeded retry limit")


# ---------- Supabase ----------

def supabase_client() -> Client:
    return create_client(SB_URL, SB_KEY)


def normalize_row(r: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    """
    Приводим ключи WB к snake_case колонкам таблицы wb_paid_storage_x.
    Поля берём по спецификации WB.
    """
    return {
        "task_id":           task_id,
        "date":              r.get("date"),                # str (YYYY-MM-DD)
        "log_warehouse_coef":r.get("logWarehouseCoef"),
        "office_id":         r.get("officeId"),
        "warehouse":         r.get("warehouse"),
        "warehouse_coef":    r.get("warehouseCoef"),
        "gi_id":             r.get("giId"),
        "chrt_id":           r.get("chrtId"),
        "size":              r.get("size"),
        "barcode":           r.get("barcode"),
        "subject":           r.get("subject"),
        "brand":             r.get("brand"),
        "vendor_code":       r.get("vendorCode"),
        "nm_id":             r.get("nmId"),
        "volume":            r.get("volume"),
        "calc_type":         r.get("calcType"),
        "warehouse_price":   r.get("warehousePrice"),
        "barcodes_count":    r.get("barcodesCount"),
        "pallet_place_code": r.get("palletPlaceCode"),
        "pallet_count":      r.get("palletCount"),
        "original_date":     r.get("originalDate"),
        "loyalty_discount":  r.get("loyaltyDiscount"),
        "tariff_fix_date":   r.get("tariffFixDate"),
        "tariff_lower_date": r.get("tariffLowerDate"),
    }


def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    sb = supabase_client()
    # Важно: on_conflict должен соответствовать первичному ключу в таблице
    sb.table(TABLE_NAME).upsert(
        rows,
        on_conflict=ON_CONFLICT,
        ignore_duplicates=False
    ).execute()


# ---------- Основной флоу по окну ----------

def fetch_window(d_from: dt.date, d_to: dt.date):
    task_id = create_task(d_from, d_to)
    time.sleep(2)  # небольшой лаг перед опросом статуса
    wait_done(task_id)
    data = download_report(task_id)
    print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
    rows = [normalize_row(r, task_id) for r in data]
    upsert(rows)
    # Пауза под лимит download: 1/мин (иначе может прилететь 429 на следующее окно)
    time.sleep(POST_DOWNLOAD_SLEEP)


# ---------- Режимы ----------

def mode_range(date_from: str, date_to: str):
    a = parse_date(date_from)
    b = parse_date(date_to)
    for df, dt_ in chunks_8days(a, b):
        print(f"[RANGE] {df}..{dt_}")
        for attempt in range(3):
            try:
                fetch_window(df, dt_)
                break
            except Exception as e:
                print("Retry:", e)
                time.sleep(30)


def mode_backfill(year: int):
    a = dt.date(year, 1, 1)
    b = dt.date(year, 12, 31)
    for df, dt_ in chunks_8days(a, b):
        print(f"[BACKFILL] {df}..{dt_}")
        for attempt in range(3):
            try:
                fetch_window(df, dt_)
                break
            except Exception as e:
                print("Retry:", e)
                time.sleep(30)


def mode_since(date_from: str):
    a = parse_date(date_from)
    b = today_local()
    mode_range(a.isoformat(), b.isoformat())


def mode_sync(days: int):
    # последние N дней включительно (сегодня тоже)
    end = today_local()
    start = end - dt.timedelta(days=days - 1)
    print(f"[SYNC] {start}..{end}")
    for df, dt_ in chunks_8days(start, end):
        for attempt in range(3):
            try:
                fetch_window(df, dt_)
                break
            except Exception as e:
                print("Retry:", e)
                time.sleep(30)


# ---------- Entry point ----------

if __name__ == "__main__":
    # для отладки в Actions можно печатать длину токена (сам токен маскируется)
    # print(f"WB token length: {len(WB_TOKEN)}")

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python wb_paid_storage_sync.py backfill <YEAR>")
        print("  python wb_paid_storage_sync.py range <YYYY-MM-DD> <YYYY-MM-DD>")
        print("  python wb_paid_storage_sync.py since <YYYY-MM-DD>")
        print("  python wb_paid_storage_sync.py sync <N_days>")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "backfill":
        year = int(sys.argv[2])
        mode_backfill(year)
    elif mode == "range":
        d1, d2 = sys.argv[2], sys.argv[3]
        mode_range(d1, d2)
    elif mode == "since":
        d1 = sys.argv[2]
        mode_since(d1)
    elif mode == "sync":
        ndays = int(sys.argv[2])
        mode_sync(ndays)
    else:
        raise SystemExit(f"Unknown mode: {mode}")
