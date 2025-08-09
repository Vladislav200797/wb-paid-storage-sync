#!/usr/bin/env python3
import os
import sys
import time
import math
import json
import datetime as dt
from typing import List, Dict, Any, Iterable

import requests
from supabase import create_client, Client

# === Конфиг через переменные окружения ===
WB_BASE   = os.getenv("WB_API_BASE", "https://seller-analytics-api.wildberries.ru")
WB_TOKEN  = os.environ["WB_API_TOKEN"]
SB_URL    = os.environ["SUPABASE_URL"]
SB_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TABLE     = os.getenv("WB_TABLE", "wb_paid_storage")

# Ограничения WB: отчёт максимум за 8 дней (включительно)
WB_MAX_WINDOW_DAYS = 8

# Глобальный фолбэк по виду заголовка Authorization:
# сначала пробуем без Bearer (как у тебя в PowerShell), если получаем 401 — переключаемся на Bearer
_AUTH_STYLE = None  # None | "plain" | "bearer"


# === Вспомогалки ===
def chunks_8days(d_from: dt.date, d_to: dt.date) -> Iterable[tuple[dt.date, dt.date]]:
    """Режем произвольный интервал на окна по ≤8 дней включительно (формат WB)."""
    cur = d_from
    one_day = dt.timedelta(days=1)
    while cur <= d_to:
        end = min(cur + dt.timedelta(days=WB_MAX_WINDOW_DAYS - 1), d_to)
        yield cur, end
        cur = end + one_day


def _headers(auth_style: str) -> Dict[str, str]:
    if auth_style == "bearer":
        return {"Authorization": f"Bearer {WB_TOKEN}"}
    return {"Authorization": WB_TOKEN}


def _get(url: str, params: Dict[str, Any] | None = None, timeout: int = 60) -> requests.Response:
    """GET с автоподбором стиля Authorization (plain→bearer при 401)."""
    global _AUTH_STYLE
    tried = []

    styles = [_AUTH_STYLE] if _AUTH_STYLE else ["plain", "bearer"]
    # plain сначала, потому что именно так у тебя работал PowerShell
    if styles == ["bearer"]:
        styles = ["bearer", "plain"]  # если уже зафиксирован bearer — пробуем его первым

    for style in styles:
        tried.append(style)
        r = requests.get(url, headers=_headers(style), params=params, timeout=timeout)
        if r.status_code == 401 and _AUTH_STYLE is None:
            # переключимся и попробуем второй стиль
            continue
        # запомним рабочий стиль
        if 200 <= r.status_code < 300 and _AUTH_STYLE is None:
            _AUTH_STYLE = style
        r.raise_for_status()
        return r

    # сюда придём только если был 401 в первом стиле и ошибка/401 во втором
    # сделаем последнюю попытку вторым стилем, чтобы поднять корректную ошибку
    fallback = "bearer" if tried and tried[0] == "plain" else "plain"
    r = requests.get(url, headers=_headers(fallback), params=params, timeout=timeout)
    if 200 <= r.status_code < 300 and _AUTH_STYLE is None:
        _AUTH_STYLE = fallback
    r.raise_for_status()
    return r


# === API Wildberries ===
def create_task(d_from: dt.date, d_to: dt.date) -> str:
    # Создать задачу — GET /api/v1/paid_storage?dateFrom=...&dateTo=...
    r = _get(
        f"{WB_BASE}/api/v1/paid_storage",
        params={"dateFrom": d_from.isoformat(), "dateTo": d_to.isoformat()},
        timeout=60,
    )
    js = r.json()
    return js["data"]["taskId"]


def task_status(task_id: str) -> str:
    r = _get(
        f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/status",
        timeout=60,
    )
    js = r.json()
    return js["data"]["status"]


def download_report(task_id: str) -> List[Dict[str, Any]]:
    r = _get(
        f"{WB_BASE}/api/v1/paid_storage/tasks/{task_id}/download",
        timeout=120,
    )
    return r.json()  # массив объектов


def wait_task(task_id: str, max_wait_sec: int = 600, poll_sec: int = 8) -> None:
    """Ждём статуса done до max_wait_sec. Бросаем исключение при timeout или ошибке."""
    start = time.time()
    while True:
        st = task_status(task_id)
        elapsed = int(time.time() - start)
        if st == "done":
            return
        if st in {"canceled", "error", "failed"}:
            raise RuntimeError(f"WB task {task_id} failed with status={st}")
        if elapsed >= max_wait_sec:
            raise TimeoutError(f"WB task {task_id} timeout")
        # красивые логи только раз в ~минуту (чтобы не засорять)
        if elapsed % 60 in (0, 1):
            print(f"waiting WB task {task_id}, status={st}, elapsed={elapsed}s")
        time.sleep(poll_sec)


# === Supabase ===
def supabase_client() -> Client:
    return create_client(SB_URL, SB_KEY)


def map_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводим ключи и типы под схему:
    PK: (date, nm_id, chrt_id, office_id)
    """
    def to_float(x):
        if x is None or x == "":
            return None
        return float(x)

    def to_int(x):
        if x is None or x == "":
            return None
        return int(x)

    out = {
        "date": raw.get("date"),  # YYYY-MM-DD
        "log_warehouse_coef": to_float(raw.get("logWarehouseCoef")),
        "office_id": to_int(raw.get("officeId")),
        "warehouse": raw.get("warehouse"),
        "warehouse_coef": to_float(raw.get("warehouseCoef")),
        "gi_id": to_int(raw.get("giId")),
        "chrt_id": to_int(raw.get("chrtId")),
        "size": raw.get("size"),
        "barcode": raw.get("barcode"),
        "subject": raw.get("subject"),
        "brand": raw.get("brand"),
        "vendor_code": raw.get("vendorCode"),
        "nm_id": to_int(raw.get("nmId")),
        "volume": to_float(raw.get("volume")),
        "calc_type": raw.get("calcType"),
        "warehouse_price": to_float(raw.get("warehousePrice")),
        "barcodes_count": to_int(raw.get("barcodesCount")),
        "pallet_place_code": to_int(raw.get("palletPlaceCode")),
        "pallet_count": to_int(raw.get("palletCount")),
        "original_date": raw.get("originalDate"),
        "loyalty_discount": to_float(raw.get("loyaltyDiscount")),
        "tariff_fix_date": raw.get("tariffFixDate"),
        "tariff_lower_date": raw.get("tariffLowerDate"),
    }
    return out


def upsert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sb = supabase_client()
    # батчами по 1000 — так надёжнее
    BATCH = 1000
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        # on_conflict должен соответствовать PK, чтобы не было дублей
        res = sb.table(TABLE).insert(batch, upsert=True, on_conflict="date,nm_id,chrt_id,office_id").execute()
        # не бросаем, но логируем размер
    # можно вывести общий счётчик
    print(f"rows upserted: {len(rows)}")


# === Основная логика ===
def fetch_window(d_from: dt.date, d_to: dt.date) -> None:
    for attempt in range(1, 4):
        try:
            task_id = create_task(d_from, d_to)
            wait_task(task_id, max_wait_sec=600, poll_sec=8)
            data = download_report(task_id)
            print(f"rows downloaded: {len(data)} for {d_from}..{d_to}")
            rows = [map_row(r) for r in data]
            upsert_rows(rows)
            return
        except requests.HTTPError as e:
            # 429/5xx/… — подождём и повторим
            print(f"Retry ({attempt}) HTTP: {e}")
            time.sleep(30 * attempt)
        except TimeoutError as e:
            print(f"Retry ({attempt}) {e}")
            time.sleep(30 * attempt)
        except Exception as e:
            print(f"Retry ({attempt}) {e}")
            time.sleep(15 * attempt)
    # если все попытки сгорели — бросаем
    raise RuntimeError(f"Failed to fetch {d_from}..{d_to} after retries")


def mode_range(date_from: str, date_to: str) -> None:
    a = dt.date.fromisoformat(date_from)
    b = dt.date.fromisoformat(date_to)
    print(f"[RANGE] {a}..{b}")
    for df, dt_ in chunks_8days(a, b):
        fetch_window(df, dt_)


def mode_backfill(year: str) -> None:
    y = int(year)
    a = dt.date(y, 1, 1)
    b = dt.date(y, 12, 31)
    cur = a
    while cur <= b:
        end = min(cur + dt.timedelta(days=WB_MAX_WINDOW_DAYS - 1), b)
        print(f"[BACKFILL] {cur}..{end}")
        fetch_window(cur, end)
        cur = end + dt.timedelta(days=1)


def mode_since(date_from: str) -> None:
    a = dt.date.fromisoformat(date_from)
    b = dt.date.today()
    mode_range(a.isoformat(), b.isoformat())


def mode_sync(days_back: int = 8) -> None:
    # ежедневно гоняем последние N дней (по умолчанию 8, чтобы точно помещалось в один запрос)
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back - 1)
    # WB включает обе границы -> 8 дней = [today-7 .. today]
    print(f"[SYNC] {start}..{today}")
    fetch_window(start, today)


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python wb_paid_storage_sync.py backfill 2024")
        print("  python wb_paid_storage_sync.py range YYYY-MM-DD YYYY-MM-DD")
        print("  python wb_paid_storage_sync.py since YYYY-MM-DD")
        print("  python wb_paid_storage_sync.py sync [DAYS_BACK]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "backfill":
        year = sys.argv[2]
        mode_backfill(year)
    elif mode == "range":
        date_from, date_to = sys.argv[2], sys.argv[3]
        mode_range(date_from, date_to)
    elif mode == "since":
        date_from = sys.argv[2]
        mode_since(date_from)
    elif mode == "sync":
        days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 8
        if days_back < 1 or days_back > WB_MAX_WINDOW_DAYS:
            # держим в разумных рамках, чтобы не выходить за лимит WB
            days_back = min(max(days_back, 1), WB_MAX_WINDOW_DAYS)
        mode_sync(days_back)
    else:
        raise SystemExit(f"Unknown mode: {mode}")


if __name__ == "__main__":
    main()
