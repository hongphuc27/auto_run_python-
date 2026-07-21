#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Đồng bộ Lark Base Booking Video sang Google Sheets trên GitHub Actions."""

import math
import os
import time
from typing import Any

import google.auth
import gspread
import pandas as pd
import requests

BASE_URL = "https://open.larksuite.com/open-apis"

GG_SHEET_ID = os.getenv(
    "GG_SHEET_ID",
    "1H0Y_Y7EbuBzL2I2ifwVS86syrZUg-lp51WbYzsvt5Wo",
)
GG_SHEET_NAME = os.getenv("GG_SHEET_NAME", "fact_booking_video")

SELECTED_FIELDS = [
    "record_id",
    "CODE",
    "Ngày Book",
    "Tháng Book",
    "Năm Book",
    "Nhân Sự",
    "Chi phí",
    "Tên Kênh_KOC",
    "ID Kênh",
    "ID VIDEO",
    "Mã nhân viên",
]

REQUEST_TIMEOUT = 60
MAX_RETRIES = 4


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"[CONFIG] Thiếu biến môi trường/secret: {name}")
    return value


def get_lark_config() -> tuple[str, str, str, str]:
    return (
        required_env("LARK_APP_ID"),
        required_env("LARK_APP_SECRET"),
        required_env("LARK_APP_TOKEN"),
        required_env("LARK_TABLE_ID"),
    )


def request_json(method: str, url: str, **kwargs: Any) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(
                method,
                url,
                timeout=REQUEST_TIMEOUT,
                **kwargs,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"Lark API lỗi: {data}")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            wait_seconds = attempt * 2
            print(f"  Retry {attempt}/{MAX_RETRIES - 1} sau {wait_seconds}s: {exc}", flush=True)
            time.sleep(wait_seconds)
    raise RuntimeError(f"Không gọi được Lark API sau {MAX_RETRIES} lần: {last_error}")


def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    data = request_json(
        "POST",
        f"{BASE_URL}/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
    )
    token = data.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"Lark không trả tenant_access_token: {data}")
    return token


def list_larkbase_records(token: str, app_token: str, table_id: str) -> list[dict]:
    all_records: list[dict] = []
    page_token: str | None = None

    while True:
        params: dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        data = request_json(
            "GET",
            f"{BASE_URL}/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )

        result = data.get("data") or {}
        items = result.get("items") or []
        all_records.extend(items)
        print(f"  Fetched: {len(items)} | Total: {len(all_records)}", flush=True)

        if not result.get("has_more"):
            break

        page_token = result.get("page_token")
        if not page_token:
            raise RuntimeError("Lark báo has_more=true nhưng không trả page_token")
        time.sleep(0.2)

    return all_records


def normalize_value(value: Any) -> Any:
    if value is None:
        return ""

    if isinstance(value, float) and math.isnan(value):
        return ""

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list):
        normalized_items: list[str] = []
        for item in value:
            if isinstance(item, dict):
                normalized_items.append(
                    str(
                        item.get("text")
                        or item.get("name")
                        or item.get("url")
                        or item.get("file_token")
                        or item
                    )
                )
            else:
                normalized_items.append(str(item))
        return ", ".join(normalized_items)

    if isinstance(value, dict):
        users = value.get("users")
        if isinstance(users, list):
            return ", ".join(
                str(user.get("name", ""))
                for user in users
                if isinstance(user, dict) and user.get("name")
            )

        if "value" in value:
            return normalize_value(value["value"])

        return value.get("text") or value.get("name") or value.get("url") or str(value)

    return str(value)


def normalize_records(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for record in records:
        row = {"record_id": record.get("record_id", "")}
        for field_name, value in (record.get("fields") or {}).items():
            row[field_name] = normalize_value(value)
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=SELECTED_FIELDS)
    return pd.DataFrame(rows)


def map_ma_nhan_vien(nhan_su_val: Any) -> str:
    ns = str(nhan_su_val).strip().lower()
    if "nguyễn thị linh chi" in ns or ns == "linh chi":
        return "RM96"
    if "trần thị mai phương" in ns or "phương trần" in ns:
        return "RM135"
    if "phan thị mai hương" in ns:
        return "RM194"
    if "trần quang huy" in ns:
        return "RM06"
    return ""


def normalize_book_date(value: Any) -> str:
    if value in (None, "", "nan"):
        return ""

    try:
        number = float(value)
        if number == 0 or math.isnan(number):
            return ""
        unit = "ms" if abs(number) >= 10_000_000_000 else "s"
        return pd.Timestamp(number, unit=unit, tz="Asia/Ho_Chi_Minh").strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        text = str(value).strip()
        return "" if text.lower() == "nan" else text


def prepare_dataframe(records: list[dict]) -> pd.DataFrame:
    df = normalize_records(records)

    if "Nhân Sự" not in df.columns:
        df["Nhân Sự"] = ""
    df["Mã nhân viên"] = df["Nhân Sự"].apply(map_ma_nhan_vien)

    missing_from_lark = [
        column
        for column in SELECTED_FIELDS
        if column not in df.columns and column not in ("Mã nhân viên",)
    ]
    if missing_from_lark:
        print(f"[WARN] Lark không trả các cột: {missing_from_lark}", flush=True)

    df = df.reindex(columns=SELECTED_FIELDS, fill_value="")
    df["record_id"] = df["record_id"].fillna("").astype(str).str.strip()
    df = df[df["record_id"] != ""].drop_duplicates(subset=["record_id"], keep="last")

    df["Ngày Book"] = df["Ngày Book"].apply(normalize_book_date)
    df["Chi phí"] = pd.to_numeric(df["Chi phí"], errors="coerce").fillna(0)

    other_cols = [column for column in df.columns if column not in ("Chi phí", "Ngày Book")]
    df[other_cols] = df[other_cols].fillna("").replace("nan", "")
    return df


def get_gsheet_worksheet() -> gspread.Worksheet:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials, _ = google.auth.default(scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(GG_SHEET_ID)
    return spreadsheet.worksheet(GG_SHEET_NAME)


def chunks(items: list[Any], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def upsert_to_gsheet(df: pd.DataFrame) -> None:
    print("\nKết nối Google Sheets...", flush=True)
    worksheet = get_gsheet_worksheet()
    existing = worksheet.get_all_values()

    if existing:
        header = existing[0]
        data_rows = existing[1:]
    else:
        header = []
        data_rows = []

    if header != SELECTED_FIELDS:
        print("  Header khác cấu trúc chuẩn -> tạo lại sheet dữ liệu.", flush=True)
        worksheet.clear()
        worksheet.update(range_name="A1", values=[SELECTED_FIELDS], value_input_option="USER_ENTERED")
        header = SELECTED_FIELDS
        data_rows = []

    rid_col = header.index("record_id")
    existing_map: dict[str, tuple[int, list[str]]] = {}
    for row_number, row in enumerate(data_rows, start=2):
        padded = row + [""] * (len(SELECTED_FIELDS) - len(row))
        record_id = padded[rid_col].strip()
        if record_id:
            existing_map[record_id] = (row_number, padded[: len(SELECTED_FIELDS)])

    new_rids = set(df["record_id"].astype(str))
    append_rows: list[list[str]] = []
    update_ranges: list[dict] = []

    last_column = gspread.utils.rowcol_to_a1(1, len(SELECTED_FIELDS)).rstrip("1")

    for _, record in df.iterrows():
        values = [str(record.get(column, "")) for column in SELECTED_FIELDS]
        record_id = values[0]
        current = existing_map.get(record_id)

        if current is None:
            append_rows.append(values)
            continue

        row_number, old_values = current
        if values != old_values:
            update_ranges.append(
                {
                    "range": f"A{row_number}:{last_column}{row_number}",
                    "values": [values],
                }
            )

    rows_to_delete = sorted(
        [row_number for rid, (row_number, _) in existing_map.items() if rid not in new_rids],
        reverse=True,
    )

    for batch in chunks(update_ranges, 300):
        worksheet.batch_update(batch, value_input_option="USER_ENTERED")

    for batch in chunks(append_rows, 1000):
        worksheet.append_rows(batch, value_input_option="USER_ENTERED")

    for row_number in rows_to_delete:
        worksheet.delete_rows(row_number)

    print(
        f"Hoàn tất! Updated: {len(update_ranges)} | "
        f"Appended: {len(append_rows)} | Deleted: {len(rows_to_delete)}",
        flush=True,
    )


def main() -> None:
    app_id, app_secret, app_token, table_id = get_lark_config()

    print("Lấy token Lark Base...", flush=True)
    token = get_tenant_access_token(app_id, app_secret)

    print("Kéo dữ liệu Booking Video từ Lark Base...", flush=True)
    records = list_larkbase_records(token, app_token, table_id)

    if not records and os.getenv("ALLOW_EMPTY_SYNC", "false").lower() != "true":
        raise RuntimeError(
            "Lark trả 0 bản ghi. Dừng để tránh xóa toàn bộ Google Sheet. "
            "Chỉ đặt ALLOW_EMPTY_SYNC=true khi chắc chắn Base thực sự trống."
        )

    df = prepare_dataframe(records)
    print(f"Tổng dữ liệu hợp lệ: {len(df)} dòng | {len(df.columns)} cột", flush=True)
    upsert_to_gsheet(df)


if __name__ == "__main__":
    main()
