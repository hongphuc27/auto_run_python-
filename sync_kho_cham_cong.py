"""Đẩy bảng Kiểm tra công từ Lark Base sang Google Sheet.

Chạy: py "lark_kiem_tra_cong_to_sheet.py"
Cấu hình Lark đọc từ .env.base.kho2 (cùng thư mục).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env.base.kho2")
LARK_HOST = "https://open.larksuite.com"

# Trên GitHub Actions: workflow ghi secret ra file rồi set env GCP_KEY_PATH.
# Chạy local: giữ nguyên đường dẫn máy anh làm mặc định.
GCP_KEY_PATH = os.getenv("GCP_KEY_PATH", r"E:\RYSH MAN\ursusneit-4de5c66ae2f8.json")
SPREADSHEET_ID = "1Sxk10_TFJG9wn8oNvq02QiacIE0M9XU2b9yryyeR43I"
WORKSHEET_NAME = "Sheet5"  # gid=1829108793

# Để trống = lấy toàn bộ bảng, bỏ qua filter/sort của view.
DEFAULT_VIEW_ID = ""

# Bảng Lark hay có record trắng (tạo dòng nhưng chưa nhập gì) -> bỏ qua.
SKIP_EMPTY_ROWS = True

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Cột xuất ra sheet -> các tên field có thể có bên Lark Base.
COLUMN_MAP: list[tuple[str, list[str]]] = [
    ("Ngày", ["Ngày"]),
    ("Tháng", ["Tháng"]),
    ("Tuần", ["Tuần"]),
    ("Thứ", ["Thứ"]),
    ("Nhân viên", ["Nhân viên"]),
    ("Vai trò", ["Vai trò"]),
    ("Trạng thái", ["Trạng thái"]),
    ("Công", ["Công"]),
    ("Công đóng gói (*)", ["Công đóng gói (*)", "Công đóng gói"]),
    ("Ghi chú", ["Ghi chú"]),
]

# Ghi sang sheet dưới dạng số thay vì chuỗi, để còn SUM được.
NUMERIC_COLUMNS = {"Công", "Công đóng gói (*)"}

# Xét dòng rỗng dựa trên các cột này (Công/Công đóng gói luôn có 0 nên không tính).
KEY_COLUMNS = {"Ngày", "Nhân viên", "Trạng thái"}


def load_lark_config() -> tuple[str, str, str, str, str]:
    load_dotenv(ENV_FILE)
    missing = [
        key
        for key in (
            "LARK_APP_ID",
            "LARK_APP_SECRET",
            "LARK_BASE_APP_TOKEN",
            "LARK_BASE_TOKEN_ID",
        )
        if not os.getenv(key, "").strip()
    ]
    if missing:
        raise RuntimeError(f"Thiếu biến trong {ENV_FILE}: {', '.join(missing)}")
    return (
        os.environ["LARK_APP_ID"].strip(),
        os.environ["LARK_APP_SECRET"].strip(),
        os.environ["LARK_BASE_APP_TOKEN"].strip(),
        os.environ["LARK_BASE_TOKEN_ID"].strip(),
        os.getenv("LARK_BASE_VIEW_ID", DEFAULT_VIEW_ID).strip(),
    )


def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    resp = requests.post(
        f"{LARK_HOST}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") != 0:
        raise RuntimeError(
            f"Lấy tenant_access_token thất bại: code={body.get('code')} "
            f"msg={body.get('msg')}"
        )
    return body["tenant_access_token"]


def fetch_fields(token: str, app_token: str, table_id: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(
            f"{LARK_HOST}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(
                f"Đọc field thất bại: code={body.get('code')} msg={body.get('msg')}"
            )
        data = body.get("data", {})
        fields.extend(data.get("items", []))
        if not data.get("has_more"):
            return fields
        page_token = data.get("page_token")


def fetch_records(
    token: str, app_token: str, table_id: str, view_id: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        payload: dict[str, Any] = {"automatic_fields": False}
        if view_id:
            payload["view_id"] = view_id
        resp = requests.post(
            f"{LARK_HOST}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}"
            "/records/search",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(
                f"Đọc record thất bại: code={body.get('code')} msg={body.get('msg')}"
            )
        data = body.get("data", {})
        records.extend(data.get("items", []))
        print(f"  đã tải {len(records)} record...")
        if not data.get("has_more"):
            return records
        page_token = data.get("page_token")


def normalize(name: str) -> str:
    return " ".join(name.split()).casefold()


def resolve_columns(fields: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
    """Khớp cột cần xuất với field thật trên Lark. Trả về (tên cột, tên field, type)."""
    by_norm = {normalize(f["field_name"]): f for f in fields}
    resolved: list[tuple[str, str, int]] = []
    unresolved: list[str] = []

    for column, candidates in COLUMN_MAP:
        matched = None
        for candidate in candidates:
            matched = by_norm.get(normalize(candidate))
            if matched:
                break
        if matched:
            resolved.append((column, matched["field_name"], matched.get("type", 1)))
        else:
            unresolved.append(column)

    if unresolved:
        available = "\n".join(f"  - {f['field_name']}" for f in fields)
        raise RuntimeError(
            "Không tìm thấy field trên Lark cho các cột: "
            + ", ".join(unresolved)
            + f"\nCác field hiện có trong bảng:\n{available}"
        )
    return resolved


def cell_to_text(value: Any, field_type: int) -> str:
    """Đổi giá trị ô Lark (nhiều kiểu khác nhau) thành chuỗi cho Google Sheet."""
    # Checkbox: Lark trả None khi không tick, nên phải xử lý trước nhánh None ở dưới.
    if field_type == 7:
        return "TRUE" if value is True else "FALSE"

    if value is None or value == "":
        return ""

    # Field công thức / lookup (type 19, 20) bọc dữ liệu trong {"type": .., "value": ..}
    # với type bên trong mới là kiểu thật (1=text, 2=number, 3=option, 5=date).
    if isinstance(value, dict) and "value" in value and "type" in value:
        return cell_to_text(value["value"], value["type"])

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    # Ngày tháng: Lark trả timestamp mili-giây
    if field_type in (5, 1001, 1002) and isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, VN_TZ).strftime("%d/%m/%Y")

    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, list):
        parts = [cell_to_text(item, field_type) for item in value]
        return ", ".join(part for part in parts if part)

    if isinstance(value, dict):
        # text segment / link / người dùng / file đính kèm
        for key in ("text", "name", "en_name", "file_token"):
            if value.get(key):
                return str(value[key]).strip()
        if value.get("link"):
            return str(value["link"]).strip()
        return ""

    return str(value).strip()


def to_number(text: str) -> Any:
    """Cột công cần ghi dạng số để SUM được; không parse được thì giữ nguyên chuỗi."""
    if not text:
        return ""
    try:
        number = float(text.replace(",", "."))
    except ValueError:
        return text
    return int(number) if number.is_integer() else number


def build_rows(
    records: list[dict[str, Any]], resolved: list[tuple[str, str, int]]
) -> tuple[list[list[Any]], int]:
    """Trả về (các dòng có dữ liệu, số dòng rỗng đã bỏ)."""
    key_idx = [i for i, (col, _, _) in enumerate(resolved) if col in KEY_COLUMNS]
    numeric_idx = {i for i, (col, _, _) in enumerate(resolved) if col in NUMERIC_COLUMNS}

    rows: list[list[Any]] = []
    skipped = 0
    for record in records:
        fields = record.get("fields", {})
        texts = [
            cell_to_text(fields.get(field_name), field_type)
            for _, field_name, field_type in resolved
        ]
        if SKIP_EMPTY_ROWS and not any(texts[i].strip() for i in key_idx):
            skipped += 1
            continue
        rows.append(
            [to_number(t) if i in numeric_idx else t for i, t in enumerate(texts)]
        )
    return rows, skipped


def push_to_sheet(header: list[str], rows: list[list[Any]]) -> None:
    credentials = service_account.Credentials.from_service_account_file(
        GCP_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    spreadsheet = gspread.authorize(credentials).open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        raise RuntimeError(
            f"Không tìm thấy tab {WORKSHEET_NAME!r} trong sheet. "
            f"Các tab hiện có: {[ws.title for ws in spreadsheet.worksheets()]}"
        ) from None

    worksheet.clear()
    # Tab mặc định chỉ 1000 dòng, không resize thì ghi nhiều dòng sẽ vượt grid.
    worksheet.resize(rows=max(len(rows) + 1, 2), cols=max(len(header), 1))
    worksheet.update([header] + rows, "A1", value_input_option="RAW")
    worksheet.freeze(rows=1)
    print(f"Đã ghi {len(rows)} dòng vào tab {WORKSHEET_NAME!r}.")


def main() -> None:
    app_id, app_secret, app_token, table_id, view_id = load_lark_config()

    print("Đăng nhập Lark...")
    token = get_tenant_access_token(app_id, app_secret)

    print("Đọc cấu trúc bảng...")
    fields = fetch_fields(token, app_token, table_id)
    resolved = resolve_columns(fields)

    print(f"Tải record (view_id={view_id or 'toàn bộ bảng'})...")
    records = fetch_records(token, app_token, table_id, view_id)

    header = [column for column, _, _ in resolved]
    rows, skipped = build_rows(records, resolved)
    if skipped:
        print(f"Bỏ qua {skipped} record trắng trên Lark.")
    print(f"Tổng {len(rows)} dòng, {len(header)} cột.")

    push_to_sheet(header, rows)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"LỖI: {exc}", file=sys.stderr)
        sys.exit(1)
