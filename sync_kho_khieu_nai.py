"""Đẩy bảng Đơn khiếu nại từ Lark Base sang Google Sheet.

Chạy: python "lark_don_khieu_nai_to_sheet.py"
Cấu hình Lark đọc từ .env.base.kho (cùng thư mục).
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

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env.base.kho")
LARK_HOST = "https://open.larksuite.com"

GCP_KEY_PATH = os.getenv("GCP_KEY_PATH", r"E:\RYSH MAN\ursusneit-4de5c66ae2f8.json")
SPREADSHEET_ID = "1Sxk10_TFJG9wn8oNvq02QiacIE0M9XU2b9yryyeR43I"
WORKSHEET_NAME = "Sheet4"

# Để trống = lấy toàn bộ bảng (1.897 dòng), bỏ qua filter/sort của view.
# Đặt "vewmP8G2E4" nếu muốn bám view DATA — view đó lọc `Ngày tiếp nhận is
# CurrentMonth` nên chỉ ra dữ liệu tháng hiện tại.
DEFAULT_VIEW_ID = ""

# Bảng Lark có rất nhiều record trắng (chỉ tạo dòng, chưa nhập gì). Bật cờ này
# để bỏ qua chúng thay vì đẩy dòng trống sang sheet.
SKIP_EMPTY_ROWS = True

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Cột xuất ra sheet -> các tên field có thể có bên Lark Base.
# Nhiều ứng viên để chịu được khác biệt chính tả (xử lý / xử lí, SĐT / SĐT nhận hàng...).
COLUMN_MAP: list[tuple[str, list[str]]] = [
    ("Ngày tiếp nhận", ["Ngày tiếp nhận"]),
    ("Kênh", ["Kênh"]),
    ("Mã đơn hàng", ["Mã đơn hàng"]),
    ("Khách đặt", ["Khách đặt"]),
    ("Khách nhận", ["Khách nhận"]),
    ("Lỗi do kho", ["Lỗi do kho"]),
    # Bảng có 2 field gần trùng tên: 'Phương án xử lí' (dữ liệu thật) và
    # 'Phương án xử lý' (field rác, mọi dòng đều là "False"). Ưu tiên bản chữ 'í'.
    ("Phương án xử lý", ["Phương án xử lí", "Phương án xử lý"]),
    ("Tên người nhận", ["Tên người nhận"]),
    ("SĐT", ["SDT", "SĐT", "SĐT nhận hàng", "Số điện thoại"]),
    ("Địa chỉ", ["Địa chỉ", "Địa chỉ nhận hàng"]),
    ("Sản phẩm gửi", ["Sản phẩm gửi"]),
    (
        "Mã đơn Salework (Kho)",
        ["Mã đơn Salework (Kho)", "Mã đơn Salework(Kho)", "Mã đơn Salework"],
    ),
]


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


def build_rows(
    records: list[dict[str, Any]], resolved: list[tuple[str, str, int]]
) -> tuple[list[list[str]], int]:
    """Trả về (các dòng có dữ liệu, số dòng rỗng đã bỏ)."""
    # Cột checkbox luôn có giá trị TRUE/FALSE nên không dùng để xét dòng rỗng,
    # nếu không thì record trắng trong Lark vẫn bị coi là có dữ liệu.
    meaningful = [i for i, (_, _, ftype) in enumerate(resolved) if ftype != 7]

    rows: list[list[str]] = []
    skipped = 0
    for record in records:
        fields = record.get("fields", {})
        row = [
            cell_to_text(fields.get(field_name), field_type)
            for _, field_name, field_type in resolved
        ]
        if SKIP_EMPTY_ROWS and not any(row[i].strip() for i in meaningful):
            skipped += 1
            continue
        rows.append(row)
    return rows, skipped


def push_to_sheet(header: list[str], rows: list[list[str]]) -> None:
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
    # Tab mặc định chỉ 1000 dòng, không resize thì ghi 1.897 dòng sẽ vượt grid.
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
