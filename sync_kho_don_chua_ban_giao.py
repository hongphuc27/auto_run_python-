"""Đẩy danh sách ĐƠN CHƯA BÀN GIAO (đơn muộn) lên BigQuery — BẢN GỘP 1 FILE.

Trước đây file này import "nhanh_order_status_history (V2).py" để dùng chung logic.
Bản này gộp thẳng engine đó vào đây nên KHÔNG cần file phụ thuộc nữa — chỉ 1 file chạy.

Grain: 1 dòng = 1 đơn x 1 ngày báo cáo. Khóa duy nhất (report_date, order_id).
Chỉ ghi các ngày ĐÃ ĐÓNG: ngày báo cáo mới nhất là HÔM QUA, không ghi hôm nay.

Định nghĩa "chưa bàn giao" trong 1 ngày báo cáo:
  - Đơn thuộc một trong các nhóm PHẢI bàn giao của ngày đó:
        * "Đơn phải bàn giao sáng hôm nay"      (morning_required)
        * "Đơn mới trong ngày"                    (new_today)
        * "Hàng tồn đọng từ 2 đến 5 ngày trước"   (backlog_from_two_days)
  - VÀ trong ngày đó KHÔNG có log bàn giao
  - VÀ KHÔNG bị hủy trong ngày

Chạy:
    py sync_kho_don_chua_ban_giao.py               # 3 ngày đã đóng gần nhất -> BigQuery
    py sync_kho_don_chua_ban_giao.py --days 7      # 7 ngày đã đóng gần nhất
    py sync_kho_don_chua_ban_giao.py --dry-run     # chỉ xuất file local, KHÔNG đụng BigQuery
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Credential fix cứng để khỏi cần file .env khi chạy trên GitHub Actions.
# setdefault: nếu môi trường đã set (vd chạy local có .env riêng) thì vẫn ưu tiên cái đó.
# LƯU Ý: accessToken nằm thẳng trong code -> để repo PRIVATE. Token lộ thì cấp lại trên Nhanh.
# ---------------------------------------------------------------------------
os.environ.setdefault("AppID", "77571")
os.environ.setdefault(
    "accessToken",
    "Td5xd7XZOD3Nq7eEKekAE5tsdRBn2h6ZFpotZyTPHIXUBuKySfjKhrgQFqXkKZ8ItXnYWLeuKSblbIGEqIhpXPo0JAftYcFwJghL4uNoMuNawRYJIixGMx1wt6BXSnR2KsEf5tRZd4MZ",
)
os.environ.setdefault("BusinessID", "224108")
os.environ.setdefault("BIGQUERY_PROJECT_ID", "rhysman-data-warehouse-488306")
os.environ.setdefault("BIGQUERY_DATASET_ID", "rhysman")

# ===========================================================================
# ENGINE dùng chung (gộp từ nhanh_order_status_history V2)
# ===========================================================================
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
API_BASE_URL = "https://pos.open.nhanh.vn/v3.0"
DEFAULT_DEPOT_NAME = "Kho đóng hàng RM"

ORDER_STATUSES = {
    40: "Đã đóng gói",
    42: "Đang đóng gói",
    43: "Chờ thu gom",
    54: "Đơn mới",
    55: "Đang xác nhận",
    56: "Đã xác nhận",
    57: "Chờ khách xác nhận",
    58: "Hãng vận chuyển hủy đơn",
    59: "Đang chuyển",
    60: "Thành công",
    61: "Thất bại",
    63: "Khách hủy",
    64: "Hệ thống hủy",
    68: "Hết hàng",
    71: "Đang chuyển hoàn",
    72: "Đã chuyển hoàn",
    73: "Đổi kho xuất hàng",
    74: "Xác nhận hoàn",
}

SALE_CHANNELS = {
    1: "Admin",
    2: "Website",
    10: "API",
    20: "Facebook",
    21: "Instagram",
    41: "Lazada",
    42: "Shopee",
    43: "Sendo",
    45: "Tiki",
    48: "Tiktok Shop",
    49: "Zalo OA",
    50: "Shopee chat",
    51: "Lazada chat",
    52: "Zalo cá nhân",
}

# Status 61 intentionally belongs to both groups, per the requested allocation.
STATUS_GROUPS = {
    "Chờ xác nhận": {54, 55, 57, 73},
    "Chờ lấy hàng": {40, 42, 43, 56},
    "Đang giao": {59},
    "Đã hủy": {61, 63, 64},
    "Trả hàng": {61, 71, 74},
}

HANDOVER_STATUS_ID = 59
DELIVERED_STATUS_ID = 60
CANCEL_STATUS_IDS = {61, 63, 64}
PROCESSED_STATUS_IDS = {59, 60, 71, 72, 74}
POST_DELIVERY_STATUS_IDS = {60, 71, 72, 74}


def handover_status_ids_for(events: list[dict[str, Any]]) -> set[Any]:
    """Bộ trạng thái coi là 'đã bàn giao' cho một đơn, xét theo lịch sử của đơn.

    Lớp lọc 2 bước, KHÔNG phụ thuộc tên HVC:
    - Nếu đơn CÓ đi qua 59 (Đang chuyển): dùng {59} như logic gốc.
    - Nếu đơn KHÔNG có 59 nhưng CÓ 60 (Thành công): đã giao tới khách nghĩa là
      chắc chắn đã được bàn giao, chỉ là luồng này bỏ qua bước 59 (vd shop tự giao)
      -> lấy 60 làm mốc bàn giao.
    """
    if any(e.get("new_status_id") == HANDOVER_STATUS_ID for e in events):
        return {HANDOVER_STATUS_ID}
    if any(e.get("new_status_id") == DELIVERED_STATUS_ID for e in events):
        return {DELIVERED_STATUS_ID}
    return {HANDOVER_STATUS_ID}


def load_credentials() -> tuple[str, str, str]:
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
    token = os.getenv("accessToken")
    app_id = os.getenv("AppID")
    business_id = os.getenv("BusinessID", "")
    if not token or not app_id:
        raise RuntimeError("Thiếu accessToken hoặc AppID trong file .env.")
    return token, app_id, business_id


class ApiClient:
    def __init__(self, token: str) -> None:
        self.headers = {"Authorization": token, "Content-Type": "application/json"}

    def post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                with urlopen(request, timeout=45) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as error:
                last_error = error
                if error.code not in {429, 500, 502, 503, 504}:
                    details = error.read().decode("utf-8", errors="replace")
                    raise RuntimeError(f"HTTP {error.code}: {details}") from error
            except URLError as error:
                last_error = error
            if attempt < 4:
                time.sleep(2**attempt)
        raise RuntimeError(f"Không thể gọi API sau 5 lần thử: {last_error}")


def api_url(path: str, app_id: str, business_id: str) -> str:
    url = f"{API_BASE_URL}/{path}?appId={app_id}"
    if business_id:
        url += f"&businessId={business_id}"
    return url


def post_api(
    session: ApiClient,
    url: str,
    payload: dict[str, Any],
    endpoint_name: str,
    max_retries: int = 5,
) -> dict[str, Any]:
    for attempt in range(max_retries):
        data = session.post(url, payload)
        if data.get("code") == 1:
            return data

        error_msg = str(data.get("messages") or data)
        if "exceeded the API Rate Limit" in error_msg or "Too Many Requests" in error_msg:
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt
                print(f"  [Rate Limit] Bị giới hạn API, chờ {sleep_time}s rồi thử lại...")
                time.sleep(sleep_time)
                continue

        raise RuntimeError(f"API {endpoint_name} trả lỗi: {error_msg}")
    return {}


def fetch_all_pages(
    session: ApiClient,
    url: str,
    filters: dict[str, Any],
    endpoint_name: str,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_cursor: Any = ""
    seen_cursors: set[str] = set()
    page = 0

    while True:
        page += 1
        paginator: dict[str, Any] = {"size": 100}
        if next_cursor:
            paginator["next"] = next_cursor
        request_started = time.monotonic()
        data = post_api(
            session,
            url,
            {"filters": filters, "paginator": paginator},
            endpoint_name,
        )
        request_seconds = time.monotonic() - request_started
        batch = data.get("data", [])
        if isinstance(batch, dict):
            if "order/history" in endpoint_name:
                flattened = []
                for v in batch.values():
                    if isinstance(v, list):
                        flattened.extend(v)
                batch = flattened
            else:
                batch = batch.get("orders", [])
        if not batch:
            if show_progress:
                print(f"  {endpoint_name}: hết dữ liệu tại trang {page}.")
            break
        rows.extend(batch)
        new_cursor = data.get("paginator", {}).get("next", "")
        if show_progress and (page == 1 or page % 10 == 0 or not new_cursor):
            print(
                f"  {endpoint_name}: trang {page}, đã nhận {len(rows):,} dòng "
                f"(request {request_seconds:.1f}s)."
            )
        if not new_cursor:
            break
        cursor_key = json.dumps(
            new_cursor, ensure_ascii=False, sort_keys=True, default=str
        )
        previous_cursor_key = (
            json.dumps(next_cursor, ensure_ascii=False, sort_keys=True, default=str)
            if next_cursor
            else ""
        )
        if cursor_key == previous_cursor_key or cursor_key in seen_cursors:
            raise RuntimeError(
                f"API {endpoint_name} trả cursor phân trang bị lặp tại trang {page}."
            )
        seen_cursors.add(cursor_key)
        next_cursor = new_cursor

    return rows


def fetch_depot_id(
    session: ApiClient, app_id: str, business_id: str, depot_name: str
) -> int:
    data = post_api(
        session,
        api_url("business/depot", app_id, business_id),
        {"filters": {}},
        "business/depot",
    )
    depots = data.get("data", [])
    exact_matches = [
        depot for depot in depots if str(depot.get("name", "")).strip() == depot_name
    ]
    if len(exact_matches) == 1:
        return int(exact_matches[0]["id"])

    available = ", ".join(str(depot.get("name", "")) for depot in depots)
    raise RuntimeError(
        f"Không tìm thấy duy nhất kho '{depot_name}'. Các kho API trả về: {available}"
    )


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def fetch_history_for_orders(
    session: ApiClient, history_url: str, order_ids: list[int]
) -> tuple[list[dict[str, Any]], list[int]]:
    try:
        return (
            fetch_all_pages(
                session,
                history_url,
                {"orderIds": order_ids},
                "order/history",
            ),
            [],
        )
    except RuntimeError as error:
        if "Order without histories" not in str(error):
            raise
        if len(order_ids) == 1:
            return [], order_ids
        middle = len(order_ids) // 2
        left_logs, left_without_history = fetch_history_for_orders(
            session, history_url, order_ids[:middle]
        )
        right_logs, right_without_history = fetch_history_for_orders(
            session, history_url, order_ids[middle:]
        )
        return left_logs + right_logs, left_without_history + right_without_history


def timestamp_to_local_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromtimestamp(int(value), VN_TZ).replace(tzinfo=None)


def status_name(status_id: Any) -> str:
    if status_id in (None, "", 0):
        return ""
    if status_id == "HVC_CHANGE":
        return "Thay đổi mã vận đơn HVC"
    try:
        numeric_id = int(status_id)
        return ORDER_STATUSES.get(numeric_id, f"Chưa khai báo ({numeric_id})")
    except ValueError:
        return str(status_id)


def status_group(status_id: Any) -> str:
    if status_id in (None, "", 0):
        return ""
    if status_id == "HVC_CHANGE":
        return "Cập nhật HVC"
    try:
        numeric_id = int(status_id)
        groups = [name for name, ids in STATUS_GROUPS.items() if numeric_id in ids]
        return "; ".join(groups) if groups else "Chưa phân nhóm"
    except ValueError:
        return "Chưa phân nhóm"


def make_raw_order_row(
    order: dict[str, Any],
    depot_name: str,
    status_history_count: int,
    api_without_history: bool,
) -> dict[str, Any]:
    info = order.get("info") or {}
    channel = order.get("channel") or {}
    carrier = order.get("carrier") or {}
    payment = order.get("payment") or {}
    service = carrier.get("service") or {}
    current_status = info.get("status")
    sale_channel_id = channel.get("saleChannel")

    return {
        "order_id": info.get("id"),
        "mã đơn sàn": channel.get("appOrderId", ""),
        "sale_channel_id": sale_channel_id,
        "sale_channel_name": SALE_CHANNELS.get(
            sale_channel_id, f"Khác ({sale_channel_id})"
        ),
        "traffic_source": channel.get("trafficSource", ""),
        "shop_id": channel.get("shopId", ""),
        "shop_name": channel.get("shopName", ""),
        "depot_id": info.get("depotId"),
        "depot_name": depot_name,
        "carrier_id": carrier.get("id"),
        "carrier_name": carrier.get("name", ""),
        "carrier_service": service.get("name", ""),
        "carrier_code": carrier.get("carrierCode", ""),
        "tracking_code": carrier.get("fullCarrierCode")
        or carrier.get("carrierCode", ""),
        "created_at": timestamp_to_local_datetime(info.get("createdAt")),
        "updated_at": timestamp_to_local_datetime(info.get("updatedAt")),
        "confirmed_at": timestamp_to_local_datetime(info.get("confirmedAt")),
        "packed_at": timestamp_to_local_datetime(info.get("packedAt")),
        "send_carrier_at": timestamp_to_local_datetime(carrier.get("sendCarrierAt")),
        "delivery_at": timestamp_to_local_datetime(carrier.get("deliveryAt")),
        "current_status_id": current_status,
        "current_status_name": status_name(current_status),
        "current_status_group": status_group(current_status),
        "status_history_count": status_history_count,
        "status_history_result": (
            "API báo không có history"
            if api_without_history
            else (
                "Có log đổi trạng thái"
                if status_history_count
                else "Có history nhưng không có log đổi trạng thái"
            )
        ),
        "handover_ids": str(info.get("handoverIds", [])),
        "COD": payment.get("codAmount", 0),
    }


def make_history_rows(
    orders_by_id: dict[int, dict[str, Any]],
    history: list[dict[str, Any]],
    depot_name: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for log in history:
        order_id = log.get("orderId")
        order = orders_by_id.get(order_id)
        if not order:
            continue
        status = log.get("status")
        if isinstance(status, dict):
            old_status = status.get("old")
            new_status = status.get("new")
        else:
            old_status = None
            new_status = None

        log_step = log.get("step")
        is_hvc_change = (log_step in (8, 16, 26) or str(log_step) in ("8", "16", "26"))

        # Keep only actual Order Status changes or HVC changes. Order Step is deliberately ignored otherwise.
        if (new_status in (None, "", 0) or old_status == new_status) and not is_hvc_change:
            continue

        if is_hvc_change:
            new_status = "HVC_CHANGE"

        unique_key = str(
            log.get("logId")
            or f"{order_id}_{log.get('createdAt')}_{old_status}_{new_status}"
        )
        if unique_key in seen:
            continue
        seen.add(unique_key)
        info = order.get("info") or {}
        channel = order.get("channel") or {}
        current_status = info.get("status")
        rows.append(
            {
                "log_id": log.get("logId"),
                "order_id": order_id,
                "mã đơn sàn": channel.get("appOrderId", ""),
                "depot_id": info.get("depotId"),
                "depot_name": depot_name,
                "event_at": timestamp_to_local_datetime(log.get("createdAt")),
                "old_status_id": old_status or "",
                "old_status_name": status_name(old_status),
                "old_status_group": status_group(old_status),
                "new_status_id": new_status,
                "new_status_name": status_name(new_status),
                "new_status_group": status_group(new_status),
                "current_status_id": current_status,
                "current_status_name": status_name(current_status),
                "current_status_group": status_group(current_status),
                "created_by_id": log.get("createdById", ""),
                "created_by": log.get("createdBy", ""),
            }
        )
    return rows


def find_incomplete_history_orders(
    orders_by_id: dict[int, dict[str, Any]],
    raw_history: list[dict[str, Any]],
) -> list[int]:
    """Đơn mà current_status chứng tỏ đã qua bàn giao/hủy nhưng THIẾU log tương ứng."""
    handover_orders: set[int] = set()
    cancel_orders: set[int] = set()
    for row in raw_history:
        oid = int(row["order_id"])
        if row["new_status_id"] == HANDOVER_STATUS_ID:
            handover_orders.add(oid)
        elif row["new_status_id"] in CANCEL_STATUS_IDS:
            cancel_orders.add(oid)

    suspects: list[int] = []
    for oid, order in orders_by_id.items():
        status = (order.get("info") or {}).get("status")
        try:
            current = int(status)
        except (TypeError, ValueError):
            continue
        if current in PROCESSED_STATUS_IDS and oid not in handover_orders:
            suspects.append(oid)
        elif current in CANCEL_STATUS_IDS and oid not in cancel_orders:
            suspects.append(oid)
    return suspects


def repair_incomplete_history(
    session: ApiClient,
    history_url: str,
    orders_by_id: dict[int, dict[str, Any]],
    history: list[dict[str, Any]],
    depot_name: str,
    max_rounds: int = 3,
    repair_chunk_size: int = 20,
) -> list[dict[str, Any]]:
    """Vá log bị rớt: fetch lại (lô nhỏ) các đơn thiếu log rồi dựng lại raw_history."""
    raw_history = make_history_rows(orders_by_id, history, depot_name)
    suspects = find_incomplete_history_orders(orders_by_id, raw_history)
    round_no = 0
    while suspects and round_no < max_rounds:
        round_no += 1
        print(
            f"  [Vá log] vòng {round_no}: {len(suspects)} đơn thiếu log bàn giao/hủy, "
            f"fetch lại theo lô {repair_chunk_size}..."
        )
        for repair_ids in chunks(suspects, repair_chunk_size):
            repair_logs, _ = fetch_history_for_orders(session, history_url, repair_ids)
            history.extend(repair_logs)
        raw_history = make_history_rows(orders_by_id, history, depot_name)
        remaining = find_incomplete_history_orders(orders_by_id, raw_history)
        # Dừng sớm nếu fetch lại không gỡ được đơn nào nữa (API thật sự không có log).
        if len(remaining) >= len(suspects):
            suspects = remaining
            break
        suspects = remaining

    if suspects:
        print(
            f"  [Vá log] Còn {len(suspects)} đơn vẫn thiếu log sau {round_no} vòng "
            f"(API không trả log tương ứng - xem là chưa bàn giao/đặt trạng thái trực tiếp)."
        )
    elif round_no:
        print(f"  [Vá log] Đã vá xong toàn bộ sau {round_no} vòng.")
    return raw_history


def first_event_at(
    events: list[dict[str, Any]],
    status_ids: set[int],
    start_at: datetime,
    end_at: datetime,
) -> datetime | None:
    timestamps = [
        event["event_at"]
        for event in events
        if event.get("new_status_id") in status_ids
        and event.get("event_at") is not None
        and start_at <= event["event_at"] < end_at
    ]
    return min(timestamps, default=None)


def first_event_or_current_status_at(
    order: dict[str, Any],
    events: list[dict[str, Any]],
    status_ids: set[int],
    start_at: datetime,
    end_at: datetime,
) -> datetime | None:
    event_at = first_event_at(events, status_ids, start_at, end_at)
    if event_at is not None:
        return event_at

    has_any_matching_event = any(
        event.get("new_status_id") in status_ids for event in events
    )
    current_status = order.get("current_status_id")
    updated_at = order.get("updated_at")
    created_at = order.get("created_at")
    if (
        not has_any_matching_event
        and current_status in status_ids
        and updated_at is not None
        and created_at is not None
        and created_at <= updated_at
        and start_at <= updated_at < end_at
    ):
        return updated_at
    return None


def first_processed_evidence_at(
    order: dict[str, Any],
    events: list[dict[str, Any]],
    start_at: datetime,
    end_at: datetime,
) -> datetime | None:
    candidates: list[datetime] = []
    event_at = first_event_at(events, PROCESSED_STATUS_IDS, start_at, end_at)
    if event_at is not None:
        candidates.append(event_at)

    send_carrier_at = order.get("send_carrier_at")
    if send_carrier_at is not None and start_at <= send_carrier_at < end_at:
        candidates.append(send_carrier_at)

    current_status = order.get("current_status_id")
    delivery_at = order.get("delivery_at")
    if (
        current_status in POST_DELIVERY_STATUS_IDS
        and delivery_at is not None
        and start_at <= delivery_at < end_at
    ):
        candidates.append(delivery_at)

    updated_at = order.get("updated_at")
    if (
        current_status in PROCESSED_STATUS_IDS
        and updated_at is not None
        and start_at <= updated_at < end_at
    ):
        candidates.append(updated_at)

    return min(candidates, default=None)


# ===========================================================================
# ĐƠN CHƯA BÀN GIAO (bản riêng, tách khỏi KPI)
# ===========================================================================
TABLE_NAME = os.getenv("BIGQUERY_TABLE_DON_CHUA_BAN_GIAO", "fact_nhanh_don_chua_ban_giao")
OUTPUT_DIR = Path("output 21-7")

COHORT_MORNING = "Đơn phải bàn giao sáng hôm nay"
COHORT_NEW = "Đơn mới trong ngày"
COHORT_BACKLOG = "Hàng tồn đọng từ 2 đến 5 ngày trước"

# Schema khai tay (KHÔNG dùng autodetect): nhiều cột có thể rỗng hoàn toàn
# trong 1 kỳ, autodetect sẽ đoán nhầm DATETIME thành FLOAT rồi fail ở kỳ sau.
TABLE_SCHEMA = [
    ("report_date", "DATE"),
    ("nhom_phai_ban_giao", "STRING"),
    ("hvc_event_at", "DATETIME"),
    ("so_ngay_ton", "INT64"),
    ("thoi_diem_ban_giao_muon", "DATETIME"),
    ("ma_don_san", "STRING"),
    ("sale_channel_name", "STRING"),
    ("carrier_name", "STRING"),
    ("current_status_name", "STRING"),
    ("synced_at", "DATETIME"),
]
DETAIL_COLUMNS = [name for name, _ in TABLE_SCHEMA]


class RobustApiClient(ApiClient):
    """Như ApiClient nhưng retry cả lỗi mạng (RemoteDisconnected/ConnectionError)
    — loại lỗi lọt qua retry gốc vì nó không phải HTTPError."""

    def post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                return super().post(url, payload)
            except OSError as error:
                last_error = error
                wait = min(2**attempt, 30)
                print(f"  [Mạng lỗi] {error} -> chờ {wait}s rồi thử lại...")
                time.sleep(wait)
        raise RuntimeError(f"Không thể gọi API sau 6 lần thử (mạng): {last_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Đẩy list đơn chưa bàn giao lên BigQuery (bảng riêng, tách khỏi KPI)."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="Số ngày ĐÃ ĐÓNG gần nhất (tính lùi từ hôm qua), mặc định: 3.",
    )
    parser.add_argument(
        "--depot-name",
        default=DEFAULT_DEPOT_NAME,
        help=f"Tên kho cần lọc, mặc định: {DEFAULT_DEPOT_NAME}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ xuất file local để đối chiếu, KHÔNG xóa/nạp BigQuery.",
    )
    return parser.parse_args()


def export_local(
    rows: list[dict[str, Any]], report_start: datetime, report_end: datetime
) -> None:
    """Xuất đúng bộ dữ liệu sắp đẩy BQ ra file, để đối chiếu tay trước khi ghi."""
    import pandas as pd

    df = pd.DataFrame(build_payload(rows), columns=DETAIL_COLUMNS)
    OUTPUT_DIR.mkdir(exist_ok=True)
    tag = f"{report_start:%Y%m%d}_{report_end:%Y%m%d}"
    xlsx_file = OUTPUT_DIR / f"{TABLE_NAME}_{tag}.xlsx"
    csv_file = OUTPUT_DIR / f"{TABLE_NAME}_{tag}.csv"

    doi_chieu = (
        df.groupby(["report_date", "nhom_phai_ban_giao", "carrier_name"])
        .size()
        .reset_index(name="so_don")
    )
    theo_ngay = (
        df.assign(da_ban_giao_muon=df["thoi_diem_ban_giao_muon"].notna())
        .groupby(["report_date", "da_ban_giao_muon"])
        .size()
        .reset_index(name="so_don")
    )

    with pd.ExcelWriter(xlsx_file) as writer:
        df.to_excel(writer, sheet_name="don_chua_ban_giao", index=False)
        doi_chieu.to_excel(writer, sheet_name="doi_chieu", index=False)
        theo_ngay.to_excel(writer, sheet_name="theo_ngay", index=False)
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")

    print(f"\nExcel (đối chiếu)     : {xlsx_file}")
    print(f"CSV   (bản sẽ đẩy BQ) : {csv_file}")


def fetch_source_data(
    session: RobustApiClient,
    app_id: str,
    business_id: str,
    depot_name: str,
    fetch_start: datetime,
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    """Kéo đơn + lịch sử, dùng đúng các hàm của engine."""
    depot_id = fetch_depot_id(session, app_id, business_id, depot_name)
    print(f"Kho lọc: {depot_name} (ID {depot_id})")

    print("Đang kéo danh sách đơn hàng...")
    orders = fetch_all_pages(
        session,
        api_url("order/list", app_id, business_id),
        {
            "createdAtFrom": int(fetch_start.timestamp()),
            "createdAtTo": int(now.timestamp()),
            "depotIds": [depot_id],
        },
        "order/list",
        show_progress=True,
    )
    orders_by_id = {
        int(order["info"]["id"]): order
        for order in orders
        if order.get("info", {}).get("id")
        and int(order.get("info", {}).get("depotId", 0)) == depot_id
    }
    print(f"Số đơn sau khi lọc kho: {len(orders_by_id)}")

    history: list[dict[str, Any]] = []
    history_url = api_url("order/history", app_id, business_id)
    order_id_chunks = list(chunks(list(orders_by_id), 100))
    api_without_history_ids: set[int] = set()
    print("Đang kéo lịch sử trạng thái đơn hàng...")
    for index, order_ids in enumerate(order_id_chunks, 1):
        batch_history, batch_without = fetch_history_for_orders(
            session, history_url, order_ids
        )
        history.extend(batch_history)
        api_without_history_ids.update(batch_without)
        if index == 1 or index % 20 == 0 or index == len(order_id_chunks):
            print(
                f"  order/history: batch {index}/{len(order_id_chunks)}, "
                f"đã nhận {len(history):,} log."
            )

    raw_history = repair_incomplete_history(
        session, history_url, orders_by_id, history, depot_name
    )
    status_history_counts: dict[int, int] = {}
    for row in raw_history:
        oid = int(row["order_id"])
        status_history_counts[oid] = status_history_counts.get(oid, 0) + 1
    raw_orders = [
        make_raw_order_row(
            order,
            depot_name,
            status_history_counts.get(oid, 0),
            oid in api_without_history_ids,
        )
        for oid, order in orders_by_id.items()
    ]

    history_by_order: dict[int, list[dict[str, Any]]] = {}
    for event in raw_history:
        history_by_order.setdefault(int(event["order_id"]), []).append(event)

    return raw_orders, history_by_order


def chua_ban_giao_for_day(
    raw_orders: list[dict[str, Any]],
    history_by_order: dict[int, list[dict[str, Any]]],
    report_day: datetime,
    run_time_naive: datetime,
) -> list[dict[str, Any]]:
    """List đơn chưa bàn giao của 1 ngày. Logic phân nhóm bám sát build_daily_kpis."""
    previous_18 = report_day - timedelta(hours=6)
    five_days_before = report_day - timedelta(days=5)
    morning_12 = report_day.replace(hour=12)
    today_18 = report_day.replace(hour=18)
    end_of_day = report_day.replace(hour=23, minute=59, second=59, microsecond=999999)
    new_order_limit = min(run_time_naive, today_18)
    end_of_day_data_limit = min(run_time_naive, end_of_day)

    rows: list[dict[str, Any]] = []
    for order in raw_orders:
        order_id = int(order["order_id"])
        events = history_by_order.get(order_id, [])

        hvc_event_at = first_event_at(
            events, {"HVC_CHANGE"}, datetime(2000, 1, 1), datetime(2099, 1, 1)
        )
        if hvc_event_at is None:
            continue
        order_created_at = order.get("created_at")
        if order_created_at is None:
            continue

        handover_ids = handover_status_ids_for(events)
        handover_before_midnight = first_event_or_current_status_at(
            order, events, handover_ids, order_created_at, report_day
        )
        cancel_before_midnight = first_event_or_current_status_at(
            order, events, CANCEL_STATUS_IDS, order_created_at, report_day
        )
        processed_before_midnight = first_processed_evidence_at(
            order, events, order_created_at, report_day
        )

        case_1 = previous_18 <= hvc_event_at < report_day
        case_2 = (previous_18 <= order_created_at < report_day) and (
            report_day <= hvc_event_at < morning_12
        )
        morning_required = (
            (case_1 or case_2)
            and handover_before_midnight is None
            and cancel_before_midnight is None
        )
        new_today = (
            report_day <= hvc_event_at < new_order_limit and not morning_required
        )
        backlog_from_two_days = (
            five_days_before <= order_created_at < previous_18
            and not morning_required
            and not new_today
            and processed_before_midnight is None
            and cancel_before_midnight is None
        )

        if morning_required:
            cohort = COHORT_MORNING
        elif new_today:
            cohort = COHORT_NEW
        elif backlog_from_two_days:
            cohort = COHORT_BACKLOG
        else:
            continue

        handover_all_day = first_event_or_current_status_at(
            order, events, handover_ids, report_day, end_of_day_data_limit
        )
        first_cancel_today = first_event_or_current_status_at(
            order, events, CANCEL_STATUS_IDS, report_day, end_of_day_data_limit
        )
        if handover_all_day is not None or first_cancel_today is not None:
            continue  # đã bàn giao hoặc đã hủy trong ngày -> không phải đơn muộn

        # Bàn giao muộn (sau ngày báo cáo) hay đến giờ vẫn chưa bàn giao?
        handover_ever = first_event_or_current_status_at(
            order, events, handover_ids, report_day, run_time_naive
        )
        late_handover = (
            handover_ever
            if handover_ever is not None and handover_ever >= end_of_day_data_limit
            else None
        )

        rows.append(
            {
                "report_date": report_day.date(),
                "nhom_phai_ban_giao": cohort,
                "hvc_event_at": hvc_event_at,
                "so_ngay_ton": (report_day.date() - order_created_at.date()).days,
                "thoi_diem_ban_giao_muon": late_handover,
                "ma_don_san": order.get("mã đơn sàn", ""),
                "sale_channel_name": order.get("sale_channel_name", ""),
                "carrier_name": order.get("carrier_name", ""),
                "current_status_name": order.get("current_status_name", ""),
                # Không đẩy lên BQ, chỉ dùng để check trùng khóa trước khi ghi.
                "_order_id": order_id,
            }
        )

    rows.sort(key=lambda r: (r["nhom_phai_ban_giao"], r["hvc_event_at"]))
    return rows


def to_bigquery_value(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def build_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ép kiểu về đúng schema; ô rỗng thành None thay vì chuỗi rỗng."""
    int_columns = {
        name for name, field_type in TABLE_SCHEMA if field_type == "INT64"
    }
    payload = []
    for row in rows:
        output: dict[str, Any] = {}
        for column in DETAIL_COLUMNS:
            value = row.get(column)
            if column == "report_date":
                output[column] = value.isoformat()
            elif column in int_columns:
                output[column] = None if value in (None, "") else int(value)
            else:
                output[column] = to_bigquery_value(value)
        payload.append(output)
    return payload


def sync_to_bigquery(
    rows: list[dict[str, Any]], report_start: datetime, report_end: datetime
) -> None:
    from google.cloud import bigquery
    from google.oauth2 import service_account

    key_path = os.getenv("BIGQUERY_KEY_PATH", "")
    project_id = os.getenv("BIGQUERY_PROJECT_ID", "")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "")
    missing = [
        name
        for name, value in [
            ("BIGQUERY_KEY_PATH", key_path),
            ("BIGQUERY_PROJECT_ID", project_id),
            ("BIGQUERY_DATASET_ID", dataset_id),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError(f"Thiếu cấu hình BigQuery trong .env: {', '.join(missing)}")
    if not Path(key_path).exists():
        raise RuntimeError(f"Không tìm thấy BigQuery key: {key_path}")

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project_id, credentials=credentials)
    target_table_id = f"{project_id}.{dataset_id}.{TABLE_NAME}"
    schema = [
        bigquery.SchemaField(name, field_type) for name, field_type in TABLE_SCHEMA
    ]

    # Tạo bảng nếu chưa có: partition theo report_date để BI quét ít data.
    table = bigquery.Table(target_table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="report_date"
    )
    table.clustering_fields = ["carrier_name", "nhom_phai_ban_giao"]
    client.create_table(table, exists_ok=True)

    payload = build_payload(rows)
    key_counts: dict[tuple[Any, Any], int] = {}
    for row in rows:
        key = (row["report_date"], row["_order_id"])
        key_counts[key] = key_counts.get(key, 0) + 1
    duplicates = [key for key, count in key_counts.items() if count > 1]
    if duplicates:
        raise RuntimeError(
            f"Có {len(duplicates)} khóa (report_date, order_id) bị trùng; dừng đồng bộ."
        )

    start_date = report_start.date().isoformat()
    end_date = report_end.date().isoformat()
    client.query(
        f"DELETE FROM `{target_table_id}` "
        f"WHERE report_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
    ).result()

    if payload:
        client.load_table_from_json(
            payload,
            target_table_id,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ),
        ).result()

    verify = next(
        client.query(
            f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT report_date) AS day_count "
            f"FROM `{target_table_id}` "
            f"WHERE report_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
        ).result()
    )
    if verify.row_count != len(payload):
        raise RuntimeError(
            f"BigQuery sau đồng bộ sai số dòng: dự kiến {len(payload)}, "
            f"nhận {verify.row_count}."
        )
    print(
        f"Đã đồng bộ BigQuery: {target_table_id}, "
        f"{verify.row_count} dòng / {verify.day_count} ngày."
    )


def main() -> None:
    args = parse_args()
    if args.days < 1 or args.days > 31:
        raise RuntimeError("--days phải nằm trong khoảng 1 đến 31.")

    token, app_id, business_id = load_credentials()
    session = RobustApiClient(token)
    now = datetime.now(VN_TZ)
    run_time_naive = now.replace(tzinfo=None)
    # Chỉ ghi các ngày ĐÃ ĐÓNG: ngày báo cáo cuối là hôm qua. Ngày hôm nay còn
    # đang chạy dở nên số "chưa bàn giao" sẽ phình rất to rồi tụt dần đến tối,
    # đưa lên BI là gây hiểu nhầm.
    last_report_day = run_time_naive.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    report_start = last_report_day - timedelta(days=args.days - 1)
    fetch_start = (report_start - timedelta(days=5)).replace(tzinfo=VN_TZ)

    print(
        f"Kỳ báo cáo: {report_start:%Y-%m-%d} đến {last_report_day:%Y-%m-%d} "
        f"(bỏ ngày hôm nay {run_time_naive:%Y-%m-%d} vì chưa đóng ngày); "
        f"phạm vi kéo đơn: {fetch_start:%Y-%m-%d %H:%M:%S} đến {now:%Y-%m-%d %H:%M:%S}"
    )
    raw_orders, history_by_order = fetch_source_data(
        session, app_id, business_id, args.depot_name, fetch_start, now
    )

    report_day = report_start
    rows: list[dict[str, Any]] = []
    while report_day <= last_report_day:
        day_rows = chua_ban_giao_for_day(
            raw_orders, history_by_order, report_day, run_time_naive
        )
        rows.extend(day_rows)
        print(f"  {report_day:%Y-%m-%d}: {len(day_rows)} đơn chưa bàn giao")
        report_day += timedelta(days=1)

    # Mốc đồng bộ: chung cho cả lô, để biết dòng chốt lúc nào và job có chạy không.
    for row in rows:
        row["synced_at"] = run_time_naive

    print(f"Tổng: {len(rows)} dòng.")
    if args.dry_run:
        export_local(rows, report_start, last_report_day)
        print("[dry-run] KHÔNG đụng BigQuery.")
        return
    sync_to_bigquery(rows, report_start, last_report_day)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Lỗi: {error}", file=sys.stderr)
        sys.exit(1)
