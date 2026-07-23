"""Đẩy danh sách ĐƠN CHƯA BÀN GIAO (đơn muộn) lên BigQuery.

Pipeline ĐỘC LẬP với nhanh_order_status_history: bảng riêng, không đụng vào
bảng KPI `fact_nhanh_order_status`. Chỉ dùng chung phần logic phân loại đơn
(import từ file bản chính) để số liệu luôn khớp với KPI.

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
    py nhanh_don_chua_ban_giao.py               # 3 ngày đã đóng gần nhất -> BigQuery
    py nhanh_don_chua_ban_giao.py --days 7      # 7 ngày đã đóng gần nhất
    py nhanh_don_chua_ban_giao.py --dry-run     # chỉ xuất file local, KHÔNG đụng BigQuery
"""

import argparse
import importlib.util
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# Đổi 1 dòng này khi bump version bản chính (V3, V4...).
MAIN_MODULE_FILE = Path(__file__).with_name("nhanh_order_status_history.py")

_spec = importlib.util.spec_from_file_location("nhanh_main", MAIN_MODULE_FILE)
m = importlib.util.module_from_spec(_spec)
sys.modules["nhanh_main"] = m
_spec.loader.exec_module(m)

# Credential fix cứng để khỏi cần file .env khi chạy trên GitHub Actions.
# setdefault: nếu môi trường đã set (vd chạy local có .env riêng) thì vẫn ưu tiên cái đó.
# LƯU Ý: accessToken nằm thẳng trong code -> để repo PRIVATE. Token lộ thì cấp lại trên Nhanh.
os.environ.setdefault("AppID", "77571")
os.environ.setdefault(
    "accessToken",
    "Td5xd7XZOD3Nq7eEKekAE5tsdRBn2h6ZFpotZyTPHIXUBuKySfjKhrgQFqXkKZ8ItXnYWLeuKSblbIGEqIhpXPo0JAftYcFwJghL4uNoMuNawRYJIixGMx1wt6BXSnR2KsEf5tRZd4MZ",
)
os.environ.setdefault("BusinessID", "224108")
os.environ.setdefault("BIGQUERY_PROJECT_ID", "rhysman-data-warehouse-488306")
os.environ.setdefault("BIGQUERY_DATASET_ID", "rhysman")

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


class RobustApiClient(m.ApiClient):
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
        default=m.DEFAULT_DEPOT_NAME,
        help=f"Tên kho cần lọc, mặc định: {m.DEFAULT_DEPOT_NAME}.",
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
    """Kéo đơn + lịch sử, dùng đúng các hàm của pipeline chính."""
    depot_id = m.fetch_depot_id(session, app_id, business_id, depot_name)
    print(f"Kho lọc: {depot_name} (ID {depot_id})")

    print("Đang kéo danh sách đơn hàng...")
    orders = m.fetch_all_pages(
        session,
        m.api_url("order/list", app_id, business_id),
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
    history_url = m.api_url("order/history", app_id, business_id)
    order_id_chunks = list(m.chunks(list(orders_by_id), 100))
    api_without_history_ids: set[int] = set()
    print("Đang kéo lịch sử trạng thái đơn hàng...")
    for index, order_ids in enumerate(order_id_chunks, 1):
        batch_history, batch_without = m.fetch_history_for_orders(
            session, history_url, order_ids
        )
        history.extend(batch_history)
        api_without_history_ids.update(batch_without)
        if index == 1 or index % 20 == 0 or index == len(order_id_chunks):
            print(
                f"  order/history: batch {index}/{len(order_id_chunks)}, "
                f"đã nhận {len(history):,} log."
            )

    raw_history = m.repair_incomplete_history(
        session, history_url, orders_by_id, history, depot_name
    )
    status_history_counts: dict[int, int] = {}
    for row in raw_history:
        oid = int(row["order_id"])
        status_history_counts[oid] = status_history_counts.get(oid, 0) + 1
    raw_orders = [
        m.make_raw_order_row(
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

        hvc_event_at = m.first_event_at(
            events, {"HVC_CHANGE"}, datetime(2000, 1, 1), datetime(2099, 1, 1)
        )
        if hvc_event_at is None:
            continue
        order_created_at = order.get("created_at")
        if order_created_at is None:
            continue

        handover_ids = m.handover_status_ids_for(events)
        handover_before_midnight = m.first_event_or_current_status_at(
            order, events, handover_ids, order_created_at, report_day
        )
        cancel_before_midnight = m.first_event_or_current_status_at(
            order, events, m.CANCEL_STATUS_IDS, order_created_at, report_day
        )
        processed_before_midnight = m.first_processed_evidence_at(
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

        handover_all_day = m.first_event_or_current_status_at(
            order, events, handover_ids, report_day, end_of_day_data_limit
        )
        first_cancel_today = m.first_event_or_current_status_at(
            order, events, m.CANCEL_STATUS_IDS, report_day, end_of_day_data_limit
        )
        if handover_all_day is not None or first_cancel_today is not None:
            continue  # đã bàn giao hoặc đã hủy trong ngày -> không phải đơn muộn

        # Bàn giao muộn (sau ngày báo cáo) hay đến giờ vẫn chưa bàn giao?
        handover_ever = m.first_event_or_current_status_at(
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

    token, app_id, business_id = m.load_credentials()
    session = RobustApiClient(token)
    now = datetime.now(m.VN_TZ)
    run_time_naive = now.replace(tzinfo=None)
    # Chỉ ghi các ngày ĐÃ ĐÓNG: ngày báo cáo cuối là hôm qua. Ngày hôm nay còn
    # đang chạy dở nên số "chưa bàn giao" sẽ phình rất to rồi tụt dần đến tối,
    # đưa lên BI là gây hiểu nhầm.
    last_report_day = run_time_naive.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    report_start = last_report_day - timedelta(days=args.days - 1)
    fetch_start = (report_start - timedelta(days=5)).replace(tzinfo=m.VN_TZ)

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
