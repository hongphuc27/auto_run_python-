# -*- coding: utf-8 -*-
"""ETL: Google Sheets -> BigQuery (rhysman.fact_cskhzaloOA)

- Đọc dữ liệu từ Google Sheets.
- Fill xuống thông tin đơn hàng cho các dòng sản phẩm con.
- Chỉ lấy dữ liệu WINDOW_DAYS ngày gần nhất theo giờ Việt Nam.
- Xóa dữ liệu cũ trong khoảng thời gian cập nhật rồi ghi dữ liệu mới vào BigQuery.
"""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# =========================================================
# CONFIG
# =========================================================

KEY_FILE = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    r"E:\RYSH MAN\rhysman-data-warehouse-488306-8db2b940e56a.json",
)

SHEET_ID = "1lGXhLQffq_tcaHJbObhO1zzc-GnZWEeRY9dXN5FCMbw"
GID = 1370755167

PROJECT_ID = "rhysman-data-warehouse-488306"
TABLE_FULL_ID = f"{PROJECT_ID}.rhysman.fact_cskhzaloOA"

WINDOW_DAYS = 
TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# Những cột cần fill từ dòng đầu đơn xuống dòng sản phẩm con
FILL_COLS = [
    "ID",
    "Trạng thái",
    "Thời điểm tạo đơn",
    "Nhân viên CSKH",
]

PLACEHOLDERS = {"-", "_", ""}


# =========================================================
# TÊN CỘT BIGQUERY -> TÊN CỘT GOOGLE SHEETS
# =========================================================

COLUMN_MAP = {
    "id": "ID",
    "trang_thai": "Trạng thái",
    "the": "Thẻ",
    "nhan_vien_cskh": "Nhân viên CSKH",
    "thoi_diem_tao_don": "Thời điểm tạo đơn",
    "doanh_thu_chua_tru_phi_san": "Doanh thu chưa trừ phí sàn",
    "thoi_diem_cap_nhat_trang_thai": "Thời điểm cập nhật trạng thái",
    "giam_gia": "Giảm giá",
    "phi_vc_thu_cua_khach": "Phí VC thu của khách",
    "phi_tra_cho_don_vi_vc": "Phí trả cho đơn vị VC",
    "san_pham": "Sản phẩm",
    "ma_san_pham": "Mã sản phẩm",
    "giam_gia_tren_moi_san_pham": "Giảm giá trên mỗi sản phẩm",
    "gom_cac_ma_san_pham": "Gồm các mã sản phẩm",
    "cau_thanh_san_pham": "Cấu thành sản phẩm",
    "ten_san_pham": "Tên sản phẩm",
    "ngay_tao_san_pham": "Ngày tạo sản phẩm",
}


# Các cột kiểu INT64
INT_COLS = [
    "doanh_thu_chua_tru_phi_san",
    "giam_gia",
    "phi_vc_thu_cua_khach",
    "phi_tra_cho_don_vi_vc",
    "giam_gia_tren_moi_san_pham",
]


# Các cột kiểu DATETIME
TS_COLS = [
    "thoi_diem_tao_don",
    "thoi_diem_cap_nhat_trang_thai",
    "ngay_tao_san_pham",
]


# =========================================================
# ĐỌC GOOGLE SHEETS
# =========================================================

def load_dataframe():
    gc = gspread.service_account(filename=KEY_FILE)

    ws = gc.open_by_key(SHEET_ID).get_worksheet_by_id(GID)

    if ws is None:
        raise SystemExit(
            f"Khong tim thay worksheet gid={GID}"
        )

    rows = ws.get_all_values()

    if not rows:
        raise SystemExit("Google Sheet khong co du lieu.")

    headers = [str(column).strip() for column in rows[0]]

    df = pd.DataFrame(
        rows[1:],
        columns=headers,
    )

    # Kiểm tra toàn bộ 17 cột cần lấy
    required_columns = list(COLUMN_MAP.values())

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise SystemExit(
            f"Sheet thieu cot: {missing}. "
            f"Cac cot hien co: {list(df.columns)}"
        )

    return df, ws.title


# =========================================================
# FILL DÒNG SẢN PHẨM CON
# =========================================================

def fill_down(df):
    stripped = df.apply(
        lambda column: column.fillna("").str.strip(),
        axis=0,
    )

    # Bỏ các dòng trống hoàn toàn
    junk = (stripped == "").all(axis=1)

    df = df[~junk].copy()
    stripped = stripped[~junk].copy()

    if df.empty:
        raise SystemExit(
            "Sheet khong con du lieu sau khi bo dong trong."
        )

    # Dòng có ID thật là dòng đầu của một đơn hàng
    # Các giá trị "", "-", "_" được xem là dòng sản phẩm con
    header = ~stripped[FILL_COLS[0]].isin(PLACEHOLDERS)

    if not header.iloc[0]:
        raise SystemExit(
            "Dong dau tien khong phai dong dau don. "
            "Du lieu nguon co the bi xao tron."
        )

    # Đánh số nhóm đơn hàng
    grp = header.cumsum()

    for column in FILL_COLS:
        # Lấy giá trị tại dòng đầu của từng đơn
        header_value = (
            df[column]
            .where(header)
            .groupby(grp)
            .transform("first")
        )

        # Xác định ô đang trống hoặc có dấu - hoặc _
        is_placeholder = stripped[column].isin(PLACEHOLDERS)

        # Chỉ fill dòng con, không sửa dòng đầu đơn
        df.loc[
            ~header & is_placeholder,
            column,
        ] = header_value[
            ~header & is_placeholder
        ]

    filled_rows = int((~header).sum())
    junk_rows = int(junk.sum())

    return df, filled_rows, junk_rows


# =========================================================
# CHỈ GIỮ 17 CỘT VÀ CHUYỂN KIỂU DỮ LIỆU
# =========================================================

def to_bq_frame(df):
    out = pd.DataFrame(index=df.index)

    # Chỉ lấy đúng các cột trong COLUMN_MAP
    for bq_column, sheet_column in COLUMN_MAP.items():
        out[bq_column] = (
            df[sheet_column]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"": None, "-": None, "_": None})
        )

    # Chuyển các cột tiền sang INT64
    for column in INT_COLS:
        raw = (
            out[column]
            .astype("string")
            .str.replace(",", "", regex=False)
            .str.strip()
        )

        out[column] = (
            pd.to_numeric(raw, errors="coerce")
            .round()
            .astype("Int64")
        )

    # Chuyển các cột ngày giờ
    for column in TS_COLS:
        out[column] = pd.to_datetime(
            out[column],
            errors="coerce",
            dayfirst=True,
        )

    # Kiểm tra ID sau khi fill
    missing_id = out["id"].isna().sum()

    if missing_id:
        raise SystemExit(
            f"{missing_id} dong khong co ID sau khi fill."
        )

    # Đảm bảo đúng thứ tự 17 cột
    return out[list(COLUMN_MAP.keys())]


# =========================================================
# GHI DỮ LIỆU VÀO BIGQUERY
# =========================================================

def upsert_to_bigquery(out, cutoff):
    credentials = (
        service_account.Credentials
        .from_service_account_file(KEY_FILE)
    )

    client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
    )

    # Lấy schema trực tiếp từ bảng đã tạo
    table = client.get_table(TABLE_FULL_ID)
    schema = table.schema

    # Xóa dữ liệu cũ trong cửa sổ cập nhật
    delete_sql = f"""
        DELETE FROM `{TABLE_FULL_ID}`
        WHERE DATE(thoi_diem_tao_don) >= DATE('{cutoff}')
    """

    client.query(delete_sql).result()

    print(
        f"Da xoa du lieu BigQuery tu ngay {cutoff}"
    )

    # Ghi dữ liệu mới
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_dataframe(
        out,
        TABLE_FULL_ID,
        job_config=job_config,
    )

    load_job.result()

    table_after_load = client.get_table(TABLE_FULL_ID)

    return table_after_load.num_rows


# =========================================================
# MAIN
# =========================================================

def main():
    print("=== BAT DAU ETL CSKH ZALO OA ===")

    cutoff = (
        datetime.now(TZ).date()
        - timedelta(days=WINDOW_DAYS)
    )

    print(f"Cutoff theo gio Viet Nam: {cutoff}")

    # Đọc Sheet
    df, sheet_title = load_dataframe()

    print(
        f"Da doc {len(df)} dong "
        f"tu sheet '{sheet_title}'"
    )

    # Fill dòng sản phẩm con
    df, filled_rows, junk_rows = fill_down(df)

    print(
        f"Da fill {filled_rows} dong con, "
        f"bo {junk_rows} dong trong"
    )

    # Chọn cột và chuyển kiểu dữ liệu
    out = to_bq_frame(df)

    print(
        f"So dong sau khi chuan hoa: {len(out)}"
    )

    # Chỉ lấy dữ liệu trong WINDOW_DAYS ngày gần nhất
    out = out[
        out["thoi_diem_tao_don"].notna()
        & (
            out["thoi_diem_tao_don"].dt.date
            >= cutoff
        )
    ].copy()

    if out.empty:
        raise SystemExit(
            f"0 dong trong {WINDOW_DAYS} ngay gan nhat. "
            "Khong cap nhat BigQuery."
        )

    print(
        f"Trong cua so {WINDOW_DAYS} ngay: "
        f"{len(out)} dong / "
        f"{out['id'].nunique()} don"
    )

    # Ghi BigQuery
    total_rows = upsert_to_bigquery(
        out=out,
        cutoff=cutoff,
    )

    print(
        f"BigQuery OK: bang {TABLE_FULL_ID} "
        f"hien co {total_rows} dong"
    )

    print("=== HOAN THANH ETL CSKH ZALO OA ===")


if __name__ == "__main__":
    main()
