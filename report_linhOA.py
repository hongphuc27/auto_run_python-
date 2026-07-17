# -*- coding: utf-8 -*-
"""ETL: Google Sheets -> BigQuery (rhysman.fact_fb_zalo_si)

- Doc sheet PosSheets(FB TC CS), fill xuong ID / Trang thai / Thoi diem tao don /
  Nhan vien CSKH cho cac dong con cua don nhieu san pham (fill theo cum don, khong lan don khac).
- Chi lay du lieu WINDOW_DAYS ngay gan nhat (theo gio VN).
- Update BigQuery: DELETE cua so WINDOW_DAYS ngay roi APPEND du lieu moi keo ve.

Chay tren GitHub Actions: dat secret vao file va tro bien moi truong
GOOGLE_APPLICATION_CREDENTIALS toi file do.
"""
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

KEY_FILE = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    r"E:\RYSH MAN\rhysman-data-warehouse-488306-8db2b940e56a.json",
)
SHEET_ID = "1lGXhLQffq_tcaHJbObhO1zzc-GnZWEeRY9dXN5FCMbw"
GID = 1370755167
PROJECT_ID = "rhysman-data-warehouse-488306"
TABLE_FULL_ID = f"{PROJECT_ID}.rhysman.fact_cskhzaloOA"
WINDOW_DAYS = 35
TZ = ZoneInfo("Asia/Ho_Chi_Minh")

FILL_COLS = ["ID", "Trạng thái", "Thời điểm tạo đơn", "Nhân viên CSKH"]
PLACEHOLDERS = {"-", "_", ""}

# ten cot BigQuery -> ten cot tren Google Sheet
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

INT_COLS = [
    "doanh_thu_chua_tru_phi_san",
    "giam_gia",
    "phi_vc_thu_cua_khach",
    "phi_tra_cho_don_vi_vc",
    "giam_gia_tren_moi_san_pham",
]

TS_COLS = [
    "thoi_diem_tao_don",
    "thoi_diem_cap_nhat_trang_thai",
    "ngay_tao_san_pham",
]

def load_dataframe():
    gc = gspread.service_account(filename=KEY_FILE)
    ws = gc.open_by_key(SHEET_ID).get_worksheet_by_id(GID)
    if ws is None:
        raise SystemExit(f"Khong tim thay worksheet gid={GID}")
    rows = ws.get_all_values()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    missing = [c for c in FILL_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Sheet thieu cot {missing}. Cac cot: {list(df.columns)}")
    return df, ws.title


def fill_down(df):
    stripped = df.apply(lambda c: c.fillna("").str.strip(), axis=0)

    # bo dong trong hoan toan (dong rac cuoi sheet) truoc khi fill
    junk = (stripped == "").all(axis=1)
    df, stripped = df[~junk].copy(), stripped[~junk]

    # fill theo cum don: dong con chi lay gia tri tu DONG DAU cua chinh don do,
