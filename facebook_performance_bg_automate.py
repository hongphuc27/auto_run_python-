import pandas as pd
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import os
import json


# =========================
# CONFIG
# =========================
# SERVICE_ACCOUNT_FILE = r"E:\hongphuc\Source code\code kéo dữ liệu SQL Sever (Thảo)\rhysman-data-warehouse-488306-8db2b940e56a.json"

PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_order_sales_facebook_performance"

SPREADSHEET_ID = "1XLSz6Mz_r8SdAxK2UIqAACmoXp1kahQ_0delRM-LvgI"
WORKSHEET_NAME = "PosSheets(Report cho Cường Thảo (PhucLH tạo))"

TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# =========================
# AUTHENTICATION
# =========================
# =========================
# AUTHENTICATION (Cloud Version)
# =========================

# BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# Google Sheet auth
scopes = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

sheet_creds = Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=scopes
)

gc = gspread.authorize(sheet_creds)

sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(WORKSHEET_NAME)

all_values = ws.get_all_values()

headers = all_values[0]
data = all_values[1:]

df = pd.DataFrame(data, columns=headers)

print(f"Loaded {len(df)} rows from Google Sheet")

# =========================
# CLEAN ID
# =========================
df["ID"] = df["ID"].astype(str).str.strip()
df = df[df["ID"].notna() & (df["ID"] != "")]

# =========================
# RENAME COLUMNS
# =========================
rename_map = {
    "ID": "id",
    "Mã vận đơn": "ma_van_don",
    "Trạng thái": "trang_thai",
    "Trạng thái VC": "trang_thai_vc",
    "Thẻ": "the",
    "NV xác nhận": "nv_xac_nhan",
    "Người xử lý": "nguoi_xu_ly",
    "Khách hàng": "khach_hang",
    "Địa chỉ": "dia_chi",
    "Số điện thoại": "so_dien_thoai",
    "Ngày tạo đơn": "ngay_tao_don",
    "Thời điểm tạo đơn": "thoi_diem_tao_don",
    "Ngày tạo sản phẩm": "ngay_tao_san_pham",
    "Thời điểm cập nhật trạng thái": "thoi_diem_cap_nhat_trang_thai",
    "Thời điểm cập nhật sang đã nhận": "thoi_diem_cap_nhat_da_nhan",
    "Nguồn đơn": "nguon_don",
    "Sản phẩm": "san_pham",
    "Mã sản phẩm": "ma_san_pham",
    "Mã mẫu mã": "ma_mau_ma",
    "Số lượng": "so_luong",
    "Tổng số lượng SP": "tong_so_luong_sp",
    "Giá nhập từng SP": "gia_nhap_tung_sp",
    "Tổng giá nhập SP": "tong_gia_nhap_sp",
    "Giá nhập đơn hàng": "gia_nhap_don_hang",
    "Giá SP": "gia_sp",
    "Giảm giá": "giam_gia",
    "Giảm giá từng sản phẩm": "giam_gia_tung_san_pham",
    "Trị giá đơn hàng": "tri_gia_don_hang",
    "Trị giá đơn hàng đã chiết khấu": "tri_gia_don_hang_da_chiet_khau",
    "COD": "cod",
    "COD đối soát": "cod_doi_soat",
    "Phí trả cho đơn vị VC": "phi_tra_cho_don_vi_vc",
    "Phí VC thu của khách": "phi_vc_thu_cua_khach",
    "Tổng tiền đơn hàng": "tong_tien_don_hang",
    "Tổng tiền đơn hàng (trừ phí ship)": "tong_tien_don_hang_tru_phi_ship",
    "Tổng tiền đơn hàng (trừ chiết khấu)": "tong_tien_don_hang_tru_chiet_khau",
    "Tổng số tiền": "tong_so_tien",
    "Doanh thu đơn hàng": "doanh_thu_don_hang",
    "Doanh thu chưa trừ phí sàn": "doanh_thu_chua_tru_phi_san",
    "Doanh thu không trừ hoàn": "doanh_thu_khong_tru_hoan",
    "Doanh thu bán hàng": "doanh_thu_ban_hang",
    "Ghi chú sản phẩm": "ghi_chu_san_pham",
    "Ghi chú nội bộ": "ghi_chu_noi_bo",
    "Gồm các mã sản phẩm": "gom_cac_ma_san_pham",
    "Đơn vị tiền tệ": "don_vi_tien_te",
    "Kho hàng": "kho_hang",
    "Ad Id": "ad_id",
}

df = df.rename(columns=rename_map)

# =========================
# CLEAN ALL EMPTY STRINGS
# =========================
df = df.replace(r'^\s*$', None, regex=True)

# =========================
# FIX DATE
# =========================
if "ngay_tao_don" in df.columns:
    df["ngay_tao_don"] = pd.to_datetime(
        df["ngay_tao_don"],
        errors="coerce",
        dayfirst=True
    ).dt.date

if "ad_id" in df.columns:
    df["ad_id"] = df["ad_id"].astype(str).str.strip()



# =========================
# DEFINE CUTOFF (20 NGÀY)
# =========================
cutoff_date = (datetime.today() - timedelta(days=90)).date()
print("Cutoff date:", cutoff_date)

# =========================
# DELETE 20 NGÀY GẦN NHẤT TRONG BIGQUERY
# =========================
delete_query = f"""
DELETE FROM `{TABLE_FULL_ID}`
WHERE ngay_tao_don >= DATE('{cutoff_date}')
"""

delete_job = bq_client.query(delete_query)
delete_job.result()

print("✅ Deleted last 20 days in BigQuery")

# =========================
# FILTER: CHỈ LẤY 20 NGÀY GẦN NHẤT
# =========================
if "ngay_tao_don" in df.columns:
    df = df[df["ngay_tao_don"] >= cutoff_date]

print(f"Reloading {len(df)} rows from last 20 days")



# =========================
# LOAD TO BIGQUERY
# =========================
# =========================
# FIX INTEGER COLUMNS
# =========================

int_columns = [
    "so_luong",
    "tong_so_luong_sp",
    "gia_nhap_tung_sp",
    "tong_gia_nhap_sp",
    "gia_nhap_don_hang",
    "gia_sp",
    "giam_gia",
    "giam_gia_tung_san_pham",
    "tri_gia_don_hang",
    "tri_gia_don_hang_da_chiet_khau",
    "cod",
    "cod_doi_soat",
    "phi_tra_cho_don_vi_vc",
    "phi_vc_thu_cua_khach",
    "tong_tien_don_hang",
    "tong_tien_don_hang_tru_phi_ship",
    "tong_tien_don_hang_tru_chiet_khau",
    "tong_so_tien",
    "doanh_thu_don_hang",
    "doanh_thu_chua_tru_phi_san",
    "doanh_thu_khong_tru_hoan",
    "doanh_thu_ban_hang",
]

for col in int_columns:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        ).fillna(0).astype("int64")


# =========================
# FIX TIMESTAMP COLUMNS   ← CHÈN Ở ĐÂY
# =========================
timestamp_columns = [
    "thoi_diem_tao_don",
    "ngay_tao_san_pham",
    "thoi_diem_cap_nhat_trang_thai",
    "thoi_diem_cap_nhat_da_nhan",
]

for col in timestamp_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(
            df[col],
            errors="coerce",
            dayfirst=True
        )


# =========================
# LOAD TO BIGQUERY
# =========================
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
)

load_job = bq_client.load_table_from_dataframe(
    df,
    TABLE_FULL_ID,
    job_config=job_config
)

load_job.result()



print("✅ Google Sheet → BigQuery SUCCESS")
