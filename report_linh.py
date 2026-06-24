import pandas as pd
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import os


# =========================
# CONFIG
# =========================
# SERVICE_ACCOUNT_FILE = r"E:\hongphuc\Source code\code kéo dữ liệu SQL Sever (Thảo)\rhysman-data-warehouse-488306-8db2b940e56a.json"

PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman_bronze"
TABLE_ID = "fact_cskh_fb_si"

SPREADSHEET_ID = "1QOYiRw1Z4PM4AfNSHBpOR7-4cc29jN-cTfSxR3s_0Cs"
WORKSHEET_NAME = "PosSheets(Report Linh)" 

TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# =========================
# AUTHENTICATION
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
# df = df[df["ID"].notna() & (df["ID"] != "")]


# =========================
# RENAME COLUMNS
# =========================
rename_map = {
    "ID": "ID",
    "Trạng thái": "Trang_thai",
    "Doanh thu chưa trừ phí sàn": "Doanh_thu_chua_tru_phi_san",
    "Người xử lý": "Nguoi_xu_ly",
    "Nhân viên CSKH": "Nhan_vien_CSKH",
    "Thẻ": "The",
    "Sản phẩm": "San_pham",
    "NV xác nhận": "NV_xac_nhan",
    "Ngày tạo đơn": "Ngay_tao_don",
    "Thời điểm cập nhật trạng thái": "Thoi_diem_cap_nhat_trang_trai",
    "Nguồn đơn": "Nguon_don",
    "Mã sản phẩm": "Ma_san_pham",
    "Số lượng": "So_luong",
  
}

df = df.rename(columns=rename_map)

# =========================
# CHỈ GIỮ 6 CỘT CẦN THIẾT
# =========================

required_columns = [
    "ID",
    "Trang_thai",
    "Doanh_thu_chua_tru_phi_san",
    "Nguoi_xu_ly",
    "Nhan_vien_CSKH",
    "The",
    "San_pham",
    "NV_xac_nhan",
    "Ngay_tao_don",
    "Thoi_diem_cap_nhat_trang_trai",
    "Nguon_don",
    "Ma_san_pham",
    "So_luong"
]

missing_cols = [col for col in required_columns if col not in df.columns]

if missing_cols:
    raise ValueError(f"Thiếu cột sau khi rename: {missing_cols}")

df = df[required_columns].copy()

# =========================
# REMOVE INVALID ID + DUPLICATE WITH BIGQUERY
# =========================
# =========================
# REMOVE INVALID ID
# =========================
# INVALID_IDS = {"-", "_"}

# df = df[
#     df["ID"].notna() &
#     (~df["ID"].isin(INVALID_IDS)) &
#     (df["ID"].str.strip() != "")
# ].copy()

print(f"After invalid-id filter: {len(df)} rows")

# =========================
# CLEAN ALL EMPTY STRINGS
# =========================
df = df.replace(r'^\s*$', None, regex=True)

df["Doanh_thu_chua_tru_phi_san"] = (
    df["Doanh_thu_chua_tru_phi_san"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace(".", "", regex=False)   # bỏ nếu dữ liệu của bạn có số thập phân thật
    .str.strip()
    .replace({"": None, "nan": None, "None": None})
)

def to_decimal(x):
    if x is None or pd.isna(x):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

df["Doanh_thu_chua_tru_phi_san"] = df["Doanh_thu_chua_tru_phi_san"].apply(to_decimal)

# =========================
# CLEAN DATE + FILL DATE FOR ID = "_"
# =========================

df["Ngay_tao_don"] = pd.to_datetime(
    df["Ngay_tao_don"],
    errors="coerce",
    dayfirst=True
)

df["Thoi_diem_cap_nhat_trang_trai"] = pd.to_datetime(
    df["Thoi_diem_cap_nhat_trang_trai"],
    errors="coerce",
    dayfirst=True
)

# Tạo cột fill xuống từ dòng phía trên
df["Ngay_tao_don_fill"] = df["Ngay_tao_don"].ffill()
df["Thoi_diem_cap_nhat_trang_trai_fill"] = df["Thoi_diem_cap_nhat_trang_trai"].ffill()

# Chỉ fill cho dòng con có ID = "_"
df.loc[df["ID"] == "_", "Ngay_tao_don"] = df.loc[df["ID"] == "_", "Ngay_tao_don_fill"]

df.loc[df["ID"] == "_", "Thoi_diem_cap_nhat_trang_trai"] = df.loc[
    df["ID"] == "_",
    "Thoi_diem_cap_nhat_trang_trai_fill"
]

# Xóa cột phụ
df = df.drop(columns=[
    "Ngay_tao_don_fill",
    "Thoi_diem_cap_nhat_trang_trai_fill"
])

text_columns = [
    "ID",
    "Trang_thai",
    "Nguoi_xu_ly",
    "Nhan_vien_CSKH",
    "The",
    "San_pham",
    "NV_xac_nhan",
    "Nguon_don",
    "Ma_san_pham"
]

for col in text_columns:
    df[col] = (
        df[col]
        .astype(str)
        .str.strip()
        .replace({"nan": None, "None": None, "": None})
    )

df["So_luong"] = (
    df["So_luong"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
    .replace({"": None, "nan": None, "None": None})
)

df["So_luong"] = pd.to_numeric(df["So_luong"], errors="coerce").astype("Int64")


# =========================
# DEFINE CUTOFF (25 NGÀY)
# =========================
cutoff_date = (datetime.today() - timedelta(days=30)).date()
# cutoff_date = datetime(2026, 1, 1).date()
print("Start date:", cutoff_date)

# =========================
# DELETE DỮ LIỆU TỪ 01/01/2026 TRỞ ĐI TRONG BIGQUERY
# =========================
delete_query = f"""
DELETE FROM `{TABLE_FULL_ID}`
WHERE DATE(Ngay_tao_don) >= DATE('{cutoff_date}')
"""

delete_job = bq_client.query(delete_query)
delete_job.result()

print("✅ Deleted data from 2026-01-01 in BigQuery")

# =========================
# FILTER: CHỈ LẤY DỮ LIỆU TỪ 01/01/2026 TRỞ ĐI
# =========================
df = df[df["Ngay_tao_don"].notna()]
df = df[df["Ngay_tao_don"].dt.date >= cutoff_date]

print(f"Reloading {len(df)} rows from 2026-01-01")

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
