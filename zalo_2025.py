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
DATASET_ID = "rhysman"
TABLE_ID = "fact_cskh"

SPREADSHEET_ID = "1Ea07zfSISbOoMeu_pE2CAGQNTdkEmMOT9G-xmvnSPcw"
WORKSHEET_NAME = "PosSheets(zalov1)" 

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
df = df[df["ID"].notna() & (df["ID"] != "")]


# =========================
# RENAME COLUMNS
# =========================
rename_map = {
    "ID": "ID",
    "Trạng thái": "Trang_thai",
    "Thời điểm tạo đơn": "Thoi_diem_tao_don",
    "Doanh thu chưa trừ phí sàn": "Doanh_thu_chua_tru_phi_san",
    "Người xử lý": "Nguoi_xu_ly",
    "Thẻ": "The"
}

df = df.rename(columns=rename_map)

# =========================
# CHỈ GIỮ 6 CỘT CẦN THIẾT
# =========================

required_columns = [
    "ID",
    "Trang_thai",
    "Thoi_diem_tao_don",
    "Doanh_thu_chua_tru_phi_san",
    "Nguoi_xu_ly",
    "The"
]

df = df[required_columns].copy()

# =========================
# REMOVE INVALID ID + DUPLICATE WITH BIGQUERY
# =========================
# =========================
# REMOVE INVALID ID
# =========================
INVALID_IDS = {"-", "_"}

df = df[
    df["ID"].notna() &
    (~df["ID"].isin(INVALID_IDS)) &
    (df["ID"].str.strip() != "")
].copy()

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

df["Thoi_diem_tao_don"] = pd.to_datetime(
    df["Thoi_diem_tao_don"],
    errors="coerce",
    dayfirst=True
)

df["Nguoi_xu_ly"] = df["Nguoi_xu_ly"].astype(str).str.strip().replace({"nan": None, "None": None, "": None})
df["The"] = df["The"].astype(str).str.strip().replace({"nan": None, "None": None, "": None})

# =========================
# DEFINE CUTOFF
# =========================
cutoff_date = datetime(2025, 1, 1).date()
end_date = datetime(2025, 12, 31).date()
print("Cutoff date:", cutoff_date, "→", end_date)

# =========================
# FILTER: CHỈ LẤY KHOẢNG NGÀY CHỈ ĐỊNH
# =========================
if "ngay_tao_don" in df.columns:
    df = df[
        (df["ngay_tao_don"] >= cutoff_date) &
        (df["ngay_tao_don"] <= end_date)
    ]

print(f"Reloading {len(df)} rows from {cutoff_date} to {end_date}")


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
