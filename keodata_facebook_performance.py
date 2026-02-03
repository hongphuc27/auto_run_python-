import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import urllib.parse
from sqlalchemy import create_engine, text

# =========================
# 0) CONFIG - CHỈ CẦN SỬA Ở ĐÂY
# =========================
SERVICE_ACCOUNT_FILE = r"google_key.json"  # để cùng thư mục với .py hoặc ghi đường dẫn đầy đủ
SPREADSHEET_NAME = "Performance&Sales"  # tên file Google Sheet (đúng y hệt)
WORKSHEET_NAME = "PosSheets(Performance FB + Sale)"                   # tên tab sheet

SQL_SERVER = "THOMTRAN"
SQL_DATABASE = "rhysman"
SQL_SCHEMA = "dbo"
SQL_TABLE = "fact_order_sales_facebook_performace"        # bảng bạn đang dùng

CHUNKSIZE = 50  # tăng/giảm tuỳ máy


# =========================
# 1) GOOGLE SHEET -> DATAFRAME
# =========================
scopes = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
gc = gspread.authorize(creds)

sh = gc.open(SPREADSHEET_NAME)
ws = sh.worksheet(WORKSHEET_NAME)

data = ws.get_all_records()
df = pd.DataFrame(data)
print(f"Loaded {len(df)} rows from Google Sheet")

# =========================
# 2) RENAME COLUMNS (PHẢI KHỚP HEADER GOOGLE SHEET)
# =========================
rename_map = {
    # ===== ORDER =====
    "ID": "id",
    "Mã vận đơn": "ma_van_don",
    "Trạng thái": "trang_thai",
    "Trạng thái VC": "trang_thai_vc",
    "Thẻ": "the",
    "NV xác nhận": "nv_xac_nhan",
    "Người xử lý": "nguoi_xu_ly",

    # ===== CUSTOMER =====
    "Khách hàng": "khach_hang",
    "Địa chỉ": "dia_chi",
    "Số điện thoại": "so_dien_thoai",

    # ===== TIME =====
    "Thời điểm tạo đơn": "thoi_diem_tao_don",
    "Ngày tạo đơn": "ngay_tao_don",
    "Ngày tạo sản phẩm": "ngay_tao_san_pham",
    "Thời điểm cập nhật trạng thái": "thoi_diem_cap_nhat_trang_thai",
    "Thời điểm cập nhật sang đã nhận": "thoi_diem_cap_nhat_da_nhan",

    # ===== PRODUCT =====
    "Nguồn đơn": "nguon_don",
    "Sản phẩm": "san_pham",
    "Mã sản phẩm": "ma_san_pham",
    "Mã mẫu mã": "ma_mau_ma",
    "Số lượng": "so_luong",
    "Tổng số lượng SP": "tong_so_luong_sp",

    # ===== COST / PRICE =====
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

    # ===== NOTE =====
    "Ghi chú sản phẩm": "ghi_chu_san_pham",
    "Ghi chú nội bộ": "ghi_chu_noi_bo",
    "Gồm các mã sản phẩm": "gom_cac_ma_san_pham",

    # ===== INVENTORY / CURRENCY =====
    "Đơn vị tiền tệ": "don_vi_tien_te",
    "Kho hàng": "kho_hang",

    # ===== TOTAL =====
    "Tổng tiền đơn hàng": "tong_tien_don_hang",
    "Tổng tiền đơn hàng (trừ phí ship)": "tong_tien_don_hang_tru_phi_ship",
    "Tổng tiền đơn hàng (trừ chiết khấu)": "tong_tien_don_hang_tru_chiet_khau",
    "Tổng số tiền": "tong_so_tien",

    # ===== REVENUE =====
    "Doanh thu đơn hàng": "doanh_thu_don_hang",
    "Doanh thu chưa trừ phí sàn": "doanh_thu_chua_tru_phi_san",
    "Doanh thu không trừ hoàn": "doanh_thu_khong_tru_hoan",
    "Doanh thu bán hàng": "doanh_thu_ban_hang",
}



df = df.rename(columns=rename_map)

# chỉ giữ các cột đúng schema đích (tránh thừa cột gây lỗi)
expected_cols = list(rename_map.values())
df = df[[c for c in expected_cols if c in df.columns]]

# =========================
# 3) CLEAN DATA TYPES (FACT ORDER SALES DETAIL – 46 COLS)
# =========================

# =========================
# ID (bắt buộc)
# =========================
df["id"] = df["id"].astype(str).str.strip()
df = df[df["id"].notna() & (df["id"] != "")]

# =========================
# DATE / DATETIME columns
# =========================
date_cols = [
    "ngay_tao_don",
]

datetime_cols = [
    "thoi_diem_tao_don",
    "ngay_tao_san_pham",
    "thoi_diem_cap_nhat_trang_thai",
    "thoi_diem_cap_nhat_da_nhan",
]

for c in date_cols:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True).dt.date

for c in datetime_cols:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)

# =========================
# INT columns
# =========================
int_cols = [
    "so_luong",
    "tong_so_luong_sp",
]

for c in int_cols:
    if c in df.columns:
        df[c] = (
            pd.to_numeric(df[c], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

# =========================
# MONEY columns (BIGINT)
# =========================
money_cols = [
    # cost / price
    "gia_nhap_tung_sp",
    "tong_gia_nhap_sp",
    "gia_nhap_don_hang",
    "gia_sp",
    "giam_gia",
    "giam_gia_tung_san_pham",

    # order value
    "tri_gia_don_hang",
    "tri_gia_don_hang_da_chiet_khau",

    # cod / shipping
    "cod",
    "cod_doi_soat",
    "phi_tra_cho_don_vi_vc",
    "phi_vc_thu_cua_khach",

    # totals
    "tong_tien_don_hang",
    "tong_tien_don_hang_tru_phi_ship",
    "tong_tien_don_hang_tru_chiet_khau",
    "tong_so_tien",

    # revenue
    "doanh_thu_don_hang",
    "doanh_thu_chua_tru_phi_san",
    "doanh_thu_khong_tru_hoan",
    "doanh_thu_ban_hang",
]

for c in money_cols:
    if c in df.columns:
        df[c] = (
            pd.to_numeric(df[c], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

# =========================
# TEXT columns (NVARCHAR)
# =========================
text_cols = [
    # order
    "ma_van_don",
    "trang_thai",
    "trang_thai_vc",
    "the",
    "nv_xac_nhan",
    "nguoi_xu_ly",

    # customer
    "khach_hang",
    "dia_chi",
    "so_dien_thoai",

    # product / source
    "nguon_don",
    "san_pham",
    "ma_san_pham",
    "ma_mau_ma",

    # inventory
    "kho_hang",
    "don_vi_tien_te",

    # note
    "ghi_chu_san_pham",
    "ghi_chu_noi_bo",
    "gom_cac_ma_san_pham",
]

for c in text_cols:
    if c in df.columns:
        df[c] = (
            df[c]
            .astype(str)
            .str.strip()
            .replace({"nan": None, "None": None, "": None})
        )

# =========================
# Chuẩn hoá toàn bộ object còn lại: NaN -> None
# =========================
for c in df.columns:
    if df[c].dtype == "object":
        df[c] = df[c].where(pd.notna(df[c]), None)

print(f"After cleaning: {len(df)} rows to insert")


# =========================
# 4) CONNECT SQL SERVER (TRUSTED CONNECTION)
# =========================
params = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(params)
engine = create_engine(connection_string, fast_executemany=True)



# =========================
# 4.5) DELETE OLD DATA BEFORE RELOAD
# =========================

with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {SQL_SCHEMA}.{SQL_TABLE}"))


print(f"Deleted all old data from {SQL_SCHEMA}.{SQL_TABLE}")


# =========================
# 5) AUTO FIX TRUNCATION: ALTER ALL NVARCHAR/VARCHAR columns -> NVARCHAR(MAX)
#    (Đây là phần giúp bạn chắc chắn không còn lỗi truncation)
# =========================
alter_sql = f"""
DECLARE @schema SYSNAME = N'{SQL_SCHEMA}';
DECLARE @table  SYSNAME = N'{SQL_TABLE}';

IF OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table), 'U') IS NULL
BEGIN
    RAISERROR('Table %s.%s does not exist', 16, 1, @schema, @table);
    RETURN;
END;

DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql + N'
ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) +
N' ALTER COLUMN ' + QUOTENAME(c.name) + N' NVARCHAR(MAX) ' +
CASE WHEN c.is_nullable = 1 THEN N'NULL' ELSE N'NOT NULL' END + N';'
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table))
  AND t.name IN ('nvarchar', 'varchar', 'nchar', 'char')
  AND c.name <> 'ID'; -- ID thường là PK, giữ nguyên

EXEC sp_executesql @sql;
"""

with engine.begin() as conn:
    conn.execute(text(alter_sql))


# =========================
# 5.5) REMOVE DUPLICATE WITH DB (ID not in ['-', '_'])
# =========================

INVALID_IDS = {"-", "_"}

with engine.connect() as conn:
    result = conn.execute(text(f"""
        SELECT DISTINCT id
        FROM {SQL_SCHEMA}.{SQL_TABLE}
        WHERE id IS NOT NULL
          AND id NOT IN ('-', '_')
    """))
    existing_ids = {row[0] for row in result}

print(f"Existing valid IDs in DB: {len(existing_ids)}")

# Điều kiện ID hợp lệ để xét insert
mask_valid_id = (
    df["id"].notna() &
    (~df["id"].isin(INVALID_IDS))
)

# ID chưa tồn tại trong DB
mask_new_id = ~df["id"].isin(existing_ids)

# Chỉ giữ:
# - ID hợp lệ
# - ID chưa có trong DB
df = df[mask_valid_id & mask_new_id]

print(f"After deduplicate & invalid-id filter: {len(df)} rows to insert")


# =========================
# 6) INSERT INTO SQL (APPEND)
# =========================
# Lưu ý: nếu bảng có PRIMARY KEY ID và bạn insert trùng ID -> sẽ lỗi duplicate.
# Bạn đã drop_duplicates trong file, nhưng nếu DB đã có ID rồi thì vẫn trùng.
# Khi cần chống trùng với DB, mình sẽ đưa bạn bản upsert ở bước sau.

df.to_sql(
    SQL_TABLE,
    engine,
    schema="dbo",
    if_exists="append",
    index=False,
    chunksize=CHUNKSIZE   # <= QUAN TRỌNG
)


print(f"INSERT SUCCESSFULLY INTO {SQL_DATABASE}.{SQL_SCHEMA}.{SQL_TABLE}")
