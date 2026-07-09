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
SHEET_ID = "1MaIhb0FSi5cpZ0kX99dGPYem1ofH_tgqjfms5KBkKeQ"
GID = 108803117
PROJECT_ID = "rhysman-data-warehouse-488306"
TABLE_FULL_ID = f"{PROJECT_ID}.rhysman.fact_fb_zalo_si"
WINDOW_DAYS = 25
TZ = ZoneInfo("Asia/Ho_Chi_Minh")

FILL_COLS = ["ID", "Trạng thái", "Thời điểm tạo đơn"]
PLACEHOLDERS = {"-", "_", ""}

# ten cot BigQuery -> ten cot tren Google Sheet
COLUMN_MAP = {
    "id": "ID",
    "ma_van_don": "Mã vận đơn",
    "trang_thai": "Trạng thái",
    "the": "Thẻ",
    "nv_xac_nhan": "NV xác nhận",
    "nguoi_xu_ly": "Người xử lý",
    "so_dien_thoai": "Số điện thoại",
    "thoi_diem_tao_don": "Thời điểm tạo đơn",
    "thoi_diem_cap_nhat_trang_thai": "Thời điểm cập nhật trạng thái",
    "thoi_diem_cap_nhat_da_nhan": "Thời điểm cập nhật sang đã nhận",
    "nguon_don": "Nguồn đơn",
    "san_pham": "Sản phẩm",
    "ma_san_pham": "Mã sản phẩm",
    "ma_mau_ma": "Mã mẫu mã",
    "so_luong": "Số lượng",
    "gia_nhap_tung_sp": "Giá nhập từng SP",
    "tong_gia_nhap_sp": "Tổng giá nhập SP",
    "gia_nhap_don_hang": "Giá nhập đơn hàng",
    "gia_sp": "Giá SP",
    "giam_gia": "Giảm giá",
    "giam_gia_tung_san_pham": "Giảm giá cả mặt hàng",
    "tri_gia_don_hang": "Trị giá đơn hàng",
    "tri_gia_don_hang_da_chiet_khau": "Trị giá đơn hàng đã chiết khấu",
    "cod": "COD",
    "cod_doi_soat": "COD đối soát",
    "phi_tra_cho_don_vi_vc": "Phí trả cho đơn vị VC",
    "phi_vc_thu_cua_khach": "Phí VC thu của khách",
    "tong_tien_don_hang": "Tổng tiền đơn hàng",
    "tong_tien_don_hang_tru_phi_ship": "Tổng tiền đơn hàng (trừ phí ship)",
    "tong_tien_don_hang_tru_chiet_khau": "Tổng tiền đơn hàng (trừ chiết khấu)",
    "tong_so_tien": "Tổng số tiền",
    "doanh_thu_don_hang": "Doanh thu đơn hàng",
    "doanh_thu_chua_tru_phi_san": "Doanh thu chưa trừ phí sàn",
    "doanh_thu_khong_tru_hoan": "Doanh thu không trừ hoàn",
    "doanh_thu_ban_hang": "Doanh thu bán hàng",
    "kho_hang": "Kho hàng",  # sheet nguon khong co cot nay -> NULL
    "ad_id": "Ad ID",
}

INT_COLS = [
    "so_luong", "gia_nhap_tung_sp", "tong_gia_nhap_sp", "gia_nhap_don_hang",
    "gia_sp", "giam_gia", "giam_gia_tung_san_pham", "tri_gia_don_hang",
    "tri_gia_don_hang_da_chiet_khau", "cod", "cod_doi_soat",
    "phi_tra_cho_don_vi_vc", "phi_vc_thu_cua_khach", "tong_tien_don_hang",
    "tong_tien_don_hang_tru_phi_ship", "tong_tien_don_hang_tru_chiet_khau",
    "tong_so_tien", "doanh_thu_don_hang", "doanh_thu_chua_tru_phi_san",
    "doanh_thu_khong_tru_hoan", "doanh_thu_ban_hang",
]
TS_COLS = ["thoi_diem_tao_don", "thoi_diem_cap_nhat_trang_thai", "thoi_diem_cap_nhat_da_nhan"]


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
    # khong ffill tron de tranh keo nham gia tri cua don phia tren
    header = ~stripped[FILL_COLS[0]].isin(PLACEHOLDERS)
    if not header.iloc[0]:
        raise SystemExit("Dong dau tien khong phai dong dau don - du lieu nguon bi xao tron, dung lai.")
    grp = header.cumsum()
    for col in FILL_COLS:
        header_val = df[col].where(header).groupby(grp).transform("first")
        is_ph = stripped[col].isin(PLACEHOLDERS)
        df.loc[~header & is_ph, col] = header_val[~header & is_ph]
    return df, int((~header).sum()), int(junk.sum())


def to_bq_frame(df):
    out = pd.DataFrame(index=df.index)
    for bq_col, src in COLUMN_MAP.items():
        if src in df.columns:
            out[bq_col] = df[src].fillna("").str.strip().replace({"": None})
        else:
            out[bq_col] = None

    for col in INT_COLS:
        raw = out[col].str.replace(",", "", regex=False)
        out[col] = pd.to_numeric(raw, errors="coerce").round().astype("Int64")

    # gio Viet Nam, luu wall-clock (naive) de DATE(ts) trong BQ ra dung ngay VN
    for col in TS_COLS:
        out[col] = pd.to_datetime(out[col], errors="coerce", dayfirst=True)

    if out["id"].isna().any():
        raise SystemExit(f"{int(out['id'].isna().sum())} dong khong co ID sau khi fill - dung lai.")
    return out


def upsert_to_bigquery(out, cutoff):
    creds = service_account.Credentials.from_service_account_file(KEY_FILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    schema = client.get_table(TABLE_FULL_ID).schema

    client.query(
        f"DELETE FROM `{TABLE_FULL_ID}` WHERE DATE(thoi_diem_tao_don) >= DATE('{cutoff}')"
    ).result()

    client.load_table_from_dataframe(
        out, TABLE_FULL_ID,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
    ).result()
    return client.get_table(TABLE_FULL_ID).num_rows


def main():
    cutoff = datetime.now(TZ).date() - timedelta(days=WINDOW_DAYS)
    print(f"Cutoff (gio VN): {cutoff}")

    df, title = load_dataframe()
    print(f"Da doc {len(df)} dong tu sheet '{title}'")

    df, filled, junk = fill_down(df)
    print(f"Da fill {filled} dong con, bo {junk} dong trong")

    out = to_bq_frame(df)
    out = out[out["thoi_diem_tao_don"].dt.date >= cutoff]
    if out.empty:
        raise SystemExit(f"0 dong trong {WINDOW_DAYS} ngay gan nhat - nghi van sheet loi, khong dong vao BigQuery.")
    print(f"Trong cua so {WINDOW_DAYS} ngay: {len(out)} dong / {out['id'].nunique()} don")

    total = upsert_to_bigquery(out, cutoff)
    print(f"BigQuery OK: bang {TABLE_FULL_ID} hien co {total} dong")


if __name__ == "__main__":
    main()
