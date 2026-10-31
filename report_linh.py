# # -*- coding: utf-8 -*-
# """ETL: Google Sheets -> BigQuery (rhysman.fact_fb_zalo_si)

# - Doc sheet PosSheets(FB TC CS), fill xuong ID / Trang thai / Thoi diem tao don /
#   Nhan vien CSKH cho cac dong con cua don nhieu san pham (fill theo cum don, khong lan don khac).
# - Chi lay du lieu WINDOW_DAYS ngay gan nhat (theo gio VN).
# - Update BigQuery: DELETE cua so WINDOW_DAYS ngay roi APPEND du lieu moi keo ve.

# Chay tren GitHub Actions: dat secret vao file va tro bien moi truong
# GOOGLE_APPLICATION_CREDENTIALS toi file do.
# """
# import os
# from datetime import datetime, timedelta
# from zoneinfo import ZoneInfo

# import gspread
# import pandas as pd
# from google.cloud import bigquery
# from google.oauth2 import service_account

# KEY_FILE = os.environ.get(
#     "GOOGLE_APPLICATION_CREDENTIALS",
#     r"E:\RYSH MAN\rhysman-data-warehouse-488306-8db2b940e56a.json",
# )
# SHEET_ID = "1MaIhb0FSi5cpZ0kX99dGPYem1ofH_tgqjfms5KBkKeQ"
# GID = 108803117
# PROJECT_ID = "rhysman-data-warehouse-488306"
# TABLE_FULL_ID = f"{PROJECT_ID}.rhysman.fact_fb_zalo_si"
# WINDOW_DAYS = 60
# TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# FILL_COLS = ["ID", "Trạng thái", "Thời điểm tạo đơn", "Nhân viên CSKH"]
# PLACEHOLDERS = {"-", "_", ""}

# # ten cot BigQuery -> ten cot tren Google Sheet
# COLUMN_MAP = {
#     "id": "ID",
#     "ma_van_don": "Mã vận đơn",
#     "trang_thai": "Trạng thái",
#     "the": "Thẻ",
#     "nv_xac_nhan": "NV xác nhận",
#     "nguoi_xu_ly": "Người xử lý",
#     "nhan_vien_cskh": "Nhân viên CSKH",
#     "thoi_diem_tao_don": "Thời điểm tạo đơn",
#     "thoi_diem_cap_nhat_trang_thai": "Thời điểm cập nhật trạng thái",
#     "thoi_diem_cap_nhat_da_nhan": "Thời điểm cập nhật sang đã nhận",
#     "nguon_don": "Nguồn đơn",
#     "ma_san_pham": "Mã sản phẩm",
#     "ma_mau_ma": "Mã mẫu mã",
#     "so_luong": "Số lượng",
#     "gia_nhap_tung_sp": "Giá nhập từng SP",
#     "tong_gia_nhap_sp": "Tổng giá nhập SP",
#     "gia_nhap_don_hang": "Giá nhập đơn hàng",
#     "gia_sp": "Giá SP",
#     "giam_gia": "Giảm giá",
#     "giam_gia_tung_san_pham": "Giảm giá cả mặt hàng",
#     "tri_gia_don_hang": "Trị giá đơn hàng",
#     "tri_gia_don_hang_da_chiet_khau": "Trị giá đơn hàng đã chiết khấu",
#     "cod": "COD",
#     "cod_doi_soat": "COD đối soát",
#     "phi_tra_cho_don_vi_vc": "Phí trả cho đơn vị VC",
#     "phi_vc_thu_cua_khach": "Phí VC thu của khách",
#     "tong_tien_don_hang": "Tổng tiền đơn hàng",
#     "tong_tien_don_hang_tru_phi_ship": "Tổng tiền đơn hàng (trừ phí ship)",
#     "tong_tien_don_hang_tru_chiet_khau": "Tổng tiền đơn hàng (trừ chiết khấu)",
#     "tong_so_tien": "Tổng số tiền",
#     "doanh_thu_don_hang": "Doanh thu đơn hàng",
#     "doanh_thu_chua_tru_phi_san": "Doanh thu chưa trừ phí sàn",
#     "doanh_thu_khong_tru_hoan": "Doanh thu không trừ hoàn",
#     "doanh_thu_ban_hang": "Doanh thu bán hàng",
#     "ad_id": "Ad ID",
# }

# INT_COLS = [
#     "so_luong", "gia_nhap_tung_sp", "tong_gia_nhap_sp", "gia_nhap_don_hang",
#     "gia_sp", "giam_gia", "giam_gia_tung_san_pham", "tri_gia_don_hang",
#     "tri_gia_don_hang_da_chiet_khau", "cod", "cod_doi_soat",
#     "phi_tra_cho_don_vi_vc", "phi_vc_thu_cua_khach", "tong_tien_don_hang",
#     "tong_tien_don_hang_tru_phi_ship", "tong_tien_don_hang_tru_chiet_khau",
#     "tong_so_tien", "doanh_thu_don_hang", "doanh_thu_chua_tru_phi_san",
#     "doanh_thu_khong_tru_hoan", "doanh_thu_ban_hang",
# ]
# TS_COLS = ["thoi_diem_tao_don", "thoi_diem_cap_nhat_trang_thai", "thoi_diem_cap_nhat_da_nhan"]


# def load_dataframe():
#     gc = gspread.service_account(filename=KEY_FILE)
#     ws = gc.open_by_key(SHEET_ID).get_worksheet_by_id(GID)
#     if ws is None:
#         raise SystemExit(f"Khong tim thay worksheet gid={GID}")
#     rows = ws.get_all_values()
#     df = pd.DataFrame(rows[1:], columns=rows[0])
#     missing = [c for c in FILL_COLS if c not in df.columns]
#     if missing:
#         raise SystemExit(f"Sheet thieu cot {missing}. Cac cot: {list(df.columns)}")
#     return df, ws.title


# def fill_down(df):
#     stripped = df.apply(lambda c: c.fillna("").str.strip(), axis=0)

#     # bo dong trong hoan toan (dong rac cuoi sheet) truoc khi fill
#     junk = (stripped == "").all(axis=1)
#     df, stripped = df[~junk].copy(), stripped[~junk]

#     # fill theo cum don: dong con chi lay gia tri tu DONG DAU cua chinh don do,
#     # khong ffill tron de tranh keo nham gia tri cua don phia tren
#     header = ~stripped[FILL_COLS[0]].isin(PLACEHOLDERS)
#     if not header.iloc[0]:
#         raise SystemExit("Dong dau tien khong phai dong dau don - du lieu nguon bi xao tron, dung lai.")
#     grp = header.cumsum()
#     for col in FILL_COLS:
#         header_val = df[col].where(header).groupby(grp).transform("first")
#         is_ph = stripped[col].isin(PLACEHOLDERS)
#         df.loc[~header & is_ph, col] = header_val[~header & is_ph]
#     return df, int((~header).sum()), int(junk.sum())


# def to_bq_frame(df):
#     out = pd.DataFrame(index=df.index)
#     for bq_col, src in COLUMN_MAP.items():
#         if src in df.columns:
#             out[bq_col] = df[src].fillna("").str.strip().replace({"": None})
#         else:
#             out[bq_col] = None

#     for col in INT_COLS:
#         raw = out[col].str.replace(",", "", regex=False)
#         out[col] = pd.to_numeric(raw, errors="coerce").round().astype("Int64")

#     # gio Viet Nam, luu wall-clock (naive) de DATE(ts) trong BQ ra dung ngay VN
#     for col in TS_COLS:
#         out[col] = pd.to_datetime(out[col], errors="coerce", dayfirst=True)

#     if out["id"].isna().any():
#         raise SystemExit(f"{int(out['id'].isna().sum())} dong khong co ID sau khi fill - dung lai.")
#     return out


# def upsert_to_bigquery(out, cutoff):
#     creds = service_account.Credentials.from_service_account_file(KEY_FILE)
#     client = bigquery.Client(project=PROJECT_ID, credentials=creds)
#     schema = client.get_table(TABLE_FULL_ID).schema

#     client.query(
#         f"DELETE FROM `{TABLE_FULL_ID}` WHERE DATE(thoi_diem_tao_don) >= DATE('{cutoff}')"
#     ).result()

#     client.load_table_from_dataframe(
#         out, TABLE_FULL_ID,
#         job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
#     ).result()
#     return client.get_table(TABLE_FULL_ID).num_rows


# def main():
#     cutoff = datetime.now(TZ).date() - timedelta(days=WINDOW_DAYS)
#     print(f"Cutoff (gio VN): {cutoff}")

#     df, title = load_dataframe()
#     print(f"Da doc {len(df)} dong tu sheet '{title}'")

#     df, filled, junk = fill_down(df)
#     print(f"Da fill {filled} dong con, bo {junk} dong trong")

#     out = to_bq_frame(df)
#     out = out[out["thoi_diem_tao_don"].dt.date >= cutoff]
#     if out.empty:
#         raise SystemExit(f"0 dong trong {WINDOW_DAYS} ngay gan nhat - nghi van sheet loi, khong dong vao BigQuery.")
#     print(f"Trong cua so {WINDOW_DAYS} ngay: {len(out)} dong / {out['id'].nunique()} don")

#     total = upsert_to_bigquery(out, cutoff)
#     print(f"BigQuery OK: bang {TABLE_FULL_ID} hien co {total} dong")


# if __name__ == "__main__":
#     main()


# -*- coding: utf-8 -*-
"""ETL: Pancake POS API -> BigQuery (rhysman.fact_fb_zalo_si)

Thay script Google Sheets (PosSheets FB TC CS) -> BigQuery, giu nguyen cau truc bang:
- Moi dong = don x san pham, theo thu tu items cua API. ID / Trang thai / Thoi diem tao don /
  Nhan vien CSKH co o moi dong; cot cap don (tien, nguon, the...) chi o dong san pham dau tien.
- O chu cat khoang trang dau / cuoi ca o, o trong -> NULL; gio Viet Nam cat toi phut.
- Chi lay WINDOW_DAYS ngay gan nhat theo ngay tao don (gio VN); don cu hon giu nguyen tren bang.
- 7 nguon goc: 6 nguon nhu Sheet + Qua tang Doanh Nghiep.

MAC DINH CHAY THU: keo API, dung dong, kiem tra - KHONG dong vao BigQuery.
Chi ghi khi chay voi --write: nap vao bang tam, roi DELETE cua so + INSERT trong 1 transaction
(loi giua chung -> bang giu nguyen, khong bi trong nhu cach DELETE roi LOAD).

Chan truoc khi ghi (dung lai, bang giu nguyen, job bao loi):
- API tra thieu don so voi total_entries, hoac so don < MIN_RATIO x so don dang co trong cua so
- dong thieu ID / trang thai / thoi diem tao don; don khong co dung 1 dong cap don
- schema bang (ten + kieu cot) khac script
Sau khi ghi: dem lai so dong / so don trong cua so tren bang phai bang so vua nap.

Bien moi truong:
  POS_API_KEY                     key Pancake POS (bat buoc)
  GOOGLE_APPLICATION_CREDENTIALS  file service account (chi can khi --write)

Vi du:
  python etl_pancake_api_fact_fb_zalo_si.py                  # chay thu 60 ngay
  python etl_pancake_api_fact_fb_zalo_si.py --out rows.csv   # chay thu, luu dong ra file de xem
  python etl_pancake_api_fact_fb_zalo_si.py --write          # ghi that (chi khi da duyet)
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

SHOP_ID = 120214703
POS_URL = f"https://pos.pages.fm/api/v1/shops/{SHOP_ID}"
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET = "rhysman"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET}.fact_fb_zalo_si"
WINDOW_DAYS = 60
PAGE_SIZE = 200
TZ = ZoneInfo("Asia/Ho_Chi_Minh")
# --write dung lai neu so don moi < MIN_RATIO x so don dang co trong cua so (nghi API tra thieu)
MIN_RATIO = 0.9

# ma nguon goc Pancake -> ten (order_sources / order_sources_name)
SOURCES = {
    "-1": "Facebook",
    "-7": "Webcake",
    "-16": "Woocommerce",
    "716020434": "CSKH",
    "716020016": "Sỉ Ngoại Sàn",
    "-11": "Instagram",
    "308025394": "Quà tặng Doanh Nghiệp",
}
# nguon chi ghi ten nguon goc, khong kem " / ten trang"
SOURCE_ONLY = {"CSKH", "Sỉ Ngoại Sàn"}

# ma trang thai Pancake -> nhan nhu Sheet
STATUS = {
    0: "Mới", 1: "Đã xác nhận", 2: "Đã gửi hàng", 3: "Đã nhận", 4: "Đang hoàn", 5: "Đã hoàn",
    6: "Đã huỷ", 8: "Đang đóng hàng", 9: "Chờ chuyển hàng", 11: "Chờ hàng",
}

# dung thu tu cot cua bang
COLUMNS = [
    ("id", "STRING"), ("ma_van_don", "STRING"), ("trang_thai", "STRING"), ("the", "STRING"),
    ("nv_xac_nhan", "STRING"), ("nguoi_xu_ly", "STRING"), ("nhan_vien_cskh", "STRING"),
    ("thoi_diem_tao_don", "DATETIME"), ("thoi_diem_cap_nhat_trang_thai", "DATETIME"),
    ("thoi_diem_cap_nhat_da_nhan", "DATETIME"), ("nguon_don", "STRING"), ("ma_san_pham", "STRING"),
    ("ma_mau_ma", "STRING"), ("so_luong", "INTEGER"), ("gia_nhap_tung_sp", "INTEGER"),
    ("tong_gia_nhap_sp", "INTEGER"), ("gia_nhap_don_hang", "INTEGER"), ("gia_sp", "INTEGER"),
    ("giam_gia", "INTEGER"), ("giam_gia_tung_san_pham", "INTEGER"), ("tri_gia_don_hang", "INTEGER"),
    ("tri_gia_don_hang_da_chiet_khau", "INTEGER"), ("cod", "INTEGER"), ("cod_doi_soat", "INTEGER"),
    ("phi_tra_cho_don_vi_vc", "INTEGER"), ("phi_vc_thu_cua_khach", "INTEGER"),
    ("tong_tien_don_hang", "INTEGER"), ("tong_tien_don_hang_tru_phi_ship", "INTEGER"),
    ("tong_tien_don_hang_tru_chiet_khau", "INTEGER"), ("tong_so_tien", "INTEGER"),
    ("doanh_thu_don_hang", "INTEGER"), ("doanh_thu_chua_tru_phi_san", "INTEGER"),
    ("doanh_thu_khong_tru_hoan", "INTEGER"), ("doanh_thu_ban_hang", "INTEGER"), ("ad_id", "STRING"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]
TYPES = dict(COLUMNS)
EVERY_COLS = ["id", "trang_thai", "nhan_vien_cskh", "thoi_diem_tao_don"]
ITEM_COLS = ["ma_san_pham", "ma_mau_ma", "so_luong", "gia_nhap_tung_sp", "tong_gia_nhap_sp", "gia_sp",
             "giam_gia_tung_san_pham"]
ORDER_COLS = [c for c in COLUMN_NAMES if c not in EVERY_COLS and c not in ITEM_COLS]


# ---------------- Pancake POS API ----------------
def pos_get(path, params, tries=6):
    key = os.environ.get("POS_API_KEY")
    if not key:
        raise SystemExit("Thieu bien moi truong POS_API_KEY")
    url = f"{POS_URL}{path}?{urlencode({**params, 'api_key': key})}"
    for i in range(tries + 1):
        try:
            with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=60) as r:
                body = json.loads(r.read().decode("utf-8"))
            if body.get("success") is False:
                raise SystemExit(f"API Pancake bao loi {path}: {str(body)[:300]}")
            return body
        except HTTPError as e:
            # khong in url (co key)
            if e.code != 429 and e.code < 500:
                raise SystemExit(f"API Pancake HTTP {e.code} {path}") from None
            err = f"HTTP {e.code}"
        except (URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            err = type(e).__name__
        if i == tries:
            raise SystemExit(f"API Pancake loi sau {tries} lan thu lai ({err}) {path}")
        print(f"  thu lai {i + 1}/{tries} ({err})")
        time.sleep(3 * (i + 1))


def fetch_orders(start, end):
    """Moi don co ngay tao trong [start, end) (datetime co mui gio). Kiem du so don API bao."""
    params = {"page_size": PAGE_SIZE, "updateStatus": "inserted_at",
              "startDateTime": int(start.timestamp()), "endDateTime": int(end.timestamp())}
    for attempt in (1, 2):
        orders, expected, page = {}, None, 1
        while True:
            r = pos_get("/orders", {**params, "page_number": page})
            if expected is None:
                expected = r.get("total_entries")
            for o in r.get("data") or []:
                orders[o["id"]] = o
            total_pages = r.get("total_pages") or 0
            if page % 20 == 0 or page >= total_pages:
                print(f"  trang {page}/{total_pages}")
            if page >= total_pages:
                break
            page += 1
        if expected is None or len(orders) >= expected:
            return list(orders.values())
        print(f"  API bao {expected} don nhung keo duoc {len(orders)} - keo lai ({attempt}/2)")
    raise SystemExit(f"Keo thieu don: API bao {expected}, keo duoc {len(orders)} - dung lai, khong dong vao BigQuery.")


# ---------------- API -> dong cua bang ----------------
def trim(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def vn_minute(s):
    """Gio UTC cua API -> gio Viet Nam, cat toi phut (nhu Sheet)."""
    if not s:
        return None
    return (datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=7)).replace(second=0)


def num(v):
    return int(round(v)) if v is not None else 0


def editor_name(h):
    return trim((h.get("editor") or {}).get("name") or h.get("name"))


unknown_status = {}


def status_label(o):
    s = o.get("status")
    if o.get("is_abandoned_order") and s == 0:
        return "Chưa hoàn tất"
    if s == 0 and trim(o.get("order_sources_name")) == "Webcake":
        return "Đơn Webcake"
    if s in STATUS:
        return STATUS[s]
    key = f"{s} {o.get('status_name')}"
    unknown_status[key] = unknown_status.get(key, 0) + 1
    return o.get("status_name") or str(s)


def source_label(o):
    s = trim(o.get("order_sources_name"))
    if not s or s in SOURCE_ONLY:
        return s
    a = trim(o.get("account_name"))
    return f"{s} / {a}" if a else s


def order_cols(o):
    sh = o.get("status_history") or []
    conf = next((h for h in sh if h.get("status") in (1, 11) and editor_name(h)), None)
    recv = [h for h in sh if h.get("status") == 3]
    items = o.get("items") or []
    tp, disc, ship = num(o.get("total_price")), num(o.get("total_discount")), num(o.get("shipping_fee"))
    tax, sur, st = num(o.get("tax")), num(o.get("surcharge")), o.get("status")
    rev = tp - disc + ship + sur
    # don hoan: doanh thu don hang = phi ship khach tra; huy = 0
    rev_order = 0 if st == 6 else ship if st in (4, 5) else rev
    partner = o.get("partner") or None
    return {
        "ma_van_don": trim((partner or {}).get("extend_code")),
        "the": ", ".join(str(t.get("name") or "") for t in (o.get("tags") or [])),
        "nv_xac_nhan": editor_name(conf) if conf else None,
        "nguoi_xu_ly": trim((o.get("assigning_seller") or {}).get("name")),
        # chua doi trang thai lan nao -> gio tao don
        "thoi_diem_cap_nhat_trang_thai": vn_minute(sh[-1].get("updated_at") if len(sh) > 1 else o.get("inserted_at")),
        "thoi_diem_cap_nhat_da_nhan": vn_minute(recv[-1].get("updated_at")) if recv else None,
        "nguon_don": source_label(o),
        "gia_nhap_don_hang": sum(num((i.get("variation_info") or {}).get("last_imported_price")) * num(i.get("quantity"))
                                 for i in items),
        "giam_gia": disc,
        "tri_gia_don_hang": tp,
        "tri_gia_don_hang_da_chiet_khau": tp - disc,
        # Sheet khong co COD am
        "cod": max(0, num(o.get("cod"))),
        "cod_doi_soat": num(partner["cod"]) if partner and partner.get("cod") is not None else None,
        "phi_tra_cho_don_vi_vc": num(o.get("partner_fee")),
        "phi_vc_thu_cua_khach": ship,
        # don si co VAT: tong tien gom thue; phu thu cong vao tong tien + doanh thu (tru doanh thu ban hang)
        "tong_tien_don_hang": tp + tax + ship + sur,
        "tong_tien_don_hang_tru_phi_ship": tp + tax + sur,
        "tong_tien_don_hang_tru_chiet_khau": num(o.get("total_price_after_sub_discount")),
        "tong_so_tien": tp + sur,
        "doanh_thu_don_hang": rev_order,
        "doanh_thu_chua_tru_phi_san": rev_order,
        "doanh_thu_khong_tru_hoan": 0 if st == 6 else rev,
        "doanh_thu_ban_hang": 0 if st in (4, 5, 6) else rev - sur,
        "ad_id": trim(o.get("ad_id")),
    }


def item_cols(i):
    if i is None:
        return {c: None for c in ITEM_COLS}
    v = i.get("variation_info") or {}
    qty, cost = num(i.get("quantity")), num(v.get("last_imported_price"))
    return {
        "ma_san_pham": v.get("product_display_id"),
        "ma_mau_ma": v.get("display_id"),
        "so_luong": qty,
        "gia_nhap_tung_sp": cost,
        "tong_gia_nhap_sp": cost * qty,
        "gia_sp": num(v.get("retail_price")) * qty,
        "giam_gia_tung_san_pham": num(i.get("total_discount")),
    }


def order_rows(o):
    every = {"id": o["id"], "trang_thai": status_label(o), "nhan_vien_cskh": trim((o.get("assigning_care") or {}).get("name")),
             "thoi_diem_tao_don": vn_minute(o.get("inserted_at"))}
    oc = order_cols(o)
    empty = {c: None for c in ORDER_COLS}
    rows = []
    for k, it in enumerate(o.get("items") or [None]):
        r = {**every, **item_cols(it), **(oc if k == 0 else empty)}
        # nhu Sheet: o chu cat khoang trang dau / cuoi, o trong -> NULL
        rows.append({c: trim(r[c]) if TYPES[c] == "STRING" else r[c] for c in COLUMN_NAMES})
    return rows


def build_rows(orders):
    kept = [o for o in orders if str(o.get("order_sources")) in SOURCES]
    other_named = [o for o in orders if str(o.get("order_sources")) not in SOURCES
                   and trim(o.get("order_sources_name")) in SOURCES.values()]
    if other_named:
        print(f"CANH BAO: {len(other_named)} don co ten nguon trong danh sach nhung ma nguon khac - bo qua, vd {other_named[0]['id']}")
    rows = [r for o in kept for r in order_rows(o)]
    return rows, kept


def to_frame(rows):
    df = pd.DataFrame(rows, columns=COLUMN_NAMES)
    for c, t in COLUMNS:
        if t == "INTEGER":
            df[c] = pd.array(df[c].tolist(), dtype="Int64")
        elif t == "DATETIME":
            df[c] = pd.to_datetime(df[c])
        else:
            df[c] = df[c].astype(object).where(df[c].notna(), None)
    return df


def check_frame(df):
    if df.empty:
        raise SystemExit("0 dong - nghi API loi, khong dong vao BigQuery.")
    for c in ("id", "trang_thai", "thoi_diem_tao_don"):
        if df[c].isna().any():
            raise SystemExit(f"{int(df[c].isna().sum())} dong thieu {c} - dung lai.")
    # moi don dung 1 dong cap don (dong san pham dau tien co nguon_don)
    per_order = df.groupby("id")["nguon_don"].count()
    bad = per_order[per_order != 1]
    if len(bad):
        raise SystemExit(f"{len(bad)} don khong co dung 1 dong cap don (vd {bad.index[0]}) - dung lai.")


# ---------------- BigQuery (chi khi --write) ----------------
def write_bigquery(df, cutoff, force=False):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    creds = service_account.Credentials.from_service_account_file(key_file) if key_file else None
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    table = client.get_table(TABLE_FULL_ID)
    have = [(f.name, f.field_type) for f in table.schema]
    if have != COLUMNS:
        diff = [f"{h} != {w}" for h, w in zip(have, COLUMNS) if h != w] or [f"{len(have)} cot != {len(COLUMNS)} cot"]
        raise SystemExit(f"Schema bang khac script ({'; '.join(diff[:3])}) - dung lai.")

    count_sql = (f"SELECT COUNT(*) dong, COUNT(DISTINCT id) don FROM `{TABLE_FULL_ID}` "
                 f"WHERE DATE(thoi_diem_tao_don) >= DATE('{cutoff}')")
    cur = next(iter(client.query(count_sql).result())).don
    new = int(df["id"].nunique())
    print(f"So don cua so: bang dang co {cur}, API {new}")
    if cur and new < MIN_RATIO * cur and not force:
        raise SystemExit(f"So don API ({new}) < {MIN_RATIO:.0%} so don dang co ({cur}) - dung lai (them --force neu chac chan).")

    # bang tam tu het han sau 1 gio, lo job chet giua chung cung khong de rac trong dataset
    stg = bigquery.Table(f"{PROJECT_ID}.{DATASET}._stg_fact_fb_zalo_si_api_{uuid4().hex[:12]}", schema=table.schema)
    stg.expires = datetime.now(TZ) + timedelta(hours=1)
    stg = client.create_table(stg)
    try:
        client.load_table_from_dataframe(
            df, stg, job_config=bigquery.LoadJobConfig(schema=table.schema, write_disposition="WRITE_APPEND"),
        ).result()
        client.query(replace_sql(f"{stg.project}.{stg.dataset_id}.{stg.table_id}", cutoff)).result()
    finally:
        client.delete_table(stg, not_found_ok=True)

    after = next(iter(client.query(count_sql).result()))
    print(f"Sau khi ghi: cua so co {after.don} don / {after.dong} dong")
    if (after.dong, after.don) != (len(df), new):
        raise SystemExit(f"Bang sau khi ghi ({after.don} don / {after.dong} dong) khac so vua nap ({new} / {len(df)}) "
                         "- kiem tra co job khac dang ghi bang khong (script Sheet cu?).")
    return client.get_table(TABLE_FULL_ID).num_rows


def replace_sql(stg, cutoff):
    """Xoa cua so + nap lai trong 1 transaction; loi bat ky -> rollback, bang giu nguyen, job bao loi."""
    cols = ", ".join(COLUMN_NAMES)
    return f"""
BEGIN
  BEGIN TRANSACTION;
  DELETE FROM `{TABLE_FULL_ID}` WHERE DATE(thoi_diem_tao_don) >= DATE('{cutoff}');
  INSERT INTO `{TABLE_FULL_ID}` ({cols}) SELECT {cols} FROM `{stg}`;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = @@error.message;
END;
"""


# ---------------- chay ----------------
def window(days=WINDOW_DAYS, since=None):
    now = datetime.now(TZ)
    cutoff = since or (now.date() - timedelta(days=days))
    return cutoff, datetime(cutoff.year, cutoff.month, cutoff.day, tzinfo=TZ), now


def summarize(kept, df):
    by_src = {}
    for o in kept:
        k = SOURCES[str(o.get("order_sources"))]
        by_src[k] = by_src.get(k, 0) + 1
    print("Don theo nguon:", ", ".join(f"{k} {v}" for k, v in sorted(by_src.items(), key=lambda x: -x[1])))
    print("Trang thai:", df.drop_duplicates("id")["trang_thai"].value_counts().to_dict())
    if unknown_status:
        print("CANH BAO: trang thai chua co nhan (giu ten Pancake):", unknown_status)


def main():
    ap = argparse.ArgumentParser(description="Pancake POS API -> rhysman.fact_fb_zalo_si (mac dinh chay thu)")
    ap.add_argument("--days", type=int, default=WINDOW_DAYS, help="so ngay gan nhat (mac dinh 60)")
    ap.add_argument("--since", help="tu ngay YYYY-MM-DD (thay --days, de kiem tra)")
    ap.add_argument("--out", help="chay thu: luu dong ra file CSV de xem")
    ap.add_argument("--write", action="store_true", help="GHI THAT vao BigQuery")
    ap.add_argument("--force", action="store_true", help="bo qua chan so don tut manh")
    a = ap.parse_args()

    since = datetime.strptime(a.since, "%Y-%m-%d").date() if a.since else None
    cutoff, start, now = window(a.days, since)
    print(f"Cua so (gio VN): {cutoff} -> {now:%Y-%m-%d %H:%M}")
    t0 = time.time()
    orders = fetch_orders(start, now)
    rows, kept = build_rows(orders)
    df = to_frame(rows)
    check_frame(df)
    print(f"API: {len(orders)} don moi nguon, giu {len(kept)} don / {len(df)} dong ({time.time() - t0:.0f} giay)")
    summarize(kept, df)

    if a.out:
        df.to_csv(a.out, index=False, encoding="utf-8-sig")
        print(f"Da luu {a.out}")
    if not a.write:
        print("CHAY THU - khong ghi BigQuery (them --write de ghi).")
        return
    if a.since or a.days != WINDOW_DAYS:
        raise SystemExit("--write chi chay voi cua so mac dinh 60 ngay.")
    total = write_bigquery(df, cutoff, a.force)
    print(f"BigQuery OK: bang {TABLE_FULL_ID} hien co {total} dong")


if __name__ == "__main__":
    main()
