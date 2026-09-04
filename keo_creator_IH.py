# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# TikTok Seller — NGƯỜI NHẬN HOA HỒNG theo đơn, nạp BigQuery fact_creator_tiktok.

# Vì sao có script này: job affiliate cũ ngừng ghi nhận 3 tài khoản nhà (rhysman.com,
# rhysman.shopping, rhysman_channel) từ 29/06/2026, trong khi TikTok VẪN trả dữ liệu.
# Script này lấy thẳng từ API danh sách đơn của Seller Center.

# NGUỒN DỮ LIỆU (quan trọng, đừng đổi sang file export):
#   Người nhận hoa hồng nằm ở  main_orders[].sku_module[].creator_info_name.items[]
#   phần tử position=3 -> "Người nhận hoa hồng: <handle>".
#   Cột "Creator Handle" trong file export .xlsx KHÔNG phải trường này — nó để trống
#   cho toàn bộ đơn Thẻ sản phẩm, thiếu ~27% dữ liệu. Đã kiểm chứng 06/08/2026.

#   promotion_position_type lấy bằng cách lọc sale_source (mỗi đơn thuộc đúng 1 kênh,
#   đã đối chiếu 501/501 đơn ngày 05/08/2026 khớp với cột Order Channel của file export):
#       sale_source 1 = LIVE          -> promotion_position_type 3
#       sale_source 2 = Video         -> promotion_position_type 2
#       sale_source 3 = Thẻ sản phẩm  -> promotion_position_type 1

# VÍ DỤ:
#   py -X utf8 keo_creator_daily.py --last-days 2
#   py -X utf8 keo_creator_daily.py --date 2026-08-05 --no-bq --csv out.csv
#   py -X utf8 keo_creator_daily.py --from 2026-06-29 --to 2026-07-31   # backfill
#   py -X utf8 keo_creator_daily.py --last-days 2 --cookie cookie_shop2.txt

# Idempotent: DELETE đúng (id_shop x creator_username x khoảng create_time) rồi INSERT.
# Chạy lại bao nhiêu lần cũng không nhân đôi.
# """
# import argparse, datetime, json, os, re, sys, time, urllib.parse, urllib.request, urllib.error

# # ---------------------------------------------------------------- cấu hình
# BASE = "https://seller-vn.tiktok.com"
# API = BASE + "/api/fulfillment/order/list"
# COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
# ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
# TZ = datetime.timezone(datetime.timedelta(hours=7))

# BQ_PROJECT = "rhysman-data-warehouse-488306"
# BQ_DATASET = "rhysman"
# BQ_TABLE = "fact_creator_tiktok"

# # Chỉ nạp các creator biết chắc nickname. API chỉ trả username, không trả nickname —
# # thêm tên mới thì phải bổ sung ở đây, nếu không sẽ bị bỏ qua (có cảnh báo).
# CREATORS = {
#     "rhysman.com": "Rhys Man Chính Hãng",
#     "rhysman.shopping": "Rhys Man",
#     "rhysman_channel": "Rhys Man Chăm Sóc Cơ Thể",
# }

# # sale_source (API) -> promotion_position_type (BigQuery)
# SALE_SOURCE_TO_POSITION = {"1": 3, "2": 2, "3": 1}
# SALE_SOURCE_TEN = {"1": "LIVE", "2": "Video", "3": "Thẻ sản phẩm"}

# RE_HOA_HONG = re.compile(r"hoa hồng:\s*(\S+)")
# PAGE_SIZE = 100
# MAX_PAGES = 400


# def log(*a):
#     print(datetime.datetime.now(TZ).strftime("%H:%M:%S"), *a, flush=True)


# # ---------------------------------------------------------------- cookie
# def doc_cookie(path):
#     """Ưu tiên biến môi trường TIKTOK_COOKIE (secret trên GitHub Actions), không có
#     thì đọc file. Nhờ vậy trên Actions chỉ cần thay secret, không phải ghi file."""
#     ck = (os.environ.get(ENV_COOKIE) or "").strip()
#     nguon = f"biến môi trường {ENV_COOKIE}"
#     if not ck:
#         if not os.path.exists(path):
#             sys.exit(f"[COOKIE] không có biến môi trường {ENV_COOKIE}, cũng không thấy file "
#                      f"{path}.\nMở Seller Center -> F12 -> Network -> copy giá trị header "
#                      f"cookie của một request bất kỳ, rồi đặt vào {ENV_COOKIE} hoặc {path}.")
#         ck = open(path, encoding="utf-8").read().strip()
#         nguon = f"file {path}"
#     if not ck:
#         sys.exit(f"[COOKIE] {nguon} rỗng.")
#     # phòng trường hợp dán nhiều dòng — cookie phải nằm trên một dòng
#     ck = ck.replace("\r", "").replace("\n", "")
#     log(f"cookie đọc từ {nguon} ({len(ck)} ký tự)")
#     m = re.search(r"oec_seller_id_unified_seller_env=(\d+)", ck)
#     if not m:
#         m = re.search(r"(?:^|;\s*)SHOP_ID=(\d+)", ck)
#     if not m:
#         sys.exit("[COOKIE] không đọc được seller_id (oec_seller_id_unified_seller_env) từ cookie.")
#     return ck, m.group(1)


# def common_params(seller_id):
#     return {
#         "locale": "vi-VN", "language": "vi-VN",
#         "oec_seller_id": seller_id, "seller_id": seller_id,
#         "aid": "4068", "app_name": "i18n_ecom_shop",
#         "device_platform": "web", "cookie_enabled": "true",
#     }


# def post(url, cookie, body, tries=6):
#     """Gọi API, retry khi lỗi mạng. Cookie chết thì dừng hẳn, retry vô nghĩa."""
#     data = json.dumps(body).encode()
#     headers = {
#         "content-type": "application/json",
#         "accept": "*/*",
#         "cookie": cookie,
#         "origin": BASE,
#         "referer": BASE + "/order",
#         "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                       "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
#         "x-tt-oec-region": "VN",
#     }
#     for k in range(tries):
#         try:
#             req = urllib.request.Request(url, data=data, headers=headers, method="POST")
#             with urllib.request.urlopen(req, timeout=90) as r:
#                 js = json.loads(r.read())
#             code = js.get("code")
#             if code == 0:
#                 return js
#             # 98001002 = "Bạn phải đăng nhập"; order/list trả 10000 khi phiên chết.
#             # Retry mấy code này vô nghĩa, thoát luôn cho nhanh và rõ.
#             if code in (98001002, 98001008, 10000):
#                 sys.exit(f"[COOKIE] TikTok trả code {code}"
#                          f"{': ' + str(js.get('message')) if js.get('message') else ''}. "
#                          f"Gần như chắc chắn cookie đã hết hạn — lấy cookie mới rồi chạy lại. "
#                          f"(Nếu vừa đổi tham số tìm kiếm thì kiểm tra lại tham số trước.)")
#             if k == tries - 1:
#                 sys.exit(f"[API] code={code} message={js.get('message')}")
#         except SystemExit:
#             raise
#         except Exception as e:
#             if k == tries - 1:
#                 raise
#             log(f"    retry {k+1} ({e})")
#         time.sleep(2 * (k + 1))


# # ---------------------------------------------------------------- kéo dữ liệu
# def keo_mot_kenh(cookie, seller_id, sale_source, t0, t1):
#     """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh."""
#     url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
#     cond = {
#         "order_source": {"value": ["1"]},
#         "time_order_created": {"value": [str(t0), str(t1)]},
#         "sale_source": {"value": [sale_source]},
#     }
#     out, cursor, pages, total = [], "", 0, None
#     while pages < MAX_PAGES:
#         body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
#                 "search_cursor": cursor, "search_condition": {"condition_list": cond}}
#         js = post(url, cookie, body)
#         d = js["data"]
#         total = d.get("total_count")
#         orders = d.get("main_orders") or []
#         if not orders:
#             break
#         for o in orders:
#             oid = o.get("main_order_id")
#             t = (o.get("trade_order_module") or {}).get("create_time")
#             seen = set()
#             for sku in (o.get("sku_module") or []):
#                 for it in ((sku.get("creator_info_name") or {}).get("items") or []):
#                     m = RE_HOA_HONG.search(it.get("message_content") or "")
#                     if m:
#                         seen.add(m.group(1))
#             for c in seen:
#                 out.append((oid, int(t), c))
#         cursor = d.get("search_next_cursor") or ""
#         pages += 1
#         if not d.get("search_next_has_more") or not cursor:
#             break
#     log(f"  sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
#         f"total_count={total}, {pages} trang, {len(out)} cặp đơn-creator")
#     return out


# def keo(cookie, seller_id, t0, t1):
#     rows, la = {}, set()
#     for ss in ("1", "2", "3"):
#         for oid, t, cu in keo_mot_kenh(cookie, seller_id, ss, t0, t1):
#             if cu not in CREATORS:
#                 la.add(cu)
#                 continue
#             key = (oid, cu)
#             pos = SALE_SOURCE_TO_POSITION[ss]
#             if key in rows:
#                 if rows[key]["promotion_position_type"] != pos:
#                     log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
#                         f"({rows[key]['promotion_position_type']} và {pos}) — giữ kênh đầu")
#                 continue
#             rows[key] = {
#                 "main_order_id": int(oid),
#                 "creator_nickname": CREATORS[cu],
#                 "creator_username": cu,
#                 "promotion_position_type": pos,
#                 "create_time": datetime.datetime.fromtimestamp(
#                     t, datetime.timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
#                 "cos_ratio": None,
#                 "estimated_cos_fee": None,
#                 "shop_ads_commission_ratio": None,
#                 "estimated_shop_ads_commission": None,
#                 "id_shop": seller_id,
#             }
#     if la:
#         log(f"  (bỏ qua {len(la)} creator ngoài danh sách CREATORS, vd: {sorted(la)[:5]})")
#     return sorted(rows.values(), key=lambda r: r["create_time"])


# # ---------------------------------------------------------------- BigQuery
# def bq_client():
#     """Giống các job khác trong repo: ưu tiên key file, rồi JSON inline trong biến
#     môi trường (secret GOOGLE_SERVICE_ACCOUNT_JSON), cuối cùng mới dùng ADC."""
#     from google.cloud import bigquery
#     key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
#     key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

#     if key_path and os.path.exists(key_path):
#         from google.oauth2 import service_account
#         cred = service_account.Credentials.from_service_account_file(key_path)
#         log(f"  BigQuery: key file {key_path}")
#         return bigquery.Client(project=BQ_PROJECT, credentials=cred)
#     if key_json:
#         from google.oauth2 import service_account
#         try:
#             info = json.loads(key_json)
#         except json.JSONDecodeError as e:
#             sys.exit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
#         cred = service_account.Credentials.from_service_account_info(info)
#         log(f"  BigQuery: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email','?')})")
#         return bigquery.Client(project=BQ_PROJECT, credentials=cred)
#     log("  BigQuery: credentials mặc định của môi trường (ADC)")
#     return bigquery.Client(project=BQ_PROJECT)


# def nap_bq(rows, seller_id, utc0, utc1):
#     from google.cloud import bigquery
#     client = bq_client()
#     table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

#     cfg = bigquery.QueryJobConfig(query_parameters=[
#         bigquery.ScalarQueryParameter("shop", "STRING", seller_id),
#         bigquery.ArrayQueryParameter("creators", "STRING", list(CREATORS)),
#         bigquery.ScalarQueryParameter("t0", "DATETIME", utc0),
#         bigquery.ScalarQueryParameter("t1", "DATETIME", utc1),
#     ])
#     sql = f"""
#         DELETE FROM `{table_id}`
#         WHERE id_shop = @shop
#           AND creator_username IN UNNEST(@creators)
#           AND create_time >= @t0 AND create_time <= @t1
#     """
#     job = client.query(sql, job_config=cfg)
#     job.result()
#     log(f"  DELETE xong: {job.num_dml_affected_rows} dòng cũ")

#     job = client.load_table_from_json(
#         rows, table_id,
#         job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
#     job.result()
#     if job.errors:
#         sys.exit(f"[BQ] lỗi nạp: {job.errors}")
#     log(f"  INSERT xong: {len(rows)} dòng -> {table_id}")


# # ---------------------------------------------------------------- main
# def main():
#     ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery")
#     g = ap.add_mutually_exclusive_group()
#     g.add_argument("--date", help="một ngày YYYY-MM-DD (giờ VN)")
#     g.add_argument("--last-days", type=int, default=41,
#                    help="N ngày gần nhất tính cả hôm nay (mặc định 2)")
#     ap.add_argument("--from", dest="d_from", help="từ ngày YYYY-MM-DD")
#     ap.add_argument("--to", dest="d_to", help="đến ngày YYYY-MM-DD")
#     ap.add_argument("--cookie", default=COOKIE,
#                     help=f"file cookie (mặc định {COOKIE}); bị bỏ qua nếu đã có "
#                          f"biến môi trường {ENV_COOKIE}")
#     ap.add_argument("--csv", help="ghi thêm ra file CSV để kiểm tra")
#     ap.add_argument("--no-bq", action="store_true", help="không đụng BigQuery")
#     ap.add_argument("--min-rows", type=int, default=1,
#                     help="ít hơn số này thì DỪNG, không xoá gì (chống wipe)")
#     a = ap.parse_args()

#     if a.d_from or a.d_to:
#         if not (a.d_from and a.d_to):
#             sys.exit("--from và --to phải đi cùng nhau.")
#         d0 = datetime.date.fromisoformat(a.d_from)
#         d1 = datetime.date.fromisoformat(a.d_to)
#     elif a.date:
#         d0 = d1 = datetime.date.fromisoformat(a.date)
#     else:
#         d1 = datetime.datetime.now(TZ).date()
#         d0 = d1 - datetime.timedelta(days=a.last_days - 1)
#     if d0 > d1:
#         sys.exit("Khoảng ngày không hợp lệ.")

#     cookie, seller_id = doc_cookie(a.cookie)
#     start = datetime.datetime.combine(d0, datetime.time(0, 0, 0), TZ)
#     end = datetime.datetime.combine(d1, datetime.time(23, 59, 59), TZ)
#     t0, t1 = int(start.timestamp()), int(end.timestamp())
#     utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
#     utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

#     log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
#     log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")

#     rows = keo(cookie, seller_id, t0, t1)
#     log(f"TỔNG: {len(rows)} dòng, {len({r['main_order_id'] for r in rows})} đơn")
#     for cu in CREATORS:
#         sub = [r for r in rows if r["creator_username"] == cu]
#         if sub:
#             b = {}
#             for r in sub:
#                 b[r["promotion_position_type"]] = b.get(r["promotion_position_type"], 0) + 1
#             log(f"  {cu:<20} {len(sub):>5}  (1 Thẻ SP={b.get(1,0)}, 2 Video={b.get(2,0)}, 3 LIVE={b.get(3,0)})")

#     if a.csv:
#         import csv as _csv
#         with open(a.csv, "w", newline="", encoding="utf-8-sig") as f:
#             w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
#             if rows:
#                 w.writeheader()
#                 w.writerows(rows)
#         log(f"đã ghi {a.csv}")

#     if a.no_bq:
#         log("--no-bq: dừng, không đụng BigQuery.")
#         return
#     if len(rows) < a.min_rows:
#         sys.exit(f"[GUARD] chỉ có {len(rows)} dòng (< --min-rows {a.min_rows}). "
#                  f"DỪNG, không xoá gì. Nghi cookie hỏng hoặc TikTok đổi API.")
#     nap_bq(rows, seller_id, utc0, utc1)
#     log("XONG.")


# if __name__ == "__main__":
#     main()











#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TikTok Seller — NGƯỜI NHẬN HOA HỒNG theo đơn, nạp BigQuery fact_creator_tiktok.

Vì sao có script này: job affiliate cũ ngừng ghi nhận 3 tài khoản nhà (rhysman.com,
rhysman.shopping, rhysman_channel) từ 29/06/2026, trong khi TikTok VẪN trả dữ liệu.
Script này lấy thẳng từ API danh sách đơn của Seller Center.

NGUỒN DỮ LIỆU (quan trọng, đừng đổi sang file export):
  Người nhận hoa hồng nằm ở  main_orders[].sku_module[].creator_info_name.items[]
  phần tử position=3 -> "Người nhận hoa hồng: <handle>".
  Cột "Creator Handle" trong file export .xlsx KHÔNG phải trường này — nó để trống
  cho toàn bộ đơn Thẻ sản phẩm, thiếu ~27% dữ liệu. Đã kiểm chứng 06/08/2026.

  promotion_position_type lấy bằng cách lọc sale_source (mỗi đơn thuộc đúng 1 kênh,
  đã đối chiếu 501/501 đơn ngày 05/08/2026 khớp với cột Order Channel của file export):
      sale_source 1 = LIVE          -> promotion_position_type 3
      sale_source 2 = Video         -> promotion_position_type 2
      sale_source 3 = Thẻ sản phẩm  -> promotion_position_type 1

VÍ DỤ:
  py -X utf8 keo_creator_daily.py                     # 41 ngày gần nhất (LAST_DAYS_MAC_DINH)
  py -X utf8 keo_creator_daily.py --date 2026-08-05 --no-bq --csv out.csv
  py -X utf8 keo_creator_daily.py --from 2026-06-29 --to 2026-07-31   # backfill
  py -X utf8 keo_creator_daily.py --last-days 2 --cookie cookie_shop2.txt

Idempotent: load vào bảng staging, rồi DELETE (id_shop x creator_username x khoảng
create_time) + INSERT trong MỘT transaction. Chạy lại bao nhiêu lần cũng không nhân đôi,
và load hỏng thì bảng thật không bị thủng. Service account cần quyền tạo/xoá bảng trong
dataset (roles/bigquery.dataEditor là đủ).

create_time ghi xuống BigQuery là UTC (không tzinfo) — đúng quy ước sẵn có của bảng,
đã đối chiếu phân bố giờ của dữ liệu cũ ngày 06/08/2026. Đừng đổi sang giờ VN.

Kéo thiếu là hỏng dữ liệu chứ không phải chạy chậm: vì nap_bq xoá cả khoảng ngày rồi ghi
lại, script sẽ DỪNG HẲN (không đụng BigQuery) nếu chạm trần MAX_PAGES mà TikTok còn báo
dữ liệu, hoặc nếu số đơn lấy được ít hơn total_count TikTok tự báo. Chỉ dùng
--skip-total-check khi đã kiểm tay và biết chắc total_count của TikTok sai.
"""
import argparse, datetime, json, os, re, sys, time, urllib.parse, uuid

# ---------------------------------------------------------------- cấu hình
BASE = "https://seller-vn.tiktok.com"
API = BASE + "/api/fulfillment/order/list"
COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
TZ = datetime.timezone(datetime.timedelta(hours=7))

BQ_PROJECT = "rhysman-data-warehouse-488306"
BQ_DATASET = "rhysman"
BQ_TABLE = "fact_creator_tiktok"

# Số ngày kéo khi không truyền --date/--from/--to. Đổi số ngày mặc định thì sửa ĐÚNG ở
# đây (và ${LAST_DAYS:-41} trong .github/workflows/creator-daily.yml cho lịch chạy tự
# động). Mỗi run xoá và ghi lại trọn cửa sổ này, nên tăng lên là tăng cả thời gian chạy
# lẫn số partition BigQuery bị viết lại mỗi lần.
LAST_DAYS_MAC_DINH = 41

# Chỉ nạp các creator biết chắc nickname. API chỉ trả username, không trả nickname —
# thêm tên mới thì phải bổ sung ở đây, nếu không sẽ bị bỏ qua (có cảnh báo).
CREATORS = {
    "rhysman.com": "Rhys Man Chính Hãng",
    "rhysman.shopping": "Rhys Man",
    "rhysman_channel": "Rhys Man Chăm Sóc Cơ Thể",
}

# sale_source (API) -> promotion_position_type (BigQuery)
SALE_SOURCE_TO_POSITION = {"1": 3, "2": 2, "3": 1}
SALE_SOURCE_TEN = {"1": "LIVE", "2": "Video", "3": "Thẻ sản phẩm"}

# Chỉ nhận đúng item "Người nhận hoa hồng: <handle>". Regex lỏng r"hoa hồng:" nuốt luôn
# "Tỷ lệ hoa hồng: 20%" -> rác lọt vào cảnh báo "creator ngoài danh sách", làm hỏng đúng
# cái cảnh báo dùng để biết khi nào phải thêm tên vào CREATORS. Giữ RE_LONG để đếm và
# log số item bị loại, phòng khi TikTok đổi câu chữ thì thấy ngay.
RE_NGUOI_NHAN = re.compile(r"người nhận hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
RE_LONG = re.compile(r"hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
PAGE_SIZE = 100
MAX_PAGES = 800
BROWSER_RESTARTS = 3
BROWSER_RESTART_WAIT_SECONDS = 5
PAGE_TIMEOUT_MS = 90_000
JS_FETCH_TIMEOUT_MS = 90_000
BROWSER_SETTLE_MS = 4_000


def log(*a):
    print(datetime.datetime.now(TZ).strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- cookie
def chuan_hoa_cookie(cookie):
    """Cookie phải nằm trên một dòng để dùng làm HTTP header."""
    return (cookie or "").strip().replace("\r", "").replace("\n", "")


def seller_id_tu_cookie(cookie):
    m = re.search(r"oec_seller_id_unified_seller_env=(\d+)", cookie)
    if not m:
        m = re.search(r"(?:^|;\s*)SHOP_ID=(\d+)", cookie)
    return m.group(1) if m else None


class CookieState:
    """Giữ cookie gốc để inject lại mỗi lần dựng browser mới."""

    def __init__(self, cookie, seller_id, source_kind, path=None):
        self.cookie = cookie
        self.seller_id = seller_id
        self.source_kind = source_kind
        self.path = path

    def reload_if_changed(self):
        try:
            if self.source_kind == "env":
                candidate = chuan_hoa_cookie(os.environ.get(ENV_COOKIE))
            else:
                with open(self.path, encoding="utf-8") as cookie_file:
                    candidate = chuan_hoa_cookie(cookie_file.read())
        except OSError as error:
            log(f"    chưa nạp được cookie mới: {error}")
            return False

        if not candidate or candidate == self.cookie:
            return False
        seller_id = seller_id_tu_cookie(candidate)
        if seller_id != self.seller_id:
            log("    bỏ qua cookie mới vì seller_id không khớp shop đang crawl")
            return False
        self.cookie = candidate
        return True


def doc_cookie(path):
    """Ưu tiên biến môi trường TIKTOK_COOKIE (secret trên GitHub Actions), không có
    thì đọc file. Nhờ vậy trên Actions chỉ cần thay secret, không phải ghi file."""
    ck = (os.environ.get(ENV_COOKIE) or "").strip()
    nguon = f"biến môi trường {ENV_COOKIE}"
    source_kind = "env"
    if not ck:
        if not os.path.exists(path):
            sys.exit(f"[COOKIE] không có biến môi trường {ENV_COOKIE}, cũng không thấy file "
                     f"{path}.\nMở Seller Center -> F12 -> Network -> copy giá trị header "
                     f"cookie của một request bất kỳ, rồi đặt vào {ENV_COOKIE} hoặc {path}.")
        with open(path, encoding="utf-8") as cookie_file:
            ck = cookie_file.read()
        nguon = f"file {path}"
        source_kind = "file"
    if not ck:
        sys.exit(f"[COOKIE] {nguon} rỗng.")
    ck = chuan_hoa_cookie(ck)
    log(f"cookie đọc từ {nguon} ({len(ck)} ký tự)")
    seller_id = seller_id_tu_cookie(ck)
    if not seller_id:
        sys.exit("[COOKIE] không đọc được seller_id (oec_seller_id_unified_seller_env) từ cookie.")
    return CookieState(ck, seller_id, source_kind, path)


def common_params(seller_id):
    return {
        "locale": "vi-VN", "language": "vi-VN",
        "oec_seller_id": seller_id, "seller_id": seller_id,
        "aid": "4068", "app_name": "i18n_ecom_shop",
        "device_platform": "web", "cookie_enabled": "true",
    }


class BrowserRestartRequired(RuntimeError):
    pass


FETCH_IN_PAGE = """
async ({url, body, timeoutMs}) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "accept": "application/json, text/plain, */*",
                "x-tt-oec-region": "VN"
            },
            body: JSON.stringify(body),
            credentials: "include",
            signal: controller.signal
        });
        const responseText = await response.text();
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (error) {
            return {
                status: response.status,
                error: `response không phải JSON: ${responseText.slice(0, 500)}`
            };
        }
        return {status: response.status, data};
    } catch (error) {
        return {error: error instanceof Error ? error.message : String(error)};
    } finally {
        clearTimeout(timeoutId);
    }
}
"""


class TikTokBrowserClient:
    """Gọi API trong page và thay toàn bộ Chromium khi browser/context hỏng."""

    def __init__(self, playwright, cookie_state, browser_restarts=BROWSER_RESTARTS,
                 browser_restart_wait=BROWSER_RESTART_WAIT_SECONDS):
        self.playwright = playwright
        self.cookie_state = cookie_state
        self.browser_restarts = browser_restarts
        self.browser_restart_wait = browser_restart_wait
        self.restart_count = 0
        self.browser_number = 0
        self.browser = None
        self.context = None
        self.page = None

    def _playwright_cookies(self):
        cookies = []
        for part in self.cookie_state.cookie.split(";"):
            part = part.strip()
            if "=" not in part:
                continue
            name, _, value = part.partition("=")
            cookies.append({"name": name.strip(), "value": value.strip(), "url": BASE})
        return cookies

    def _destroy_browser(self):
        context, browser = self.context, self.browser
        self.page = None
        self.context = None
        self.browser = None
        if context is not None:
            try:
                context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass

    def _launch_browser(self):
        self.browser_number += 1
        log(f"    launch Chromium process #{self.browser_number}")
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            viewport={"width": 1440, "height": 960},
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
        )
        cookies = self._playwright_cookies()
        self.context.add_cookies(cookies)
        self.page = self.context.new_page()
        self.page.set_default_timeout(PAGE_TIMEOUT_MS)
        self.page.goto(BASE + "/order", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        self.page.wait_for_timeout(BROWSER_SETTLE_MS)
        current_url = self.page.url.lower()
        if "login" in current_url or "passport" in current_url:
            sys.exit(f"[COOKIE] Browser bị chuyển sang trang đăng nhập: {self.page.url}. "
                     "Hãy cập nhật cookie rồi chạy lại.")
        if "verify" in current_url or "captcha" in current_url:
            raise BrowserRestartRequired(f"browser mở vào trang xác minh: {self.page.url}")
        log(f"    Chromium #{self.browser_number} sẵn sàng: {self.page.url}")

    def _restart_browser(self, page_description, error):
        self._destroy_browser()
        if self.restart_count >= self.browser_restarts:
            sys.exit(
                f"[BROWSER] Đã đập và dựng lại Chromium {self.browser_restarts} lần tại "
                f"{page_description} nhưng vẫn lỗi: {error}. Cursor chưa bị tăng và BigQuery "
                "chưa bị thay đổi."
            )
        self.restart_count += 1
        log(f"  ! browser lỗi tại {page_description}: {error}")
        log(f"    đập toàn bộ browser; giữ checkpoint cursor của page hiện tại "
            f"({self.restart_count}/{self.browser_restarts})")
        if self.browser_restart_wait:
            time.sleep(self.browser_restart_wait)
        cookie_changed = self.cookie_state.reload_if_changed()
        if cookie_changed:
            log("    đã nạp cookie mới trước khi dựng Chromium")

    def _ensure_browser(self, page_description):
        while self.page is None:
            try:
                self._launch_browser()
            except SystemExit:
                self._destroy_browser()
                raise
            except Exception as error:
                self._restart_browser(page_description, error)

    def close(self):
        self._destroy_browser()

    def post(self, url, body, page_description):
        while True:
            self._ensure_browser(page_description)
            try:
                result = self.page.evaluate(
                    FETCH_IN_PAGE,
                    {"url": url, "body": body, "timeoutMs": JS_FETCH_TIMEOUT_MS},
                )
                if result.get("error"):
                    raise BrowserRestartRequired(result["error"])
                status = result.get("status")
                if status in (401, 403):
                    sys.exit(f"[COOKIE] Browser nhận HTTP {status} — cookie hết hạn hoặc bị "
                             "risk-control chặn. Lấy cookie mới rồi chạy lại. "
                             "(Dựng lại browser cũng không cứu được, nên dừng luôn.)")
                if status != 200:
                    raise BrowserRestartRequired(f"HTTP status {status}")
                js = result.get("data") or {}
                code = js.get("code")
                if str(code) == "0":
                    # Ngân sách restart là cho một CHUỖI lỗi liên tiếp, không phải cho cả run:
                    # backfill vài trăm trang dính 3 lỗi vặt cách xa nhau vẫn phải chạy tiếp.
                    self.restart_count = 0
                    return js
                if str(code) in ("98001002", "98001008"):
                    sys.exit(f"[COOKIE] TikTok trả code {code}: {js.get('message')}. "
                             "Cookie đã hết hạn — lấy cookie mới rồi chạy lại.")
                if str(code) == "10000":
                    raise BrowserRestartRequired(
                        js.get("message") or "TikTok yêu cầu xác minh browser"
                    )
                raise BrowserRestartRequired(f"API code={code}: {js.get('message')}")
            except SystemExit:
                raise
            except Exception as error:
                self._restart_browser(page_description, error)


# ---------------------------------------------------------------- kéo dữ liệu
def keo_mot_kenh(client, seller_id, sale_source, t0, t1, kiem_tra_total=True):
    """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh."""
    url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
    cond = {
        "order_source": {"value": ["1"]},
        "time_order_created": {"value": [str(t0), str(t1)]},
        "sale_source": {"value": [sale_source]},
    }
    out, cursor, pages, total = [], "", 0, None
    so_don, bo_qua, lech_regex, con_tiep = 0, 0, 0, False
    while pages < MAX_PAGES:
        body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
                "search_cursor": cursor, "search_condition": {"condition_list": cond}}
        js = client.post(url, body, f"sale_source={sale_source}, trang {pages + 1}")
        d = js.get("data") or {}
        if total is None:                      # lấy ở trang đầu: đơn mới về giữa chừng
            total = d.get("total_count")       # không được làm mốc kiểm tra nhảy số
        orders = d.get("main_orders") or []
        if not orders:
            break
        so_don += len(orders)
        for o in orders:
            try:
                oid = int(o.get("main_order_id"))
                t = int((o.get("trade_order_module") or {}).get("create_time"))
            except (TypeError, ValueError):
                bo_qua += 1                    # thiếu field -> bỏ đơn, đừng để crash cả run
                continue
            seen = set()
            for sku in (o.get("sku_module") or []):
                for it in ((sku.get("creator_info_name") or {}).get("items") or []):
                    msg = it.get("message_content") or ""
                    m = RE_NGUOI_NHAN.search(msg)
                    if m:
                        seen.add(m.group(1))
                    elif RE_LONG.search(msg):
                        lech_regex += 1
            for c in seen:
                out.append((oid, t, c))
        cursor = d.get("search_next_cursor") or ""
        pages += 1
        con_tiep = bool(d.get("search_next_has_more")) and bool(cursor)
        if not con_tiep:
            break

    log(f"  sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
        f"total_count={total}, {pages} trang, {so_don} đơn, {len(out)} cặp đơn-creator")
    if bo_qua:
        log(f"    ! bỏ {bo_qua} đơn thiếu main_order_id hoặc create_time")
    if lech_regex:
        log(f"    ({lech_regex} item có 'hoa hồng:' nhưng không phải 'Người nhận hoa hồng:' "
            f"— đã bỏ qua)")

    # Hai guard dưới đây phải là sys.exit chứ không phải log: nap_bq sẽ DELETE nguyên
    # khoảng ngày rồi INSERT lại, nên kéo thiếu mà chạy tiếp là mất dữ liệu thật.
    if con_tiep:
        sys.exit(f"[PHÂN TRANG] sale_source={sale_source} chạm trần MAX_PAGES={MAX_PAGES} "
                 f"mà TikTok vẫn báo còn dữ liệu. DỪNG, không đụng BigQuery. "
                 f"Hãy chia nhỏ khoảng --from/--to hoặc tăng MAX_PAGES.")
    try:
        total_int = int(total)
    except (TypeError, ValueError):
        total_int = None
    if kiem_tra_total and total_int is not None and so_don < total_int:
        sys.exit(f"[PHÂN TRANG] sale_source={sale_source}: TikTok báo total_count={total_int} "
                 f"nhưng chỉ lấy được {so_don} đơn. DỪNG, không đụng BigQuery. "
                 f"Nếu chắc chắn total_count của TikTok sai thì chạy lại với --skip-total-check.")
    return out


def keo(client, seller_id, t0, t1, kiem_tra_total=True):
    rows, la = {}, set()
    for ss in ("1", "2", "3"):
        for oid, t, cu in keo_mot_kenh(client, seller_id, ss, t0, t1, kiem_tra_total):
            if cu not in CREATORS:
                la.add(cu)
                continue
            key = (oid, cu)
            pos = SALE_SOURCE_TO_POSITION[ss]
            if key in rows:
                if rows[key]["promotion_position_type"] != pos:
                    log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
                        f"({rows[key]['promotion_position_type']} và {pos}) — giữ kênh đầu")
                continue
            rows[key] = {
                "main_order_id": oid,
                "creator_nickname": CREATORS[cu],
                "creator_username": cu,
                "promotion_position_type": pos,
                "create_time": datetime.datetime.fromtimestamp(
                    t, datetime.timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
                "cos_ratio": None,
                "estimated_cos_fee": None,
                "shop_ads_commission_ratio": None,
                "estimated_shop_ads_commission": None,
                "id_shop": seller_id,
            }
    if la:
        log(f"  (bỏ qua {len(la)} creator ngoài danh sách CREATORS, vd: {sorted(la)[:5]})")
    return sorted(rows.values(), key=lambda r: r["create_time"])


# ---------------------------------------------------------------- BigQuery
def bq_client():
    """Giống các job khác trong repo: ưu tiên key file, rồi JSON inline trong biến
    môi trường (secret GOOGLE_SERVICE_ACCOUNT_JSON), cuối cùng mới dùng ADC."""
    from google.cloud import bigquery
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

    if key_path and os.path.exists(key_path):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(key_path)
        log(f"  BigQuery: key file {key_path}")
        return bigquery.Client(project=BQ_PROJECT, credentials=cred)
    if key_json:
        from google.oauth2 import service_account
        try:
            info = json.loads(key_json)
        except json.JSONDecodeError as e:
            sys.exit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
        cred = service_account.Credentials.from_service_account_info(info)
        log(f"  BigQuery: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email','?')})")
        return bigquery.Client(project=BQ_PROJECT, credentials=cred)
    log("  BigQuery: credentials mặc định của môi trường (ADC)")
    return bigquery.Client(project=BQ_PROJECT)


def _dml_theo_cau_lenh(client, script_job):
    """Số dòng DELETE/INSERT của từng câu trong script. Thiếu quyền jobs.list thì thôi."""
    try:
        ra = {}
        for child in client.list_jobs(parent_job=script_job.job_id):
            if child.num_dml_affected_rows is not None:
                ra[child.statement_type] = child.num_dml_affected_rows
        return ra
    except Exception:
        return {}


def nap_bq(rows, seller_id, utc0, utc1):
    """Nạp qua bảng staging rồi DELETE + INSERT trong MỘT transaction.

    Cách cũ (DELETE thẳng rồi mới load) mà load hỏng là thủng nguyên khoảng ngày trong
    bảng thật — daily thì chạy lại là xong, nhưng backfill 1 tháng thì mất 1 tháng dữ
    liệu cho tới lần chạy sau. Staging cũng bắt luôn lỗi lệch kiểu vì nó load theo đúng
    schema bảng đích thay vì để BigQuery tự đoán.
    """
    from google.cloud import bigquery
    client = bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    bang_dich = client.get_table(table_id)

    cot_bang = {f.name for f in bang_dich.schema}
    thieu = [c for c in rows[0] if c not in cot_bang]
    if thieu:
        sys.exit(f"[BQ] các cột {thieu} không tồn tại trong {table_id}.")
    cot = ", ".join(f"`{c}`" for c in rows[0])

    stg_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}__stg_{uuid.uuid4().hex[:12]}"
    job = client.load_table_from_json(
        rows, stg_id,
        job_config=bigquery.LoadJobConfig(
            schema=bang_dich.schema, write_disposition="WRITE_TRUNCATE"))
    job.result()
    log(f"  staging {stg_id.split('.')[-1]}: {len(rows)} dòng")

    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("shop", "STRING", seller_id),
        bigquery.ArrayQueryParameter("creators", "STRING", list(CREATORS)),
        bigquery.ScalarQueryParameter("t0", "DATETIME", utc0),
        bigquery.ScalarQueryParameter("t1", "DATETIME", utc1),
    ])
    sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{table_id}`
        WHERE id_shop = @shop
          AND creator_username IN UNNEST(@creators)
          AND create_time >= @t0 AND create_time <= @t1;
        INSERT INTO `{table_id}` ({cot}) SELECT {cot} FROM `{stg_id}`;
        COMMIT TRANSACTION;
    """
    try:
        job = client.query(sql, job_config=cfg)
        job.result()
    finally:
        client.delete_table(stg_id, not_found_ok=True)
    dml = _dml_theo_cau_lenh(client, job)
    log(f"  DELETE {dml.get('DELETE', '?')} dòng cũ + INSERT {dml.get('INSERT', len(rows))} "
        f"dòng mới, cùng 1 transaction -> {table_id}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--date", help="một ngày YYYY-MM-DD (giờ VN)")
    g.add_argument("--last-days", type=int, default=None,
                   help=f"N ngày gần nhất tính cả hôm nay (mặc định {LAST_DAYS_MAC_DINH})")
    ap.add_argument("--from", dest="d_from", help="từ ngày YYYY-MM-DD")
    ap.add_argument("--to", dest="d_to", help="đến ngày YYYY-MM-DD")
    ap.add_argument("--cookie", default=COOKIE,
                    help=f"file cookie (mặc định {COOKIE}); bị bỏ qua nếu đã có "
                         f"biến môi trường {ENV_COOKIE}")
    ap.add_argument("--csv", help="ghi thêm ra file CSV để kiểm tra")
    ap.add_argument("--no-bq", action="store_true", help="không đụng BigQuery")
    ap.add_argument("--min-rows", type=int, default=1,
                    help="ít hơn số này thì DỪNG, không xoá gì (chống wipe)")
    ap.add_argument("--skip-total-check", action="store_true",
                    help="bỏ đối chiếu số đơn lấy được với total_count của TikTok "
                         "(chỉ dùng khi biết chắc total_count của TikTok sai)")
    ap.add_argument("--browser-restarts", type=int, default=BROWSER_RESTARTS,
                    help="số lần đập và dựng lại toàn bộ Chromium khi browser lỗi "
                         f"(mặc định {BROWSER_RESTARTS})")
    ap.add_argument("--browser-restart-wait", type=int, default=BROWSER_RESTART_WAIT_SECONDS,
                    help="số giây chờ trước khi launch Chromium mới "
                         f"(mặc định {BROWSER_RESTART_WAIT_SECONDS})")
    a = ap.parse_args()

    if a.browser_restarts < 0 or a.browser_restart_wait < 0:
        sys.exit("--browser-restarts và --browser-restart-wait không được âm.")

    if a.d_from or a.d_to:
        if not (a.d_from and a.d_to):
            sys.exit("--from và --to phải đi cùng nhau.")
        if a.date or a.last_days is not None:
            sys.exit("--from/--to không dùng chung với --date hoặc --last-days.")
        d0 = datetime.date.fromisoformat(a.d_from)
        d1 = datetime.date.fromisoformat(a.d_to)
    elif a.date:
        d0 = d1 = datetime.date.fromisoformat(a.date)
    else:
        d1 = datetime.datetime.now(TZ).date()
        so_ngay = LAST_DAYS_MAC_DINH if a.last_days is None else a.last_days
        d0 = d1 - datetime.timedelta(days=so_ngay - 1)
    if d0 > d1:
        sys.exit("Khoảng ngày không hợp lệ.")

    cookie_state = doc_cookie(a.cookie)
    seller_id = cookie_state.seller_id
    start = datetime.datetime.combine(d0, datetime.time(0, 0, 0), TZ)
    end = datetime.datetime.combine(d1, datetime.time(23, 59, 59), TZ)
    t0, t1 = int(start.timestamp()), int(end.timestamp())
    utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
    log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("[PLAYWRIGHT] Chưa cài Playwright. Chạy: pip install playwright && "
                 "python -m playwright install chromium")

    with sync_playwright() as playwright:
        client = TikTokBrowserClient(
            playwright,
            cookie_state,
            a.browser_restarts,
            a.browser_restart_wait,
        )
        try:
            rows = keo(client, seller_id, t0, t1, not a.skip_total_check)
        finally:
            client.close()
    log(f"TỔNG: {len(rows)} dòng, {len({r['main_order_id'] for r in rows})} đơn")
    for cu in CREATORS:
        sub = [r for r in rows if r["creator_username"] == cu]
        if sub:
            b = {}
            for r in sub:
                b[r["promotion_position_type"]] = b.get(r["promotion_position_type"], 0) + 1
            log(f"  {cu:<20} {len(sub):>5}  (1 Thẻ SP={b.get(1,0)}, 2 Video={b.get(2,0)}, 3 LIVE={b.get(3,0)})")

    if a.csv:
        import csv as _csv
        with open(a.csv, "w", newline="", encoding="utf-8-sig") as f:
            w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
            if rows:
                w.writeheader()
                w.writerows(rows)
        log(f"đã ghi {a.csv}")

    if a.no_bq:
        log("--no-bq: dừng, không đụng BigQuery.")
        return
    if len(rows) < a.min_rows:
        sys.exit(f"[GUARD] chỉ có {len(rows)} dòng (< --min-rows {a.min_rows}). "
                 f"DỪNG, không xoá gì. Nghi cookie hỏng hoặc TikTok đổi API.")
    nap_bq(rows, seller_id, utc0, utc1)
    log("XONG.")


if __name__ == "__main__":
    main()
