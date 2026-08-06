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
  py -X utf8 keo_creator_daily.py --last-days 2
  py -X utf8 keo_creator_daily.py --date 2026-08-05 --no-bq --csv out.csv
  py -X utf8 keo_creator_daily.py --from 2026-06-29 --to 2026-07-31   # backfill
  py -X utf8 keo_creator_daily.py --last-days 2 --cookie cookie_shop2.txt

Idempotent: DELETE đúng (id_shop x creator_username x khoảng create_time) rồi INSERT.
Chạy lại bao nhiêu lần cũng không nhân đôi.
"""
import argparse, datetime, json, os, re, sys, time, urllib.parse, urllib.request, urllib.error

# ---------------------------------------------------------------- cấu hình
BASE = "https://seller-vn.tiktok.com"
API = BASE + "/api/fulfillment/order/list"
COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
TZ = datetime.timezone(datetime.timedelta(hours=7))

BQ_PROJECT = "rhysman-data-warehouse-488306"
BQ_DATASET = "rhysman"
BQ_TABLE = "fact_creator_tiktok"

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

RE_HOA_HONG = re.compile(r"hoa hồng:\s*(\S+)")
PAGE_SIZE = 100
MAX_PAGES = 400
CHALLENGE_RETRIES = 3
CHALLENGE_WAIT_SECONDS = 30


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
    """Giữ cookie hiện tại và có thể nạp lại nguồn cookie khi đổi phiên."""

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


class TikTokChallenge(RuntimeError):
    pass


def post(url, cookie, body, tries=6, opener=None):
    """Gọi API, retry khi lỗi mạng. Cookie chết thì dừng hẳn, retry vô nghĩa."""
    data = json.dumps(body).encode()
    headers = {
        "content-type": "application/json",
        "accept": "*/*",
        "cookie": cookie,
        "origin": BASE,
        "referer": BASE + "/order",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "x-tt-oec-region": "VN",
    }
    open_request = opener.open if opener else urllib.request.urlopen
    for k in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with open_request(req, timeout=90) as r:
                js = json.loads(r.read())
            code = js.get("code")
            if str(code) == "0":
                return js
            if str(code) in ("98001002", "98001008"):
                sys.exit(f"[COOKIE] TikTok trả code {code}: {js.get('message')}. "
                         f"Cookie đã hết hạn — lấy cookie mới rồi chạy lại.")
            if str(code) == "10000":
                raise TikTokChallenge(js.get("message") or "TikTok yêu cầu xác minh phiên")
            if k == tries - 1:
                sys.exit(f"[API] code={code} message={js.get('message')}")
        except TikTokChallenge:
            raise
        except SystemExit:
            raise
        except Exception as e:
            if k == tries - 1:
                raise
            log(f"    retry {k+1} ({e})")
        time.sleep(2 * (k + 1))


class TikTokClient:
    """Retry nguyên request trên transport mới khi phiên bị challenge."""

    def __init__(self, cookie_state, challenge_retries=CHALLENGE_RETRIES,
                 challenge_wait=CHALLENGE_WAIT_SECONDS):
        self.cookie_state = cookie_state
        self.challenge_retries = challenge_retries
        self.challenge_wait = challenge_wait
        self.challenge_count = 0
        self.session_number = 1
        self.opener = urllib.request.build_opener()

    def post(self, url, body, page_description):
        while True:
            try:
                return post(url, self.cookie_state.cookie, body, opener=self.opener)
            except TikTokChallenge as error:
                if self.challenge_count >= self.challenge_retries:
                    sys.exit(
                        f"[CHALLENGE] Vẫn bị yêu cầu xác minh tại {page_description} sau "
                        f"{self.challenge_retries} lần tạo phiên mới. Cursor chưa bị tăng và "
                        "BigQuery chưa bị thay đổi. Hãy xác minh Seller Center, cập nhật cookie "
                        "rồi chạy lại."
                    )

                self.challenge_count += 1
                log(f"  ! challenge tại {page_description}: {error}")
                log("    giữ nguyên cursor; sẽ retry đúng page này trên phiên mới")
                if self.challenge_wait:
                    log(f"    chờ {self.challenge_wait}s để phiên cũ hạ nhiệt/cookie được cập nhật")
                    time.sleep(self.challenge_wait)

                cookie_changed = self.cookie_state.reload_if_changed()
                self.opener = urllib.request.build_opener()
                self.session_number += 1
                cookie_status = "đã nạp cookie mới" if cookie_changed else "giữ cookie hiện tại"
                log(f"    phiên HTTP #{self.session_number}: {cookie_status}")


# ---------------------------------------------------------------- kéo dữ liệu
def keo_mot_kenh(client, seller_id, sale_source, t0, t1):
    """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh."""
    url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
    cond = {
        "order_source": {"value": ["1"]},
        "time_order_created": {"value": [str(t0), str(t1)]},
        "sale_source": {"value": [sale_source]},
    }
    out, cursor, pages, total = [], "", 0, None
    while pages < MAX_PAGES:
        body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
                "search_cursor": cursor, "search_condition": {"condition_list": cond}}
        js = client.post(url, body, f"sale_source={sale_source}, trang {pages + 1}")
        d = js["data"]
        total = d.get("total_count")
        orders = d.get("main_orders") or []
        if not orders:
            break
        for o in orders:
            oid = o.get("main_order_id")
            t = (o.get("trade_order_module") or {}).get("create_time")
            seen = set()
            for sku in (o.get("sku_module") or []):
                for it in ((sku.get("creator_info_name") or {}).get("items") or []):
                    m = RE_HOA_HONG.search(it.get("message_content") or "")
                    if m:
                        seen.add(m.group(1))
            for c in seen:
                out.append((oid, int(t), c))
        cursor = d.get("search_next_cursor") or ""
        pages += 1
        if not d.get("search_next_has_more") or not cursor:
            break
    log(f"  sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
        f"total_count={total}, {pages} trang, {len(out)} cặp đơn-creator")
    return out


def keo(client, seller_id, t0, t1):
    rows, la = {}, set()
    for ss in ("1", "2", "3"):
        for oid, t, cu in keo_mot_kenh(client, seller_id, ss, t0, t1):
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
                "main_order_id": int(oid),
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


def nap_bq(rows, seller_id, utc0, utc1):
    from google.cloud import bigquery
    client = bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("shop", "STRING", seller_id),
        bigquery.ArrayQueryParameter("creators", "STRING", list(CREATORS)),
        bigquery.ScalarQueryParameter("t0", "DATETIME", utc0),
        bigquery.ScalarQueryParameter("t1", "DATETIME", utc1),
    ])
    sql = f"""
        DELETE FROM `{table_id}`
        WHERE id_shop = @shop
          AND creator_username IN UNNEST(@creators)
          AND create_time >= @t0 AND create_time <= @t1
    """
    job = client.query(sql, job_config=cfg)
    job.result()
    log(f"  DELETE xong: {job.num_dml_affected_rows} dòng cũ")

    job = client.load_table_from_json(
        rows, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job.result()
    if job.errors:
        sys.exit(f"[BQ] lỗi nạp: {job.errors}")
    log(f"  INSERT xong: {len(rows)} dòng -> {table_id}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--date", help="một ngày YYYY-MM-DD (giờ VN)")
    g.add_argument("--last-days", type=int, default=41,
                   help="N ngày gần nhất tính cả hôm nay (mặc định 2)")
    ap.add_argument("--from", dest="d_from", help="từ ngày YYYY-MM-DD")
    ap.add_argument("--to", dest="d_to", help="đến ngày YYYY-MM-DD")
    ap.add_argument("--cookie", default=COOKIE,
                    help=f"file cookie (mặc định {COOKIE}); bị bỏ qua nếu đã có "
                         f"biến môi trường {ENV_COOKIE}")
    ap.add_argument("--csv", help="ghi thêm ra file CSV để kiểm tra")
    ap.add_argument("--no-bq", action="store_true", help="không đụng BigQuery")
    ap.add_argument("--min-rows", type=int, default=1,
                    help="ít hơn số này thì DỪNG, không xoá gì (chống wipe)")
    ap.add_argument("--challenge-retries", type=int, default=CHALLENGE_RETRIES,
                    help="số lần tạo phiên HTTP mới và retry đúng page khi gặp challenge "
                         f"(mặc định {CHALLENGE_RETRIES})")
    ap.add_argument("--challenge-wait", type=int, default=CHALLENGE_WAIT_SECONDS,
                    help="số giây chờ trước mỗi lần đổi phiên "
                         f"(mặc định {CHALLENGE_WAIT_SECONDS})")
    a = ap.parse_args()

    if a.challenge_retries < 0 or a.challenge_wait < 0:
        sys.exit("--challenge-retries và --challenge-wait không được âm.")

    if a.d_from or a.d_to:
        if not (a.d_from and a.d_to):
            sys.exit("--from và --to phải đi cùng nhau.")
        d0 = datetime.date.fromisoformat(a.d_from)
        d1 = datetime.date.fromisoformat(a.d_to)
    elif a.date:
        d0 = d1 = datetime.date.fromisoformat(a.date)
    else:
        d1 = datetime.datetime.now(TZ).date()
        d0 = d1 - datetime.timedelta(days=a.last_days - 1)
    if d0 > d1:
        sys.exit("Khoảng ngày không hợp lệ.")

    cookie_state = doc_cookie(a.cookie)
    seller_id = cookie_state.seller_id
    client = TikTokClient(cookie_state, a.challenge_retries, a.challenge_wait)
    start = datetime.datetime.combine(d0, datetime.time(0, 0, 0), TZ)
    end = datetime.datetime.combine(d1, datetime.time(23, 59, 59), TZ)
    t0, t1 = int(start.timestamp()), int(end.timestamp())
    utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
    log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")

    rows = keo(client, seller_id, t0, t1)
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
