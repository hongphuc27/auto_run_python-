# import requests
# import urllib.parse
# from datetime import datetime, timezone, timedelta
# import pandas as pd
# import time
# import os
# from decimal import Decimal, InvalidOperation
# from google.oauth2 import service_account
# from google.cloud import bigquery
# import json


# # =====================================================
# # BIGQUERY  CONFIG (THEO CỦA BẠN)
# # =====================================================
# PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
# DATASET_ID = "rhysman"
# TABLE_ID = "fact_creator_tiktok"


# gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
# credentials = service_account.Credentials.from_service_account_info(gcp_key)

# client = bigquery.Client(
#     credentials=credentials,
#     project=PROJECT_ID
# )


# # =====================================================
# # TIKTOK API (DÁN TỪ F12 → COPY AS CURL)
# # =====================================================
# URL = (
#     "https://affiliate.tiktok.com/api/v1/affiliate/orders"
#     "?user_language=vi-VN"
#     "&aid=4331"
#     "&app_name=i18n_ecom_alliance"
#     "&device_platform=web"
#     "&browser_language=vi"
#     "&browser_platform=Win32"
#     "&browser_name=Mozilla"
#     "&timezone_name=Asia%2FSaigon"
#     "&shop_region=VN"
#     "&oec_seller_id=7494545630022240481"
#     "&msToken=jF_qv_JGHymfc6FH5JfxGP8W7xJeAioYjAOmpdZEYZFEyMrH-0BKLXo-rapivzkzXcssb8CnCFjmCFiODR7SDX0AksmdSWdgdghDmvmBhnBs2phUrSuzBnVtYqmOl_ChVVhmy9O50Is4dhY-Bu_ucaXo"
#     "&X-Bogus=DFSzswVOvWXPDOsJCUVVxW6-55y9"
#     "&X-Gnarly=M/pvw5L9WLqUJl-3pVEm3x1fpDmJ1h7JyAtIVoZpObcN7ya3hOsEQvkU/O0zYsj62q9YD6g2FuPdCfiPUU9aHDxNIL/jzBS9M7lGWT2cSYRH6Y5C8dNyQsTaZP0OhipauFlnvHEmMuYf91bhZpoeTvs0CGw4h0tEIOGEJbDBmfVCHDhR0CPxP4xStKVArceX7we8Li3bW04rqV0heaCcYiqPt0hM06mz3C1w3d6d6binz1eV5dEiNhLRYqWT2LHhLgjx7cU0Eikc"
# )


# HEADERS = {
#     "accept": "application/json, text/plain, */*",
#     "content-type": "application/json",
#     "origin": "https://affiliate.tiktok.com",
#     "referer": "https://affiliate.tiktok.com/product/order?shop_region=VN",
#     "user-agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/144.0.0.0 Safari/537.36"
#     ),
#     "cookie": os.environ["TIKTOK_COOKIE_RHYSMAN"]
# }

# # =====================================================
# # =====================================================
# tz_vn = timezone(timedelta(hours=7))

# # =====================================================
# def fetch_page(page: int, start_time, end_time):
#     payload = {
#     "conditions": {
#         "time_period": {
#             "beginning_time": str(start_time),
#             "ending_time": str(end_time)
#         }
#     },
#         "page": page,
#         "page_size": 100
#     }

#     r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
#     r.raise_for_status()
#     return r.json()

# # =====================================================
# def run():
#     all_rows = []

#     now_vn = datetime.now(tz=tz_vn)
#     start_date = (now_vn - timedelta(days=31)).replace(tzinfo=None)
#     end_date = now_vn.replace(tzinfo=None)

#     current_date = start_date

#     while current_date <= end_date:

#         print("Fetching date:", current_date.date())

#         START_TIME = int(datetime(
#             current_date.year,
#             current_date.month,
#             current_date.day,
#             0, 0, 0,
#             tzinfo=tz_vn
#         ).timestamp() * 1000)

#         END_TIME = int(datetime(
#             current_date.year,
#             current_date.month,
#             current_date.day,
#             23, 59, 59,
#             tzinfo=tz_vn
#         ).timestamp() * 1000)

#         page = 1
#         empty_retry = 0

#         while True:

#             try:
#                 data = fetch_page(page, START_TIME, END_TIME)
#             except Exception as e:
#                 print("API error retry:", e)
#                 time.sleep(5)
#                 continue

#             orders = data.get("orders", [])

#             print(f"Page {page} | Orders: {len(orders)}")

#             if not orders:

#                 empty_retry += 1

#                 if empty_retry >= 3:
#                     print("No more data for this day.")
#                     break

#                 print("Empty page -> retry")
#                 time.sleep(2)
#                 continue

#             empty_retry = 0

#             for o in orders:

#                 main_order_id = o.get("main_order_id")
#                 create_time_ms = o.get("create_time")

#                 if not main_order_id or not create_time_ms:
#                     continue

#                 create_time = datetime.fromtimestamp(
#                     create_time_ms / 1000
#                 ).replace(tzinfo=None)

#                 sku_details = o.get("sku_detail", [])

#                 for sku in sku_details:

#                     creator_nickname = sku.get("creator_nickname")
#                     creator_username = sku.get("creator_username")

#                     cos_ratio = sku.get("cos_ratio")
#                     estimated_cos_fee = sku.get("estimated_cos_fee")

#                     shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
#                     estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")

#                     promotion_position_type = (
#                         sku.get("promotion_position", {})
#                         .get("promotion_position_type")
#                     )

#                     all_rows.append((
#                         int(main_order_id),
#                         "7494545630022240481",
#                         creator_nickname,
#                         creator_username,
#                         promotion_position_type,
#                         create_time,
#                         cos_ratio,
#                         estimated_cos_fee,
#                         shop_ads_commission_ratio,
#                         estimated_shop_ads_commission
#                     ))

#             page += 1
#             time.sleep(0.3)

#         # sang ngày tiếp theo
#         current_date += timedelta(days=1)
    
#     print("TOTAL ROWS TO INSERT:", len(all_rows))

#     if not all_rows:
#         print("NO DATA TO INSERT")
#         return

#     df = pd.DataFrame(all_rows, columns=[
#     "main_order_id",
#     "id_shop",
#     "creator_nickname",
#     "creator_username",
#     "promotion_position_type",
#     "create_time",
#     "cos_ratio",
#     "estimated_cos_fee",
#     "shop_ads_commission_ratio",
#     "estimated_shop_ads_commission"
# ])

#     df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

#     def to_decimal(x):
#         try:
#             if pd.isna(x):
#                 return None
#             return Decimal(str(x))
#         except (InvalidOperation, ValueError):
#             return None


#     df["cos_ratio"] = df["cos_ratio"].apply(to_decimal)
#     df["estimated_cos_fee"] = df["estimated_cos_fee"].apply(to_decimal)
#     df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
#     df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

#     # drop row lỗi
#     df = df.dropna(subset=["main_order_id"])
        
#     # df = df.drop_duplicates(
#     # subset=["main_order_id", "promotion_position_type", "cos_ratio", "estimated_cos_fee", "shop_ads_commission_ratio", "estimated_shop_ads_commission"]
#     # )

#     table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

#     # ==============================
#     # DELETE DATA TODAY + YESTERDAY
#     # ==============================
#     delete_query = f"""
#     DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
#     WHERE id_shop = '7494545630022240481' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 31 DAY)  
#     """

#     client.query(delete_query).result()

#     print("Old data (today + yesterday) deleted.")

#     # ==============================

#     job_config = bigquery.LoadJobConfig(
#         write_disposition="WRITE_APPEND"
#     )

#     job = client.load_table_from_dataframe(
#         df,
#         table_ref,
#         job_config=job_config
#     )

#     job.result()

#     print(f"✅ Loaded {len(df)} rows into BigQuery")



# # =====================================================
# if __name__ == "__main__":
#     run()
















"""
Kéo đơn Affiliate Creator TikTok -> BigQuery (fact_creator_tiktok)
==================================================================
BẢN V2 — dùng endpoint MỚI của TikTok:
    https://affiliate.tiktok.com/api/oec/pay/statement/order/seller/orders/list

Lý do: endpoint cũ /api/v1/affiliate/orders đã bị TikTok khai tử
(trả code 98001008 "old affiliate es read denied") -> không kéo được data.

Khác biệt so với bản cũ:
  - Phân trang bằng CURSOR (không phải page number, không loop theo từng ngày).
  - Cấu trúc response mới: data.sku_order_list[].{base_info, commission_info}.
  - Commission tách 2 loại theo cos_type: 1=hoa hồng thường, 0=shop ads.
  - Có thêm chữ ký X-Tts-Oec-Bsid trong URL.

GIỮ NGUYÊN toàn bộ logic BigQuery cũ (bảng, delete 31 ngày, WRITE_APPEND, to_decimal).

⚠️ TOKEN HẾT HẠN NHANH: msToken / X-Bogus / X-Gnarly / X-Tts-Oec-Bsid và cookie
   là chữ ký ngắn hạn. Khi script báo lỗi login/captcha -> lấy lại từ F12:
   trang https://affiliate.tiktok.com/product/order  ->  request 'orders/list'
   -> Copy as cURL -> thay 4 tham số trong URL + cookie (TIKTOK_COOKIE_RHYSMAN).
"""

import requests
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
import os
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json


# =====================================================
# BIGQUERY CONFIG (GIỮ NGUYÊN)
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_creator_tiktok"

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

ID_SHOP = "7494545630022240481"

# =====================================================
# TIKTOK API (ENDPOINT MỚI) — DÁN 4 CHỮ KÝ + COOKIE TỪ F12
# -----------------------------------------------------
# Lấy từ request 'seller/orders/list' (Copy as cURL). 4 tham số dưới nằm trong URL.
# =====================================================
MSTOKEN = "5KnMHUZBkxOXOYDMmCm0d1QcFJ4Tpa5Qu5SLTFOAaXSjD7Qjg10Je5Qbhc-8mj_kYpMyA4IpScALjVXf4lRs4x4oBHOwFAOM7dSZ1u7hscsGr9y9ruICO-RYickRcwkEadtKhDnqhOIbB-tyZvwpU-d2"
X_BOGUS = "DFSzswVOvWIzu5sJCUVVxu6-55xD"
X_GNARLY = "MJK3jccDxoxU6UlNQltdrkNNVmIcNPSmN9yGI8fghB4aAddNcr21GqSyWcOjnksvUwtyUoRKELLs7dj-uegWK7ah7WmVY0nl0zRs7-mbunRCeMp3HFWHlzK4mPwTzFB8JncyEq1zNrWmO3B0O9E9CaMzlcoLU238hItpe48xbAzSXbU/juiXPZIBioKeOdRQY-Ih6PEJOG1ubcNiOZ0DAd5cYlIlI7qDZV3IIV1DrCPVbPY5wj3dtWGxk-HJSlbqtRlvZ5zx5KtG"
X_TTS_OEC_BSID = "9a8c00004e202710000010eb4e210000019fb1eb3483000000111cf27404f345a3f92c7d0b4e8165a2a3a2a3a2a2a2a22e21f55f23673cefaf1cd470e309dbe79fb2fabf9965888405eaf47097f14f1c84cd25eeaf3d9b62cd79935f2569cac14890b412000003c9d1b80976070049010101006e216138712e4c73129192a2597956b0897ddb3656ee50dfc99bac3b8fdaaea1be22ff0cf8eb3f02e32df234a9bfac6f86bb086b1dbd73128a2e1bd8e70cf4f3de248b8ceb"

# Query string phải KHỚP y hệt browser (X-Gnarly ký theo query string).
URL = (
    "https://affiliate.tiktok.com/api/oec/pay/statement/order/seller/orders/list"
    "?user_language=vi-VN&aid=4331&app_name=i18n_ecom_alliance&device_id=0"
    "&fp=verify_ms5v5bxd_bmrH54Gf_KUVu_402D_8p79_RQInaD0PhSrW&device_platform=web"
    "&cookie_enabled=true&screen_width=1920&screen_height=1080&browser_language=vi"
    "&browser_platform=Win32&browser_name=Mozilla"
    "&browser_version=5.0+(Windows+NT+10.0%3B+Win64%3B+x64)+AppleWebKit%2F537.36+(KHTML,+like+Gecko)+Chrome%2F150.0.0.0+Safari%2F537.36"
    "&browser_online=true&timezone_name=Asia%2FBangkok"
    f"&oec_seller_id={ID_SHOP}&shop_region=VN"
    f"&msToken={MSTOKEN}&X-Bogus={X_BOGUS}&X-Gnarly={X_GNARLY}&X-Tts-Oec-Bsid={X_TTS_OEC_BSID}"
)

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "agw-js-conv": "str",
    "content-type": "application/json",
    "origin": "https://affiliate.tiktok.com",
    "referer": f"https://affiliate.tiktok.com/product/order?shop_region=VN&shop_id={ID_SHOP}",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
    "cookie": os.environ["TIKTOK_COOKIE_RHYSMAN"],
}

# Bộ filter = "LẤY TẤT CẢ ĐƠN" (đã test: cod_type=0, campaign_type=1, op_assist_order_type=2).
COD_TYPE = 0
CAMPAIGN_TYPE = 1
OP_ASSIST_ORDER_TYPE = 2
PAGE_SIZE = 50

# Số ngày kéo về (giống bản cũ)
DAYS_BACK = 31

# Các creator không được xóa dữ liệu cũ
PROTECTED_CREATORS = {
    "rhysman.com",
    "rhysman.shopping",
    "rhysman_channel",
}

# cos_ratio/standard_cos_ratio trả về dạng "13%". Để False -> lưu số 13.
# Đổi True nếu bảng cũ đang lưu dạng phân số 0.13.
RATIO_AS_FRACTION = False

# =====================================================
# CAPTCHA / ANTI-BOT / AUTH HANDLING
# =====================================================
MAX_CAPTCHA_RETRY = 5
CAPTCHA_BACKOFF_BASE = 8
CAPTCHA_MARKERS = (
    "captcha", "geetest", "secsdk-captcha", "slider_captcha",
    "verify_event", "verification_needed", "risk_control",
    "please verify", "security check", "sms_verify",
)
CAPTCHA_STATUS = (401, 403, 405, 412, 429)
# Mã code TikTok báo cần đăng nhập lại / phiên hết hạn.
LOGIN_CODES = (98001002,)

tz_vn = timezone(timedelta(hours=7))


class CaptchaBlocked(Exception):
    pass


def _is_captcha_response(resp) -> bool:
    if resp.status_code in CAPTCHA_STATUS:
        return True
    lowered = (resp.text or "").lower()
    if any(m in lowered for m in CAPTCHA_MARKERS):
        return True
    try:
        data = resp.json()
    except Exception:
        return resp.status_code >= 400
    if isinstance(data, dict):
        if data.get("code") in LOGIN_CODES:
            return True
        msg = str(data.get("message") or data.get("msg") or "").lower()
        if any(m in msg for m in CAPTCHA_MARKERS):
            return True
    return False


# =====================================================
# FETCH 1 TRANG (theo cursor) + phá captcha
# =====================================================
def fetch_page(cursor, start_ms, end_ms):
    body = {
        "query_size": PAGE_SIZE,
        "page_size": PAGE_SIZE,
        "affiliate_seller_search_condition": {
            "cod_type": COD_TYPE,
            "campaign_type": CAMPAIGN_TYPE,
            "op_assist_order_type": OP_ASSIST_ORDER_TYPE,
            "order_create_time": {"start_time": start_ms, "end_time": end_ms},
        },
    }
    if cursor:
        body["cursor"] = cursor

    # Gửi body raw giữ nguyên thứ tự key (không để requests đổi separator).
    raw = json.dumps(body, separators=(",", ":"))

    captcha_retry = 0
    while True:
        r = requests.post(URL, headers=HEADERS, data=raw, timeout=40)

        if _is_captcha_response(r):
            captcha_retry += 1
            if captcha_retry > MAX_CAPTCHA_RETRY:
                raise CaptchaBlocked(
                    f"Bị TikTok chặn (login/captcha) sau {MAX_CAPTCHA_RETRY} lần "
                    f"(HTTP {r.status_code}). Lấy lại msToken/X-Bogus/X-Gnarly/"
                    "X-Tts-Oec-Bsid + cookie mới từ F12 request 'orders/list'."
                )
            wait = min(CAPTCHA_BACKOFF_BASE * captcha_retry, 60)
            print(f"⚠️  Captcha/login (HTTP {r.status_code}) lần {captcha_retry}/{MAX_CAPTCHA_RETRY} -> chờ {wait}s...")
            time.sleep(wait)
            continue

        r.raise_for_status()
        data = r.json()

        code = data.get("code")
        if code != 0:
            # Endpoint chết cũ trả 98001008; các code khác cũng ném lên để retry.
            raise RuntimeError(f"API code={code}, message={data.get('message')}")

        return data.get("data", {}) or {}


# =====================================================
# PARSE
# =====================================================
def _amount(x):
    if isinstance(x, dict):
        return x.get("amount")
    return x


def _ratio(s):
    """'13%' -> 13 (hoặc 0.13 nếu RATIO_AS_FRACTION)."""
    if s is None:
        return None
    s = str(s).replace("%", "").replace(",", "").strip()
    if s == "":
        return None
    try:
        v = Decimal(s)
    except InvalidOperation:
        return None
    if RATIO_AS_FRACTION:
        v = v / Decimal("100")
    return str(v)


def parse_item(item):
    base = item.get("sku_order_base_info_for_affiliate_seller", {}) or {}
    comm = item.get("sku_order_commission_info_for_affiliate_seller", {}) or {}

    creator = base.get("creator_info", {}) or {}
    order = base.get("sku_order_info", {}) or {}

    main_order_id = order.get("main_order_id")
    create_time_ms = order.get("create_time")
    if not main_order_id or not create_time_ms:
        return None

    create_time = datetime.fromtimestamp(create_time_ms / 1000).replace(tzinfo=None)

    promotion_position_type = (order.get("promotion_position_info", {}) or {}).get("promotion_type")

    # Commission: tách theo cos_type (1=standard/thường, 0=shop ads).
    cos_ratio = None
    estimated_cos_fee = None
    shop_ads_commission_ratio = None
    estimated_shop_ads_commission = None

    if "standard_cos_ratio" in comm or "est_standard_commission" in comm:
        cos_ratio = _ratio(comm.get("standard_cos_ratio"))
        estimated_cos_fee = _amount(comm.get("est_standard_commission"))
    if "shop_ads_cos_ratio" in comm or "est_shop_ads_commission" in comm:
        shop_ads_commission_ratio = _ratio(comm.get("shop_ads_cos_ratio"))
        estimated_shop_ads_commission = _amount(comm.get("est_shop_ads_commission"))

    return (
        int(main_order_id),
        ID_SHOP,
        creator.get("creator_nickname"),
        creator.get("creator_username"),
        promotion_position_type,
        create_time,
        cos_ratio,
        estimated_cos_fee,
        shop_ads_commission_ratio,
        estimated_shop_ads_commission,
    )


# =====================================================
# RUN
# =====================================================
def run():
    now_vn = datetime.now(tz=tz_vn)
    start_day = (now_vn - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(start_day.timestamp() * 1000)
    end_ms = int(now_vn.timestamp() * 1000)

    print(f"Kéo đơn từ {start_day.date()} -> {now_vn.date()} (shop {ID_SHOP})")

    all_rows = []
    cursor = None
    page = 0
    empty_retry = 0

    while True:
        try:
            data = fetch_page(cursor, start_ms, end_ms)
        except Exception as e:
            print("API error retry:", e)
            time.sleep(5)
            continue

        orders = data.get("sku_order_list", []) or []
        total = data.get("total_count")
        print(f"Page {page} | orders: {len(orders)} | total_count: {total}")

        if not orders:
            empty_retry += 1
            if empty_retry >= 3:
                print("No more data.")
                break
            print("Empty page -> retry")
            time.sleep(2)
            continue
        empty_retry = 0

        for it in orders:
            row = parse_item(it)
            if row:
                all_rows.append(row)

        has_more = bool(data.get("has_more"))
        next_cursor = data.get("next_cursor")
        if not has_more or not next_cursor:
            break

        cursor = next_cursor
        page += 1
        time.sleep(0.3)

    print("TOTAL ROWS TO INSERT:", len(all_rows))
    if not all_rows:
        print("NO DATA TO INSERT")
        return

    df = pd.DataFrame(all_rows, columns=[
        "main_order_id",
        "id_shop",
        "creator_nickname",
        "creator_username",
        "promotion_position_type",
        "create_time",
        "cos_ratio",
        "estimated_cos_fee",
        "shop_ads_commission_ratio",
        "estimated_shop_ads_commission",
    ])

    df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

    def to_decimal(x):
        try:
            if pd.isna(x):
                return None
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None

    df["cos_ratio"] = df["cos_ratio"].apply(to_decimal)
    df["estimated_cos_fee"] = df["estimated_cos_fee"].apply(to_decimal)
    df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
    df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

    df = df.dropna(subset=["main_order_id"])




    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Chuẩn hóa creator_username để so sánh
    df["_creator_username_norm"] = (
        df["creator_username"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    
    protected_creator_sql = ", ".join(
        f"'{username}'" for username in sorted(PROTECTED_CREATORS)
    )
    
    # =====================================================
    # KIỂM TRA CÁC ĐƠN PROTECTED CREATOR ĐÃ CÓ TRONG BIGQUERY
    # =====================================================
    existing_protected_query = f"""
    SELECT DISTINCT
        CAST(main_order_id AS STRING) AS main_order_id
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE id_shop = '{ID_SHOP}'
      AND DATE(create_time) >= DATE_SUB(
            CURRENT_DATE("Asia/Ho_Chi_Minh"),
            INTERVAL {DAYS_BACK} DAY
          )
      AND LOWER(TRIM(creator_username)) IN ({protected_creator_sql})
    """
    
    existing_protected_rows = client.query(existing_protected_query).result()
    
    existing_protected_order_ids = {
        str(row.main_order_id)
        for row in existing_protected_rows
        if row.main_order_id is not None
    }
    
    # Các dòng thuộc 3 creator được bảo vệ
    protected_mask = df["_creator_username_norm"].isin(PROTECTED_CREATORS)
    
    # Chỉ loại khỏi DataFrame những đơn protected đã tồn tại
    duplicate_protected_mask = (
        protected_mask
        & df["main_order_id"].astype(str).isin(existing_protected_order_ids)
    )
    
    duplicate_protected_count = int(duplicate_protected_mask.sum())
    
    if duplicate_protected_count > 0:
        print(
            f"Giữ nguyên {duplicate_protected_count} dòng của protected creators "
            "đã có trong BigQuery, không insert lại."
        )
    
    df = df.loc[~duplicate_protected_mask].copy()
    df = df.drop(columns=["_creator_username_norm"])
    
    
    # =====================================================
    # DELETE 31 NGÀY GẦN NHẤT
    # KHÔNG XÓA 3 CREATOR ĐƯỢC BẢO VỆ
    # =====================================================
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE id_shop = '{ID_SHOP}'
      AND DATE(create_time) >= DATE_SUB(
            CURRENT_DATE("Asia/Ho_Chi_Minh"),
            INTERVAL {DAYS_BACK} DAY
          )
      AND COALESCE(LOWER(TRIM(creator_username)), '') NOT IN (
            {protected_creator_sql}
          )
    """
    
    client.query(delete_query).result()
    
    print(
        f"Đã xóa dữ liệu {DAYS_BACK} ngày gần nhất, "
        f"ngoại trừ các creator: {', '.join(sorted(PROTECTED_CREATORS))}"
    )
    
    
    # =====================================================
    # LOAD DỮ LIỆU
    # =====================================================
    if df.empty:
        print("Không có dữ liệu mới để insert.")
        return
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    
    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )
    
    job.result()
    
    print(f"✅ Loaded {len(df)} rows into BigQuery")



    





















#     table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

#     # ==============================
#     # DELETE 31 NGÀY GẦN NHẤT (GIỮ NGUYÊN LOGIC CŨ)
#     # ==============================
#     delete_query = f"""
#     DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
#     WHERE id_shop = '{ID_SHOP}' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL {DAYS_BACK} DAY)
#     """
#     client.query(delete_query).result()
#     print(f"Old data (last {DAYS_BACK} days) deleted.")

#     # ==============================
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
#     job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
#     job.result()

#     print(f"✅ Loaded {len(df)} rows into BigQuery")


# # =====================================================
# if __name__ == "__main__":
#     run()




























