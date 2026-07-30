import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
import os
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json


# =====================================================
# BIGQUERY  CONFIG (THEO CỦA BẠN)
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_creator_tiktok"


gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


# =====================================================
# TIKTOK API (DÁN TỪ F12 → COPY AS CURL)
# =====================================================
URL = (
    "https://affiliate.tiktok.com/api/v1/affiliate/orders"
    "?user_language=vi-VN"
    "&aid=4331"
    "&app_name=i18n_ecom_alliance"
    "&device_platform=web"
    "&browser_language=vi"
    "&browser_platform=Win32"
    "&browser_name=Mozilla"
    "&timezone_name=Asia%2FSaigon"
    "&shop_region=VN"
    "&oec_seller_id=7494545630022240481"
    "&msToken=jF_qv_JGHymfc6FH5JfxGP8W7xJeAioYjAOmpdZEYZFEyMrH-0BKLXo-rapivzkzXcssb8CnCFjmCFiODR7SDX0AksmdSWdgdghDmvmBhnBs2phUrSuzBnVtYqmOl_ChVVhmy9O50Is4dhY-Bu_ucaXo"
    "&X-Bogus=DFSzswVOvWXPDOsJCUVVxW6-55y9"
    "&X-Gnarly=M/pvw5L9WLqUJl-3pVEm3x1fpDmJ1h7JyAtIVoZpObcN7ya3hOsEQvkU/O0zYsj62q9YD6g2FuPdCfiPUU9aHDxNIL/jzBS9M7lGWT2cSYRH6Y5C8dNyQsTaZP0OhipauFlnvHEmMuYf91bhZpoeTvs0CGw4h0tEIOGEJbDBmfVCHDhR0CPxP4xStKVArceX7we8Li3bW04rqV0heaCcYiqPt0hM06mz3C1w3d6d6binz1eV5dEiNhLRYqWT2LHhLgjx7cU0Eikc"
)


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://affiliate.tiktok.com",
    "referer": "https://affiliate.tiktok.com/product/order?shop_region=VN",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "cookie": os.environ["TIKTOK_COOKIE_RHYSMAN"]
}

# =====================================================
# =====================================================
tz_vn = timezone(timedelta(hours=7))

# =====================================================
def fetch_page(page: int, start_time, end_time):
    payload = {
    "conditions": {
        "time_period": {
            "beginning_time": str(start_time),
            "ending_time": str(end_time)
        }
    },
        "page": page,
        "page_size": 100
    }

    r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# =====================================================
def run():
    all_rows = []

    now_vn = datetime.now(tz=tz_vn)
    start_date = (now_vn - timedelta(days=31)).replace(tzinfo=None)
    end_date = now_vn.replace(tzinfo=None)

    current_date = start_date

    while current_date <= end_date:

        print("Fetching date:", current_date.date())

        START_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            0, 0, 0,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        END_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            23, 59, 59,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        page = 1
        empty_retry = 0

        while True:

            try:
                data = fetch_page(page, START_TIME, END_TIME)
            except Exception as e:
                print("API error retry:", e)
                time.sleep(5)
                continue

            orders = data.get("orders", [])

            print(f"Page {page} | Orders: {len(orders)}")

            if not orders:

                empty_retry += 1

                if empty_retry >= 3:
                    print("No more data for this day.")
                    break

                print("Empty page -> retry")
                time.sleep(2)
                continue

            empty_retry = 0

            for o in orders:

                main_order_id = o.get("main_order_id")
                create_time_ms = o.get("create_time")

                if not main_order_id or not create_time_ms:
                    continue

                create_time = datetime.fromtimestamp(
                    create_time_ms / 1000
                ).replace(tzinfo=None)

                sku_details = o.get("sku_detail", [])

                for sku in sku_details:

                    creator_nickname = sku.get("creator_nickname")
                    creator_username = sku.get("creator_username")

                    cos_ratio = sku.get("cos_ratio")
                    estimated_cos_fee = sku.get("estimated_cos_fee")

                    shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
                    estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")

                    promotion_position_type = (
                        sku.get("promotion_position", {})
                        .get("promotion_position_type")
                    )

                    all_rows.append((
                        int(main_order_id),
                        "7494545630022240481",
                        creator_nickname,
                        creator_username,
                        promotion_position_type,
                        create_time,
                        cos_ratio,
                        estimated_cos_fee,
                        shop_ads_commission_ratio,
                        estimated_shop_ads_commission
                    ))

            page += 1
            time.sleep(0.3)

        # sang ngày tiếp theo
        current_date += timedelta(days=1)
    
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
    "estimated_shop_ads_commission"
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

    # drop row lỗi
    df = df.dropna(subset=["main_order_id"])
        
    # df = df.drop_duplicates(
    # subset=["main_order_id", "promotion_position_type", "cos_ratio", "estimated_cos_fee", "shop_ads_commission_ratio", "estimated_shop_ads_commission"]
    # )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # ==============================
    # DELETE DATA TODAY + YESTERDAY
    # ==============================
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE id_shop = '7494545630022240481' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 31 DAY)  
    """

    client.query(delete_query).result()

    print("Old data (today + yesterday) deleted.")

    # ==============================

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



# =====================================================
if __name__ == "__main__":
    run()








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
#     "&msToken=ihykpW25F6VdjhjQU1DDg51GKyXup16943IkNc397v0qH4XA_n3jc1xbKm0aR1LIl43zjhxntq9Azpj1XfQYIulASgIHxSHaBT4HYdlWshBzB1TFg4Ju0ZALi-YVa3Y0ZDAqrg=="
#     "&X-Bogus=DFSzswVOU8YoToKzCi9B6QVRr3Nw"
#     "&X-Gnarly=MauTAkwV9g4WTiyTuFWfK/n0AeNkOgRztKODNVSh6M9DIaOzlP3WoSjeAy/7LGNmU-WY7dBUzWLsXgLZKbUjRgCXX3yuT58Y7/mL/XxgfRSDWMtjgNsMGWe/KVLGtvJ73b5BMFbDHUcdoxcA2RxI63nnVg1HWVd4JEEIsqCkAkXXveeMrRAxxXjzdxRyhsJYj9c5jW0nZtrXcLnRuMxCo0Wcx7ULSD4w9FqcVli1Sg6QbR7UXxmUe8M-cG8dybuoD0n2KrzV7wrY"
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
# # CAPTCHA / ANTI-BOT HANDLING (BỔ SUNG)
# # -----------------------------------------------------
# # Mô phỏng đúng cơ chế trong file keo_datalive_hairstyling.py:
# #   - Phát hiện response bị captcha / verify / risk-control.
# #   - Chờ (backoff) rồi thử lại.
# #   - Quá số lần cho phép -> ném lỗi rõ ràng yêu cầu lấy lại
# #     msToken / X-Bogus / X-Gnarly / cookie (giống init_browser
# #     yêu cầu cập nhật lại TT_COOKIE).
# # =====================================================

# # Số lần thử lại tối đa khi dính captcha trong 1 request, sau đó mới ném lỗi.
# MAX_CAPTCHA_RETRY = 5
# # Backoff cơ sở (giây). Lần thứ n sẽ chờ ~ CAPTCHA_BACKOFF_BASE * n (tối đa 60s).
# CAPTCHA_BACKOFF_BASE = 8

# # Các "marker" thường xuất hiện khi TikTok trả về trang/JSON xác minh.
# # Cố ý dùng chuỗi đặc trưng để tránh nhầm với dữ liệu đơn hàng bình thường
# # (KHÔNG dùng "verify" trần vì dễ false-positive).
# CAPTCHA_MARKERS = (
#     "captcha",
#     "geetest",
#     "secsdk-captcha",
#     "slider_captcha",
#     "verify_event",
#     "verification_needed",
#     "risk_control",
#     "please verify",
#     "security check",
#     "sms_verify",
# )

# # HTTP status đặc trưng khi bị risk-control chặn / rate-limit.
# CAPTCHA_STATUS = (401, 403, 405, 412, 429)

# # Các mã code của TikTok báo cần xác minh (tùy hệ thống, để trống nếu chưa chắc).
# # Anh có thể bổ sung thêm khi F12 thấy code cụ thể lúc bị chặn.
# CAPTCHA_CODES = ()


# class CaptchaBlocked(Exception):
#     """Ném ra khi TikTok chặn bằng captcha/verify và đã hết số lần thử lại."""
#     pass


# def _is_captcha_response(resp) -> bool:
#     """Xác định 1 response requests có phải bị captcha / verify / risk-control hay không.

#     Được thiết kế THẬN TRỌNG để không phá logic ngày rỗng của script:
#     - Ngày không có đơn (orders rỗng) KHÔNG bị coi là captcha.
#     - Chỉ coi là captcha khi có dấu hiệu xác minh rõ ràng (marker / status / code).
#     """
#     # 1) HTTP status đặc trưng của risk-control.
#     if resp.status_code in CAPTCHA_STATUS:
#         return True

#     text = resp.text or ""
#     lowered = text.lower()

#     # 2) Marker trong body (HTML trang verify hoặc JSON risk-control).
#     if any(m in lowered for m in CAPTCHA_MARKERS):
#         return True

#     # 3) Dấu hiệu trong JSON code / message.
#     try:
#         data = resp.json()
#     except Exception:
#         # Không parse được JSON: nếu status bất thường thì coi như bị chặn,
#         # còn 200 mà không phải JSON thì để luồng cũ xử lý.
#         return resp.status_code >= 400

#     if isinstance(data, dict):
#         if CAPTCHA_CODES and data.get("code") in CAPTCHA_CODES:
#             return True
#         msg = str(data.get("message") or data.get("msg") or "").lower()
#         if any(m in msg for m in CAPTCHA_MARKERS):
#             return True

#     return False

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

#     captcha_retry = 0

#     while True:
#         r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)

#         # ---- BỔ SUNG: phát hiện & phá captcha (backoff + retry) ----
#         if _is_captcha_response(r):
#             captcha_retry += 1

#             if captcha_retry > MAX_CAPTCHA_RETRY:
#                 raise CaptchaBlocked(
#                     "Bị TikTok chặn bằng captcha/verify sau "
#                     f"{MAX_CAPTCHA_RETRY} lần thử (HTTP {r.status_code}). "
#                     "Hãy mở F12 → Copy as cURL để lấy lại msToken / X-Bogus / "
#                     "X-Gnarly trong URL và cookie mới (TIKTOK_COOKIE_RHYSMAN)."
#                 )

#             wait_seconds = min(CAPTCHA_BACKOFF_BASE * captcha_retry, 60)
#             print(
#                 f"⚠️  Phát hiện captcha/verify (HTTP {r.status_code}) "
#                 f"- lần {captcha_retry}/{MAX_CAPTCHA_RETRY} -> chờ {wait_seconds}s rồi thử lại..."
#             )
#             time.sleep(wait_seconds)
#             continue
#         # ------------------------------------------------------------

#         r.raise_for_status()
#         return r.json()

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




















