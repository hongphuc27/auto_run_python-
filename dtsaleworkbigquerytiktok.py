# import requests
# import pandas as pd
# import time
# from datetime import datetime, timedelta, timezone
# from google.oauth2 import service_account
# from google.cloud import bigquery
# import os
# import json



# # ==============================
# # SALEWORK CONFIG
# # ==============================

# BASE_URL = "https://stock.salework.net/api/v2/order"
# TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6IjA5NDYzNTYxODAiLCJjb3VudHJ5IjoiVk4iLCJjb21wYW55Q29kZSI6InN3MzA4NzEiLCJjb21wYW55TmFtZSI6IlJoeXMgTWFuIiwicGVybWlzc2lvbiI6IjExMTEiLCJhdmF0YXIiOiJyZXNpemVfMTY4Mzc2ODQzMzUxNF_EkEVOLnBuZyIsImNyZWF0ZWRCeSI6InN5c3RlbSIsInNob3BBY2Nlc3MiOnsic2hvcGVlIjpbMjk0MDQxNjcwLDc4OTQxNzMwNyw3NTgxMTE0MjcsMjkzOTQ0Njc2XSwibGF6YWRhIjpbInJoeXNtYW4ud29ya0BnbWFpbC5jb20iLCJwaGFtbGViYW5ndGFtOTZAZ21haWwuY29tIiwiYmFuZ3RhbTIwMjJ4QGdtYWlsLmNvbSIsImNza2gucmh5c21hbkBnbWFpbC5jb20iLCI0bWFuLm9mZmljaWFsdm5AZ21haWwuY29tIl0sInRpa2kiOlsiRUM5NDYyMUVEMURDNTVGRTI0RjcwRDRENjg5NzQxOTkyRDQ3QThBMyJdLCJ0aWt0b2siOlsiNzQ5NDc1ODMyMDAyMzgzMzI2OCIsIjc0OTQ3NDMxMzQ2MTEyMTMwNzciLCI3NDk0NTQ1NjMwMDIyMjQwNDgxIl0sInN0b2NrIjpbIlNhbGV3b3JrIiwiU2FsZXdvcmtXYXJlaG91c2UiXX0sInJvbGVBY2NvdW50Ijp7Im9yZGVyc192aWV3Ijp0cnVlLCJwcm9kdWN0c192aWV3Ijp0cnVlLCJleHBvcnRfaW1wb3J0X3ZpZXciOnRydWUsImVjb21tZXJjZV9saW5rX3ZpZXciOnRydWUsIm9yZGVyc19lY29tbWVyY2VfaGFuZGxlIjp0cnVlLCJvcmRlcnNfbm90X2Vjb21tZXJjZV9oYW5kbGUiOnRydWUsInN0b2NrX2NydWQiOnRydWUsInN0b2NrX3N5bmNfZWNvbW1lcmNlIjp0cnVlLCJwcm9kdWN0c19jcnVkIjp0cnVlLCJwcm9kdWN0c19jb3B5X2Vjb21tZXJjZSI6dHJ1ZSwicmVwb3J0X2V4cG9ydF9pbXBvcnRfcHJvZHVjdHMiOnRydWUsInJlcG9ydF9vcmRlcnMiOnRydWUsImNvc3RfdmlldyI6dHJ1ZSwic2V0dGluZ19jcnVkIjp0cnVlLCJvcmRlcnNfYXNzaWduZWQiOmZhbHNlLCJvcmRlcnNfdmlydHVhbF92aWV3Ijp0cnVlLCJwcm9kdWN0c19naWZ0X3ZpZXciOnRydWV9LCJhY3RpdmVVc2VyIjpmYWxzZSwiaWF0IjoxNzc0NjA2OTg0LCJleHAiOjE3NzUyMTE3ODR9.dYf0FBDhRckRRgBCDAVwblstQ91uvkvJseoeKBB2Qm8"

# COMPANY_ID = "sw30871"
# CHANNEL = "Tiktok"


# now_utc = datetime.now(timezone.utc)

# DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
# DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# # DATE_FROM = datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc) \
# #     .strftime("%Y-%m-%dT%H:%M:%S.000Z")

# # DATE_TO = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc) \
# #     .strftime("%Y-%m-%dT%H:%M:%S.000Z")


# PAGE_SIZE = 550


# HEADERS = {
#     "accept": "application/json, text/plain, */*",
#     "content-type": "application/json",
#     "origin": "https://stock.salework.net",
#     "referer": "https://stock.salework.net/",
#     "user-agent": "Mozilla/5.0"
# }


# KEEP_COLUMNS = [
#     "_id","code","tiktokShopId","totalPrice","state","createdAt",
#     "channel","reconciled","estimateRevenue","customer_state",

#     "tiktok_payment_info_original_shipping_fee",
#     "tiktok_payment_info_original_total_product_price",
#     "tiktok_payment_info_platform_discount",
#     "tiktok_payment_info_seller_discount",
#     "tiktok_payment_info_shipping_fee",
#     "tiktok_payment_info_shipping_fee_platform_discount",
#     "tiktok_payment_info_shipping_fee_seller_discount",
#     "tiktok_payment_info_sub_total",
#     "tiktok_payment_info_taxes",
#     "tiktok_payment_info_total_amount"
# ]


# # ==============================
# # BIGQUERY CONFIG
# # ==============================

# PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
# DATASET_ID = "rhysman"
# TABLE_ID = "fact_orders_salework_tiktok"


# gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
# credentials = service_account.Credentials.from_service_account_info(gcp_key)

# client = bigquery.Client(
#     credentials=credentials,
#     project=PROJECT_ID
# )
# table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# # ==============================
# # FETCH DATA
# # ==============================

# def fetch_orders():
#     all_orders = []
#     start = 0

#     while True:
#         payload = {
#             "method": "getOrderList",
#             "params": {
#                 "start": start,
#                 "pageSize": PAGE_SIZE,
#                 "channel": CHANNEL,
#                 "state": "",
#                 "search": "",
#                 "company_id": COMPANY_ID,
#                 "timestart": DATE_FROM,
#                 "timeend": DATE_TO,
#                 "typeCreated": "createdAt"
#             },
#             "token": TOKEN
#         }

#         print(f"📥 Fetch start={start}")

#         r = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=30)
#         data = r.json()

#         orders = data.get("orders", [])

#         if not orders:
#             break

#         all_orders.extend(orders)
#         start += PAGE_SIZE
#         time.sleep(0.05)

#     return all_orders


# # ==============================
# # MAIN ETL
# # ==============================

# def main():
#     print("\n🚀 START SALEWORK → SQL SERVER ETL\n")

#     orders = fetch_orders()
#     print(f"\n📦 Total orders: {len(orders)}")

#     df = pd.json_normalize(orders, sep="_")

#     df = df[[c for c in KEEP_COLUMNS if c in df.columns]]
#     df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
#     df["createdAt"] = df["createdAt"].dt.tz_convert("Asia/Ho_Chi_Minh")

#     # print("\n📊 Columns:", list(df.columns))
#     # print("🧾 Rows:", len(df))

#     # 🔥 XÓA DỮ LIỆU 20 NGÀY GẦN NHẤT
#     print("\n🧹 Deleting last 20 days data in SQL...")

#     delete_query = f"""
#         DELETE FROM `{table_ref}`
#         WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
#     """

#     client.query(delete_query).result()

#     print("✅ Delete done")

#     # 💾 INSERT LẠI
#     print("\n💾 Writing to SQL Server...")

#     job_config = bigquery.LoadJobConfig(
#         write_disposition="WRITE_APPEND",
#         schema=[
#             bigquery.SchemaField("createdAt", "TIMESTAMP"),
#         ],
#     )

#     job = client.load_table_from_dataframe(
#         df,
#         table_ref,
#         job_config=job_config
#     )

#     job.result()

#     print("\n✅ DONE LOAD SQL SERVER")



# if __name__ == "__main__":
#     main()






import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.oauth2 import service_account
from google.cloud import bigquery


# ==============================
# CONFIG
# ==============================
LOGIN_URL = "https://salework.net/login"
ORDERS_URL = "https://stock.salework.net/orders"
BASE_URL = "https://stock.salework.net/api/v2/order"

SALEWORK_EMAIL = os.getenv("SALEWORK_EMAIL", "").strip()
SALEWORK_PASSWORD = os.getenv("SALEWORK_PASSWORD", "").strip()

COMPANY_ID = "sw30871"
CHANNEL = "Tiktok"
STATE = ""
PAGE_SIZE = 550

HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://stock.salework.net",
    "referer": "https://stock.salework.net/",
    "user-agent": "Mozilla/5.0"
}

KEEP_COLUMNS = [
    "_id", "code", "tiktokShopId", "totalPrice", "state", "createdAt",
    "channel", "reconciled", "estimateRevenue", "customer_state",

    "tiktok_payment_info_original_shipping_fee",
    "tiktok_payment_info_original_total_product_price",
    "tiktok_payment_info_platform_discount",
    "tiktok_payment_info_seller_discount",
    "tiktok_payment_info_shipping_fee",
    "tiktok_payment_info_shipping_fee_platform_discount",
    "tiktok_payment_info_shipping_fee_seller_discount",
    "tiktok_payment_info_sub_total",
    "tiktok_payment_info_taxes",
    "tiktok_payment_info_total_amount"
]

PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_orders_salework_tiktok"

gcp_key_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not gcp_key_raw:
    raise ValueError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON")

gcp_key = json.loads(gcp_key_raw)
credentials = service_account.Credentials.from_service_account_info(gcp_key)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ==============================
# DATE RANGE
# giữ logic ngày của code 1
# ==============================
VN_TZ = timezone(timedelta(hours=7))


def build_date_range():
    today_vn = datetime.now(VN_TZ).date()
    start_day_vn = today_vn - timedelta(days=22)

    start_dt_vn = datetime.combine(start_day_vn, datetime.min.time(), tzinfo=VN_TZ)
    end_dt_vn = datetime.combine(
        today_vn,
        datetime.max.time().replace(microsecond=999000),
        tzinfo=VN_TZ
    )

    date_from = start_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    date_to = end_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.999Z")
    return date_from, date_to


DATE_FROM, DATE_TO = build_date_range()


# ==============================
# VALIDATE
# ==============================
def validate_env():
    missing = []
    if not SALEWORK_EMAIL:
        missing.append("SALEWORK_EMAIL")
    if not SALEWORK_PASSWORD:
        missing.append("SALEWORK_PASSWORD")
    if not gcp_key_raw:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        raise ValueError(f"Thiếu environment variables: {', '.join(missing)}")


# ==============================
# LOGIN + CAPTURE REAL REQUEST TOKEN
# GIỮ NGUYÊN LOGIC CODE THỨ 1
# ==============================
def get_real_token_and_cookie():
    captured = {
        "token": None
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()
        page = context.new_page()

        def handle_request(request):
            if request.url.startswith(BASE_URL):
                try:
                    post_data = request.post_data
                    if not post_data:
                        return

                    body = json.loads(post_data)
                    params = body.get("params", {})
                    real_token = body.get("token")

                    if body.get("method") == "getOrderList" and real_token:
                        captured["token"] = real_token
                except Exception:
                    pass

        page.on("request", handle_request)

        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        page.locator('input[type="text"]').first.fill(SALEWORK_EMAIL)
        page.locator('input[type="password"]').first.fill(SALEWORK_PASSWORD)
        page.locator('button[type="submit"]').first.click()

        try:
            page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            pass

        page.goto(ORDERS_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        if not captured["token"]:
            page.reload(wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(8000)

        if not captured["token"]:
            raise RuntimeError("Không bắt được REAL JWT token từ request thật của /orders")

        cookies = context.cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

        browser.close()
        return captured["token"], cookie_str


# ==============================
# FETCH ORDERS
# logic request theo code 2, nhưng dùng token/cookie từ code 1
# ==============================
def fetch_orders(token, cookie_str):
    all_orders = []
    start = 0

    headers = HEADERS_BASE.copy()
    headers["Cookie"] = cookie_str

    while True:
        payload = {
            "method": "getOrderList",
            "params": {
                "start": start,
                "pageSize": PAGE_SIZE,
                "channel": CHANNEL,
                "state": STATE,
                "search": "",
                "company_id": COMPANY_ID,
                "timestart": DATE_FROM,
                "timeend": DATE_TO,
                "typeCreated": "createdAt"
            },
            "token": token
        }

        print(f"📥 Fetch start={start}")

        retry = 0
        while True:
            try:
                r = requests.post(BASE_URL, headers=headers, json=payload, timeout=60)
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                retry += 1
                print(f"⚠️ retry {retry}: {e}")

                if retry >= 5:
                    raise

                time.sleep(2 * retry)

        orders = data.get("orders", [])

        if not orders:
            break

        all_orders.extend(orders)

        if len(orders) < PAGE_SIZE:
            break

        start += PAGE_SIZE
        time.sleep(0.05)

    return all_orders


# ==============================
# TRANSFORM
# lấy logic normalize + KEEP_COLUMNS của code 2
# ==============================
def build_dataframe(orders):
    if not orders:
        return pd.DataFrame()

    df = pd.json_normalize(orders, sep="_")

    available_columns = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[available_columns].copy()

    if "createdAt" in df.columns:
        df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
        df["createdAt"] = df["createdAt"].dt.tz_convert("Asia/Ho_Chi_Minh")

    return df


# ========================================
# Xóa dữ liệu
# giữ logic xóa theo code 1
# ========================================
def delete_last_22_days():
    start_day_vn = datetime.now(VN_TZ).date() - timedelta(days=22)
    start_dt_vn = datetime.combine(start_day_vn, datetime.min.time(), tzinfo=VN_TZ)

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP('{start_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")}')
    """

    client.query(delete_query).result()
    print("✅ Deleted last 22 days data in BigQuery")


# ==============================
# BIGQUERY
# load theo kiểu code 2
# ==============================
def load_to_bigquery(df):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("createdAt", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )
    job.result()
    print("✅ DONE LOAD TO BIGQUERY")


# ==============================
# MAIN
# ==============================
def main():
    print("\n🚀 START SALEWORK TIKTOK → BIGQUERY ETL\n")
    print(f"DATE_FROM: {DATE_FROM}")
    print(f"DATE_TO  : {DATE_TO}")

    validate_env()

    token, cookie_str = get_real_token_and_cookie()
    orders = fetch_orders(token, cookie_str)

    print(f"\n📦 Total orders: {len(orders)}")

    if not orders:
        print("⚠️ No data")
        return

    df = build_dataframe(orders)

    if df.empty:
        print("⚠️ DataFrame rỗng")
        return

    delete_last_22_days()
    load_to_bigquery(df)


if __name__ == "__main__":
    main()




