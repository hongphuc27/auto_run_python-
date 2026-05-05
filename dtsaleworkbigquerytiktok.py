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
    "tiktok_payment_info_total_amount",
    "item_id",
    "item_name",
    "model_id",
    "model_name",
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
    start_day_vn = today_vn - timedelta(days=25)

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

    # Giữ logic cũ
    df = pd.json_normalize(orders, sep="_")

    # Lấy thêm 4 cột từ tiktok.items
    item_rows = []

    for order in orders:
        order_id = order.get("_id")
        items = order.get("tiktok", {}).get("items", [])

        if items:
            for item in items:
                item_rows.append({
                    "_id": order_id,
                    "item_id": item.get("item_id"),
                    "item_name": item.get("item_name"),
                    "model_id": item.get("model_id"),
                    "model_name": item.get("model_name"),
                })
        else:
            item_rows.append({
                "_id": order_id,
                "item_id": None,
                "item_name": None,
                "model_id": None,
                "model_name": None,
            })

    items_df = pd.DataFrame(item_rows)

    # Merge thêm item vào dataframe chính
    df = df.merge(items_df, on="_id", how="left")

    # Giữ logic KEEP_COLUMNS cũ
    available_columns = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[available_columns].copy()

    # Giữ logic convert createdAt cũ
    if "createdAt" in df.columns:
        df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
        df["createdAt"] = df["createdAt"].dt.tz_convert("Asia/Ho_Chi_Minh")

    return df

# ========================================
# Xóa dữ liệu
# giữ logic xóa theo code 1
# ========================================
def delete_last_22_days():
    start_day_vn = datetime.now(VN_TZ).date() - timedelta(days=25)
    start_dt_vn = datetime.combine(start_day_vn, datetime.min.time(), tzinfo=VN_TZ)

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP('{start_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")}')
    """

    client.query(delete_query).result()
    print("✅ Deleted last 22 days data in BigQuery")


# ==============================
# BIGQUERY
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




