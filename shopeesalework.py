import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from google.cloud import bigquery
from playwright.sync_api import sync_playwright
import os
import json


# ==============================
# SALEWORK CONFIG
# ==============================
BASE_URL = "https://stock.salework.net/api/v2/order"
LOGIN_URL = "https://stock.salework.net/login"
ORDERS_URL = "https://stock.salework.net/orders"

COMPANY_ID = "sw30871"
CHANNEL = "Shopee"
PAGE_SIZE = 500

now_utc = datetime.now(timezone.utc)
DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# Nếu muốn fix cứng ngày thì dùng:
# DATE_FROM = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
# DATE_TO = datetime(2026, 2, 24, 23, 59, 59, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://stock.salework.net",
    "referer": "https://stock.salework.net/",
    "user-agent": "Mozilla/5.0"
}

TOKEN = None


# ==============================
# BIGQUERY CONFIG
# ==============================
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_orders_salework_shopee"

gcp_key_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not gcp_key_raw:
    raise ValueError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON trong environment variables")

gcp_key = json.loads(gcp_key_raw)
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ==============================
# GET TOKEN FROM SALEWORK
# ==============================
def get_latest_token():
    username = os.getenv("SALEWORK_EMAIL")
    password = os.getenv("SALEWORK_PASSWORD")

    if not username or not password:
        raise ValueError("Thiếu SALEWORK_EMAIL hoặc SALEWORK_PASSWORD trong environment variables")

    print("🔐 Logging in SaleWork and getting latest token...")
    print("SALEWORK_EMAIL loaded:", bool(username))
    print("SALEWORK_PASSWORD loaded:", bool(password))

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            # 1. Mở login page
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)

            # 2. Điền tài khoản / mật khẩu
            page.fill('input[type="text"]', username)
            page.fill('input[type="password"]', password)
            page.click('button[type="submit"]')

            # 3. Chờ login xong rồi vào trang orders
            page.wait_for_load_state("networkidle", timeout=60000)
            page.goto(ORDERS_URL, wait_until="networkidle", timeout=60000)
            time.sleep(5)

            print("🌐 Current URL:", page.url)

            token = None

            # 4. Lấy từ localStorage
            local_storage_raw = page.evaluate("() => JSON.stringify(window.localStorage)")
            local_storage = json.loads(local_storage_raw)
            print("🧪 localStorage keys:", list(local_storage.keys()))

            for key, value in local_storage.items():
                if "token" in key.lower() and value:
                    token = value
                    print(f"✅ Found token in localStorage: {key}")
                    break

            # 5. Nếu chưa có thì lấy từ sessionStorage
            if not token:
                session_storage_raw = page.evaluate("() => JSON.stringify(window.sessionStorage)")
                session_storage = json.loads(session_storage_raw)
                print("🧪 sessionStorage keys:", list(session_storage.keys()))

                for key, value in session_storage.items():
                    if "token" in key.lower() and value:
                        token = value
                        print(f"✅ Found token in sessionStorage: {key}")
                        break

            # 6. Nếu vẫn chưa có thì lấy từ cookies
            if not token:
                cookies = context.cookies()
                for cookie in cookies:
                    if "token" in cookie["name"].lower() and cookie["value"]:
                        token = cookie["value"]
                        print(f"✅ Found token in cookie: {cookie['name']}")
                        break

            if not token:
                raise Exception(
                    "Không lấy được token ở trang /orders. "
                    "Hãy kiểm tra lại selector login hoặc nơi Salework lưu token."
                )

            return token

        finally:
            browser.close()


# ==============================
# FETCH DATA
# ==============================
def fetch_orders():
    global TOKEN

    if not TOKEN:
        raise ValueError("TOKEN đang rỗng. Hãy gọi get_latest_token() trước.")

    all_orders = []
    start = 0

    while True:
        payload = {
            "method": "getOrderList",
            "params": {
                "start": start,
                "pageSize": PAGE_SIZE,
                "channel": CHANNEL,
                "state": "",
                "search": "",
                "company_id": COMPANY_ID,
                "timestart": DATE_FROM,
                "timeend": DATE_TO,
                "typeCreated": "createdAt"
            },
            "token": TOKEN
        }

        print(f"📥 Fetch start={start}")

        r = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=60)

        # Nếu token lỗi/hết hạn thì tự refresh 1 lần
        if r.status_code in [401, 403]:
            print("🔄 Token expired or unauthorized. Refreshing token...")
            TOKEN = get_latest_token()
            payload["token"] = TOKEN
            r = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=60)

        r.raise_for_status()

        data = r.json()
        orders = data.get("orders", [])

        print(f"✅ start={start} | fetched={len(orders)}")

        if not orders:
            break

        all_orders.extend(orders)
        start += PAGE_SIZE
        time.sleep(0.05)

    return all_orders


# ==============================
# TRANSFORM DATA
# ==============================
def build_dataframe(orders):
    rows = []

    for order in orders:
        item_price = (
            order.get("shopee", {})
            .get("escrowDetails", {})
            .get("cost_of_goods_sold")
        )

        rows.append({
            "_id": order.get("_id"),
            "code": order.get("code"),
            "shopeeShopId": str(order.get("shopeeShopId")),
            "city": order.get("customer", {}).get("city"),
            "customer_state": order.get("customer", {}).get("state"),
            "order_state": order.get("state"),
            "createdAt": order.get("createdAt"),
            "price": item_price
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
    df["createdAt"] = df["createdAt"].dt.tz_convert("Asia/Ho_Chi_Minh")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


# ==============================
# DELETE OLD DATA IN BIGQUERY
# ==============================
def delete_last_35_days():
    print("\n🧹 Deleting last 35 days in BigQuery...")

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
    """

    client.query(delete_query).result()
    print("✅ Delete done")


# ==============================
# LOAD TO BIGQUERY
# ==============================
def load_to_bigquery(df):
    print("\n💾 Loading to BigQuery...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )

    job.result()
    print("✅ DONE LOAD SHOPEE → BIGQUERY")


# ==============================
# MAIN
# ==============================
def main():
    global TOKEN

    print("\n🚀 START SALEWORK SHOPEE → BIGQUERY ETL\n")
    print(f"🗓️ DATE_FROM: {DATE_FROM}")
    print(f"🗓️ DATE_TO  : {DATE_TO}")

    TOKEN = get_latest_token()
    print("✅ Got latest token")

    orders = fetch_orders()
    print(f"\n📦 Total orders: {len(orders)}")

    if not orders:
        print("⚠️ No data")
        return

    df = build_dataframe(orders)

    if df.empty:
        print("⚠️ DataFrame rỗng")
        return

    print("\n📊 Columns:", list(df.columns))
    print("🧾 Rows:", len(df))
    print(df.head(3))

    delete_last_35_days()
    load_to_bigquery(df)


if __name__ == "__main__":
    main()
