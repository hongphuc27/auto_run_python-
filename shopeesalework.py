import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.oauth2 import service_account
from google.cloud import bigquery
import urllib.parse
import base64


def decode_evn_token(evn_token):
    try:
        # B1: base64 decode trước
        decoded_b64 = base64.b64decode(evn_token).decode("utf-8")

        # B2: rồi mới URL decode
        decoded_str = urllib.parse.unquote(decoded_b64)

        return decoded_str
    except Exception as e:
        print("❌ Decode token failed:", str(e))
        return None



# ==============================
# SALEWORK CONFIG
# ==============================
LOGIN_URL = "https://salework.net/login"
ORDERS_URL = "https://stock.salework.net/orders"
BASE_URL = "https://stock.salework.net/api/v2/order"

SALEWORK_EMAIL = os.getenv("SALEWORK_EMAIL", "").strip()
SALEWORK_PASSWORD = os.getenv("SALEWORK_PASSWORD", "").strip()
SALEWORK_COMPANYCODE = os.getenv("SALEWORK_COMPANYCODE", "").strip()

COMPANY_ID = "sw30871"
CHANNEL = "Shopee"
STATE = ""
PAGE_SIZE = 500

HEADERS_BASE = {
    "Accept": "*/*",
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://stock.salework.net",
    "User-Agent": "Mozilla/5.0",
    "frontend_version": "1",
    "platform": "stock_webapp",
}


# ==============================
# DATE RANGE
# ==============================
now_utc = datetime.now(timezone.utc)
DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.999Z")

# test ngắn hơn nếu cần
# DATE_FROM = (now_utc - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
# DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.999Z")


# ==============================
# BIGQUERY CONFIG
# ==============================
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_orders_salework_shopee"

gcp_key_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not gcp_key_raw:
    raise ValueError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON")

gcp_key = json.loads(gcp_key_raw)
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ==============================
# VALIDATE ENV
# ==============================
def validate_env():
    missing = []

    if not SALEWORK_EMAIL:
        missing.append("SALEWORK_EMAIL")
    if not SALEWORK_PASSWORD:
        missing.append("SALEWORK_PASSWORD")
    if not SALEWORK_COMPANYCODE:
        missing.append("SALEWORK_COMPANYCODE")
    if not gcp_key_raw:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")

    if missing:
        raise ValueError(f"Thiếu environment variables: {', '.join(missing)}")


# ==============================
# LOGIN -> TOKEN + COOKIE
# ==============================
def get_salework_token_and_cookie():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            print("🔐 Logging in Salework...")
            print("SALEWORK_EMAIL loaded:", bool(SALEWORK_EMAIL))
            print("SALEWORK_PASSWORD loaded:", bool(SALEWORK_PASSWORD))
            print("SALEWORK_COMPANYCODE loaded:", bool(SALEWORK_COMPANYCODE))

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
            page.wait_for_timeout(5000)

            print("🌐 Current URL:", page.url)

            cookies = context.cookies()
            token = None

            # Ưu tiên token trong cookie evn-token
            for c in cookies:
                if c["name"] == "evn-token" and c["value"]:
                    raw_token = c["value"]
                    token = decode_evn_token(raw_token)
                    print("✅ Found token in cookie: evn-token")
                    print("✅ Decoded token preview:", token[:50] if token else None)
                    break

            # Fallback localStorage
            if not token:
                try:
                    local_storage = json.loads(page.evaluate("() => JSON.stringify(window.localStorage)"))
                    print("🧪 localStorage keys:", list(local_storage.keys()))
                    for key, value in local_storage.items():
                        if "token" in key.lower() and value:
                            token = value
                            print(f"✅ Found token in localStorage: {key}")
                            break
                except Exception:
                    pass

            # Fallback sessionStorage
            if not token:
                try:
                    session_storage = json.loads(page.evaluate("() => JSON.stringify(window.sessionStorage)"))
                    print("🧪 sessionStorage keys:", list(session_storage.keys()))
                    for key, value in session_storage.items():
                        if "token" in key.lower() and value:
                            token = value
                            print(f"✅ Found token in sessionStorage: {key}")
                            break
                except Exception:
                    pass

            if not token:
                raise RuntimeError("Không lấy được token sau khi đăng nhập")

            cookie_str = "; ".join([c["name"] + "=" + c["value"] for c in cookies])
            return token, cookie_str

        finally:
            browser.close()


# ==============================
# FETCH ORDERS
# ==============================
def fetch_orders(token, cookie_str):
    all_orders = []
    start = 0
    session = requests.Session()

    while True:
        payload = {
            "method": "getOrderList",
            "params": {
                "start": start,
                "pageSize": PAGE_SIZE,
                "channel": CHANNEL,
                "state": STATE,
                "search": "",
                "timestart": DATE_FROM,
                "timeend": DATE_TO,
                "typeCreated": "createdAt",
                "company_id": COMPANY_ID
            },
            "token": token
        }

        headers = HEADERS_BASE.copy()
        headers["Cookie"] = cookie_str

        print(f"📥 Fetch start={start}")
        print("PAYLOAD:", json.dumps(payload, ensure_ascii=False))

        response = session.post(BASE_URL, headers=headers, json=payload, timeout=60)

        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text[:1000])

        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            raise RuntimeError(f"API error: {data.get('error')}")

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
# BIGQUERY
# ==============================
def delete_last_35_days():
    print("\n🧹 Deleting last 35 days in BigQuery...")

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
    """

    client.query(delete_query).result()
    print("✅ Delete done")


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
    print("\n🚀 START SALEWORK SHOPEE → BIGQUERY ETL\n")
    print(f"🗓️ DATE_FROM: {DATE_FROM}")
    print(f"🗓️ DATE_TO  : {DATE_TO}")

    validate_env()

    token, cookie_str = get_salework_token_and_cookie()
    orders = fetch_orders(token, cookie_str)

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
