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
CHANNEL = "Shopee"
STATE = "MỚI"
PAGE_SIZE = 25

HEADERS_BASE = {
    "Accept": "*/*",
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://stock.salework.net",
    "User-Agent": "Mozilla/5.0",
    "frontend_version": "1",
    "platform": "stock_webapp",
}

# đúng theo pattern request thật: từ 00:00 VN -> 23:59:59.999 VN
VN_TZ = timezone(timedelta(hours=7))
today_vn = datetime.now(VN_TZ).date()
start_day = today_vn - timedelta(days=6)

DATE_FROM = datetime.combine(start_day, datetime.min.time(), tzinfo=VN_TZ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = datetime.combine(today_vn, datetime.max.time().replace(microsecond=999000), tzinfo=VN_TZ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.999Z")

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
# LOGIN + CAPTURE REAL JWT TOKEN
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
                    real_token = body.get("token")

                    params = body.get("params", {})
                    if (
                        body.get("method") == "getOrderList"
                        and real_token
                        and params.get("channel") == CHANNEL
                    ):
                        captured["token"] = real_token
                        print("✅ Captured REAL JWT token")
                except Exception:
                    pass

        page.on("request", handle_request)

        print("🔐 Logging in Salework...")
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

        print("🌐 Current URL:", page.url)

        # nếu trang chưa tự bắn request thì reload thêm 1 lần
        if not captured["token"]:
            page.reload(wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(8000)

        if not captured["token"]:
            raise RuntimeError("Không bắt được REAL JWT token từ request thật của /orders")

        cookies = context.cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

        return captured["token"], cookie_str


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
# TRANSFORM
# ==============================
def build_dataframe(orders):
    rows = []

    for order in orders:
        item_price = (
            order.get("tiktok", {})
            .get("escrowDetails", {})
            .get("cost_of_goods_sold")
        )

        rows.append({
            "_id": order.get("_id"),
            "code": order.get("code"),
            "shopId": str(order.get("shopId") or order.get("tiktokShopId") or ""),
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
    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
    """
    client.query(delete_query).result()
    print("✅ Delete done")


def load_to_bigquery(df):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
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
    print("🔑 REAL TOKEN PREVIEW:", token[:60])

    orders = fetch_orders(token, cookie_str)
    print(f"📦 Total orders: {len(orders)}")

    if not orders:
        print("⚠️ No data")
        return

    df = build_dataframe(orders)
    if df.empty:
        print("⚠️ DataFrame rỗng")
        return

    delete_last_35_days()
    load_to_bigquery(df)


if __name__ == "__main__":
    main()
