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
PAGE_SIZE = 500

HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://stock.salework.net",
    "referer": "https://stock.salework.net/",
    "user-agent": "Mozilla/5.0",
}


# ==============================
# DATE RANGE
# ==============================
now_utc = datetime.now(timezone.utc)
DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# test ngắn hơn nếu cần
# DATE_FROM = (now_utc - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
# DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


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
# LOGIN
# ==============================
def login_and_get_page():
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"]
    )
    context = browser.new_context()
    page = context.new_page()

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

    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        pass

    page.wait_for_timeout(5000)
    print("🌐 Current URL:", page.url)

    return p, browser, context, page


# ==============================
# FETCH DATA IN BROWSER SESSION
# ==============================
def fetch_orders_via_browser(page):
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
            }
        }

        print(f"📥 Fetch start={start}")

        result = page.evaluate(
            """
            async ({ url, payload, companycode }) => {
                try {
                    const res = await fetch(url, {
                        method: "POST",
                        credentials: "include",
                        headers: {
                            "accept": "application/json, text/plain, */*",
                            "content-type": "application/json",
                            "companycode": companycode,
                            "platform": "web"
                        },
                        body: JSON.stringify(payload)
                    });

                    const text = await res.text();
                    let jsonData = null;

                    try {
                        jsonData = JSON.parse(text);
                    } catch (e) {}

                    return {
                        ok: res.ok,
                        status: res.status,
                        text: text,
                        data: jsonData
                    };
                } catch (e) {
                    return {
                        ok: false,
                        status: 0,
                        text: String(e),
                        data: null
                    };
                }
            }
            """,
            {
                "url": BASE_URL,
                "payload": payload,
                "companycode": SALEWORK_COMPANYCODE
            }
        )

        print("STATUS:", result["status"])
        print("RESPONSE:", result["text"][:1000])

        if not result["ok"]:
            raise RuntimeError(f"Browser fetch failed: {result['text']}")

        data = result.get("data") or {}

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

    p = browser = context = page = None

    try:
        p, browser, context, page = login_and_get_page()
        orders = fetch_orders_via_browser(page)

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

    finally:
        if page:
            page.close()
        if context:
            context.close()
        if browser:
            browser.close()
        if p:
            p.stop()


if __name__ == "__main__":
    main()
