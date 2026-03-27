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

# Test ngắn hơn nếu cần:
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
# LOGIN + GET AUTH TOKEN + COOKIE
# ==============================
def get_salework_auth():
    result = {
        "auth_token": None,
        "cookie_str": None
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()

        # Hook fetch + XHR để bắt Authorization header do chính app Salework gắn vào
        context.add_init_script("""
            (() => {
                window.__capturedAuthToken = null;

                const origFetch = window.fetch;
                window.fetch = async (...args) => {
                    try {
                        const init = args[1] || {};
                        const headers = init.headers || {};
                        let auth = null;

                        if (headers instanceof Headers) {
                            auth = headers.get('Authorization') || headers.get('authorization');
                        } else if (Array.isArray(headers)) {
                            for (const [k, v] of headers) {
                                if (String(k).toLowerCase() === 'authorization') {
                                    auth = v;
                                    break;
                                }
                            }
                        } else {
                            auth = headers.Authorization || headers.authorization || null;
                        }

                        if (auth && !window.__capturedAuthToken) {
                            window.__capturedAuthToken = auth;
                        }
                    } catch (e) {}
                    return origFetch(...args);
                };

                const origOpen = XMLHttpRequest.prototype.open;
                const origSetHeader = XMLHttpRequest.prototype.setRequestHeader;

                XMLHttpRequest.prototype.open = function(...args) {
                    this.__headers = {};
                    return origOpen.apply(this, args);
                };

                XMLHttpRequest.prototype.setRequestHeader = function(name, value) {
                    try {
                        this.__headers[name] = value;
                        if (String(name).toLowerCase() === 'authorization' && !window.__capturedAuthToken) {
                            window.__capturedAuthToken = value;
                        }
                    } catch (e) {}
                    return origSetHeader.apply(this, arguments);
                };
            })();
        """)

        page = context.new_page()

        try:
            print("🔐 Logging in Salework and getting latest token...")
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

            page.wait_for_timeout(8000)
            print("🌐 Current URL:", page.url)

            # reload 1 lần để app tự bắn request nội bộ
            page.reload(wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(5000)

            # 1) ưu tiên token app tự gắn vào request
            auth_token = page.evaluate("() => window.__capturedAuthToken")

            # 2) fallback: quét localStorage
            if not auth_token:
                try:
                    local_storage = json.loads(page.evaluate("() => JSON.stringify(window.localStorage)"))
                    print("🧪 localStorage keys:", list(local_storage.keys()))
                    for key, value in local_storage.items():
                        if not value:
                            continue

                        # key chứa token
                        if "token" in key.lower():
                            auth_token = value
                            break

                        # value là JSON chứa token
                        try:
                            parsed = json.loads(value) if isinstance(value, str) else value
                            if isinstance(parsed, dict):
                                for k2, v2 in parsed.items():
                                    if "token" in str(k2).lower() and v2:
                                        auth_token = v2
                                        break
                        except Exception:
                            pass

                        if auth_token:
                            break
                except Exception:
                    pass

            # 3) fallback: quét sessionStorage
            if not auth_token:
                try:
                    session_storage = json.loads(page.evaluate("() => JSON.stringify(window.sessionStorage)"))
                    print("🧪 sessionStorage keys:", list(session_storage.keys()))
                    for key, value in session_storage.items():
                        if not value:
                            continue

                        if "token" in key.lower():
                            auth_token = value
                            break

                        try:
                            parsed = json.loads(value) if isinstance(value, str) else value
                            if isinstance(parsed, dict):
                                for k2, v2 in parsed.items():
                                    if "token" in str(k2).lower() and v2:
                                        auth_token = v2
                                        break
                        except Exception:
                            pass

                        if auth_token:
                            break
                except Exception:
                    pass

            if auth_token and not str(auth_token).startswith("Bearer "):
                auth_token = f"Bearer {auth_token}"

            if not auth_token:
                raise RuntimeError("Không bắt được Authorization token sau khi đăng nhập")

            cookies = context.cookies()
            cookie_str = "; ".join([f'{c["name"]}={c["value"]}' for c in cookies])

            print("✅ Got latest token")
            return auth_token, cookie_str

        finally:
            browser.close()


# ==============================
# BUILD REQUEST HEADERS
# ==============================
def build_order_headers(auth_token, cookie_str):
    headers = HEADERS_BASE.copy()
    headers.update({
        "Authorization": auth_token,
        "companycode": SALEWORK_COMPANYCODE,
        "platform": "web",
        "Cookie": cookie_str,
    })
    return headers


# ==============================
# FETCH DATA
# ==============================
def fetch_orders(auth_token, cookie_str):
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
                "state": "",
                "search": "",
                "company_id": COMPANY_ID,
                "timestart": DATE_FROM,
                "timeend": DATE_TO,
                "typeCreated": "createdAt"
            }
        }

        headers = build_order_headers(auth_token, cookie_str)

        print(f"📥 Fetch start={start}")

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

    auth_token, cookie_str = get_salework_auth()
    orders = fetch_orders(auth_token, cookie_str)

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
