import os
import json
import base64
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.oauth2 import service_account
from google.cloud import bigquery


# ==============================
# SALEWORK CONFIG
# ==============================

BASE_URL = "https://finance.salework.net/api/saleExpense/getAdsExpenseTransactionsByDays"
APP_URL = "https://finance.salework.net/"

COMPANY_ID = os.getenv("SALEWORK_COMPANY_ID", "sw30871")
CHANNEL = os.getenv("SALEWORK_CHANNEL", "Shopee")
PAGE_SIZE = int(os.getenv("SALEWORK_PAGE_SIZE", "500"))

# Timeout Playwright
PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "10000"))

# File runtime trong GitHub Actions / local
STATE_FILE = Path("salework_state.json")
TOKEN_FILE = Path("token.txt")

# Cookie phụ trợ nếu muốn ép thêm. Thường để rỗng.
STATIC_COOKIE = os.getenv("SALEWORK_STATIC_COOKIE", "").strip()

# Nếu app chỉ sinh Bearer khi vào màn cụ thể thì thêm URL ở đây.
# Có thể set từ secret/env bằng JSON array hoặc comma-separated string.
default_trigger_urls = ["https://finance.salework.net/adsExpenseTransaction"]
trigger_urls_env = os.getenv("TOKEN_TRIGGER_URLS", "").strip()

if trigger_urls_env:
    try:
        TOKEN_TRIGGER_URLS = json.loads(trigger_urls_env)
        if not isinstance(TOKEN_TRIGGER_URLS, list):
            TOKEN_TRIGGER_URLS = default_trigger_urls
    except Exception:
        TOKEN_TRIGGER_URLS = [x.strip() for x in trigger_urls_env.split(",") if x.strip()]
        if not TOKEN_TRIGGER_URLS:
            TOKEN_TRIGGER_URLS = default_trigger_urls
else:
    TOKEN_TRIGGER_URLS = default_trigger_urls


# ==============================
# DATE RANGE
# ==============================

def build_date_range():
    """
    Mặc định lấy 7 ngày gần nhất theo múi giờ VN.
    Có thể override bằng env:
      - DATE_FROM
      - DATE_TO
    """
    date_from_env = os.getenv("DATE_FROM", "").strip()
    date_to_env = os.getenv("DATE_TO", "").strip()

    if date_from_env and date_to_env:
        return date_from_env, date_to_env

    now_utc = datetime.now(timezone.utc)
    now_vn = now_utc + timedelta(hours=7)

    today_vn = now_vn.date()
    start_day_vn = today_vn - timedelta(days=1)

    date_from = datetime(
        start_day_vn.year, start_day_vn.month, start_day_vn.day,
        0, 0, 0, tzinfo=timezone.utc
    ) - timedelta(hours=7)

    date_to = datetime(
        today_vn.year, today_vn.month, today_vn.day,
        23, 59, 59, tzinfo=timezone.utc
    ) - timedelta(hours=7)

    return (
        date_from.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        date_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )


DATE_FROM, DATE_TO = build_date_range()


# ==============================
# BIGQUERY CONFIG
# ==============================

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "rhysman-data-warehouse-488306")
DATASET_ID = os.getenv("BQ_DATASET_ID", "rhysman")
TABLE_ID = os.getenv("BQ_TABLE_ID", "fact_ads_shopee")

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ==============================
# BOOTSTRAP RUNTIME FILES
# ==============================

def bootstrap_runtime_files():
    """
    Tạo token.txt và salework_state.json từ GitHub Secrets / env nếu có.
    """
    token_from_env = os.getenv("SALEWORK_ACCESS_TOKEN", "").strip()
    state_from_env = os.getenv("SALEWORK_STORAGE_STATE_JSON", "").strip()

    if token_from_env:
        TOKEN_FILE.write_text(token_from_env, encoding="utf-8")

    if state_from_env:
        STATE_FILE.write_text(state_from_env, encoding="utf-8")


# ==============================
# JWT HELPERS
# ==============================

def decode_jwt_payload(token: str):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None

        payload = parts[1]
        padding = "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload + padding)
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        return None


def get_token_exp(token: str):
    payload = decode_jwt_payload(token)
    if not payload:
        return None
    return payload.get("exp")


def is_token_expiring_soon(token: str, buffer_seconds: int = 120):
    exp = get_token_exp(token)
    if not exp:
        return True

    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (exp - now_ts) <= buffer_seconds


def save_token(token: str):
    TOKEN_FILE.write_text(token.strip(), encoding="utf-8")


def load_token_from_file():
    if TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()
        return token or None
    return None


# ==============================
# PLAYWRIGHT TOKEN CAPTURE
# ==============================

def get_token_from_playwright(timeout_ms: int = PLAYWRIGHT_TIMEOUT_MS) -> str:
    """
    Dùng Playwright headless để:
    - nạp session cũ từ salework_state.json nếu có
    - mở app
    - nghe request network
    - bắt Authorization: Bearer ...
    """
    token_holder = {"token": None}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        if STATE_FILE.exists():
            context = browser.new_context(storage_state=str(STATE_FILE))
        else:
            context = browser.new_context()

        page = context.new_page()

        def handle_request(request):
            try:
                headers = request.all_headers()
                auth = headers.get("authorization")
                if auth and auth.lower().startswith("bearer "):
                    token_holder["token"] = auth[7:].strip()
            except Exception:
                pass

        page.on("request", handle_request)

        for url in TOKEN_TRIGGER_URLS:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(3000)

                start = time.time()
                while time.time() - start < (timeout_ms / 1000):
                    if token_holder["token"]:
                        context.storage_state(path=str(STATE_FILE))
                        save_token(token_holder["token"])
                        context.close()
                        browser.close()
                        return token_holder["token"]
                    page.wait_for_timeout(500)

            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue

        # Fallback chờ thêm
        start = time.time()
        while time.time() - start < max(60, timeout_ms / 1000):
            if token_holder["token"]:
                context.storage_state(path=str(STATE_FILE))
                save_token(token_holder["token"])
                context.close()
                browser.close()
                return token_holder["token"]
            page.wait_for_timeout(500)

        context.storage_state(path=str(STATE_FILE))
        context.close()
        browser.close()

    raise RuntimeError(
        "Không lấy được Bearer token từ Playwright. "
        "Khả năng cao SALEWORK_STORAGE_STATE_JSON đã hết hạn hoặc URL trigger chưa đúng."
    )


def load_or_create_access_token() -> str:
    """
    Ưu tiên:
    1) token trong env nếu còn hạn
    2) token.txt nếu còn hạn
    3) mở Playwright để lấy token mới từ session state
    """
    token_from_env = os.getenv("SALEWORK_ACCESS_TOKEN", "").strip()
    if token_from_env and not is_token_expiring_soon(token_from_env, buffer_seconds=30):
        return token_from_env

    token = load_token_from_file()
    if token and not is_token_expiring_soon(token, buffer_seconds=30):
        return token

    print("🔄 Không có token hợp lệ, đang dùng Playwright để lấy token mới...")
    return get_token_from_playwright()


# ==============================
# SALEWORK CLIENT
# ==============================

class SaleworkClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "*/*",
            "content-type": "application/json",
            "companycode": COMPANY_ID,
            "platform": "web",
            "origin": APP_URL.rstrip("/"),
            "referer": APP_URL,
            "user-agent": "Mozilla/5.0",
        })

        if STATIC_COOKIE:
            self.session.headers["cookie"] = STATIC_COOKIE

        self._apply_auth_header()

    def _apply_auth_header(self):
        self.session.headers["authorization"] = f"Bearer {self.access_token}"

    def refresh_access_token(self):
        print("🔄 Token sắp hết hạn hoặc đã hết hạn, đang lấy token mới bằng Playwright...")
        new_token = get_token_from_playwright()

        if not new_token:
            raise RuntimeError("Không lấy được token mới từ Playwright")

        self.access_token = new_token
        self._apply_auth_header()
        print("✅ Đã cập nhật access token mới")
        payload = decode_jwt_payload(self.access_token)
        print("TOKEN USER:", payload.get("user_data", {}).get("username"))
        print("TOKEN COMPANY:", payload.get("user_data", {}).get("company", {}))
        print("TOKEN EXP:", payload.get("exp"))

    def ensure_valid_token(self):
        if is_token_expiring_soon(self.access_token, buffer_seconds=120):
            self.refresh_access_token()

    def post(self, url: str, json_payload: dict, timeout: int = 30):
        self.ensure_valid_token()

        response = self.session.post(url, json=json_payload, timeout=timeout)

        if response.status_code == 401:
            print("⚠️ API trả 401, thử lấy token mới rồi gọi lại...")
            self.refresh_access_token()
            response = self.session.post(url, json=json_payload, timeout=timeout)

        response.raise_for_status()
        return response


# ==============================
# FETCH DATA
# ==============================

def fetch_orders(sw_client: SaleworkClient):
    all_items = []
    page = 0
    total = None

    while True:
        payload = {
            "startPage": page,
            "sizePage": PAGE_SIZE,
            "startDate": DATE_FROM,
            "endDate": DATE_TO,
            "channel": CHANNEL
        }

        print(f"📥 Fetch page {page}")

        response = sw_client.post(BASE_URL, payload, timeout=30)

        print("STATUS:", response.status_code)
        print("RESPONSE TEXT:", response.text[:5000])
        print("PAYLOAD:", payload)
        
        resp = response.json()
        payload_data = resp.get("data", {})
        items = payload_data.get("tableData", [])
        total = payload_data.get("total", total)
        
        print(f"items len: {len(items)} | total: {total}")

        if not items:
            print("✅ No more data, stop")
            break

        all_items.extend(items)
        print(f"👉 Fetched: {len(all_items)} / {total}")

        if total is not None and len(all_items) >= total:
            print("✅ Reached total records, stop")
            break

        page += 1
        time.sleep(0.1)

    return all_items


# ==============================
# TRANSFORM
# ==============================

def build_dataframe(orders):
    rows = []
    for item in orders:
        rows.append({
            "shopId": str(item.get("shopId", "")),
            "date": item.get("date"),
            "amount": item.get("amount"),
            "vat": item.get("vat"),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.drop_duplicates(subset=["shopId", "date", "amount", "vat"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["vat"] = pd.to_numeric(df["vat"], errors="coerce")

    return df


# ==============================
# LOAD TO BIGQUERY
# ==============================

def delete_recent_rows():
    print("\n🧹 Deleting last 7 days...")
    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    bq_client.query(delete_query).result()
    print("✅ Delete done")


def load_to_bigquery(df: pd.DataFrame):
    print("\n💾 Loading to BigQuery...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = bq_client.load_table_from_dataframe(
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
    bootstrap_runtime_files()

    print("\n🚀 START SALEWORK SHOPEE → BIGQUERY ETL\n")
    print(f"DATE_FROM: {DATE_FROM}")
    print(f"DATE_TO  : {DATE_TO}")
    print(f"TABLE    : {table_ref}")

    access_token = load_or_create_access_token()
    sw_client = SaleworkClient(access_token=access_token)

    exp = get_token_exp(sw_client.access_token)
    if exp:
        exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=7))
        )
        print("🔐 Current token expires at:", exp_dt.strftime("%Y-%m-%d %H:%M:%S"))

    orders = fetch_orders(sw_client)
    print(f"\n📦 Total orders: {len(orders)}")

    if not orders:
        print("⚠️ No data")
        return

    df = build_dataframe(orders)

    print("\n📊 Columns:", list(df.columns))
    print("🧾 Rows:", len(df))

    if df.empty:
        print("⚠️ DataFrame rỗng, dừng load")
        return

    delete_recent_rows()
    load_to_bigquery(df)


if __name__ == "__main__":
    main()
