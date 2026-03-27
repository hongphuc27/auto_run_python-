import os
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.cloud import bigquery
from google.oauth2 import service_account


# ==============================
# CONFIG
# ==============================

LOGIN_URL = "https://salework.net/login"
APP_URL = "https://finance.salework.net/adsExpenseTransaction"
API_URL = "https://finance.salework.net/api/saleExpense/getAdsExpenseTransactionsByDays"

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "rhysman-data-warehouse-488306")
DATASET_ID = os.getenv("BQ_DATASET_ID", "rhysman")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "fact_ads_shopee")
CHANNEL = os.getenv("SALEWORK_CHANNEL", "Shopee")

SALEWORK_EMAIL = os.getenv("SALEWORK_EMAIL", "").strip()
SALEWORK_PASSWORD = os.getenv("SALEWORK_PASSWORD", "").strip()
SALEWORK_COMPANYCODE = os.getenv("SALEWORK_COMPANYCODE", "").strip()

VN_TZ = timezone(timedelta(hours=7))
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{BQ_TABLE_ID}"


# ==============================
# BIGQUERY
# ==============================

gcp_key_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not gcp_key_raw:
    raise RuntimeError("Thiếu biến môi trường GOOGLE_SERVICE_ACCOUNT_JSON")

gcp_key = json.loads(gcp_key_raw)
credentials = service_account.Credentials.from_service_account_info(gcp_key)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


# ==============================
# HELPERS
# ==============================

def validate_config() -> None:
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
        raise RuntimeError(f"Thiếu biến môi trường: {', '.join(missing)}")


def now_vn() -> datetime:
    return datetime.now(VN_TZ)


def build_date_range() -> tuple[str, str]:
    today_vn = now_vn().date()
    start_day_vn = today_vn - timedelta(days=1)
    end_day_vn = today_vn

    start_dt_vn = datetime.combine(start_day_vn, datetime.min.time(), tzinfo=VN_TZ)
    end_dt_vn = datetime.combine(
        end_day_vn,
        datetime.max.time().replace(microsecond=0),
        tzinfo=VN_TZ,
    )

    date_from = start_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    date_to = end_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return date_from, date_to


def safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def extract_date(row: Dict[str, Any]) -> str:
    today_vn = now_vn().date().isoformat()

    for key in ["date", "created_at", "createdAt", "transactionDate"]:
        val = row.get(key)
        if val:
            return str(val)[:10]

    return today_vn


# ==============================
# PLAYWRIGHT LOGIN + TOKEN CAPTURE
# ==============================

def get_salework_token_and_cookie() -> tuple[str, str]:
    token_holder = {"token": None}
    cookie_holder = {"cookie": ""}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context()
        page = context.new_page()

        def handle_request(request):
            try:
                headers = request.all_headers()
                auth = headers.get("authorization") or headers.get("Authorization")
                if auth and auth.startswith("Bearer "):
                    token_holder["token"] = auth
            except Exception:
                pass

        page.on("request", handle_request)

        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        # Thử nhiều selector phổ biến
        email_selectors = [
            'input[type="email"]',
            'input[name="email"]',
            'input[placeholder*="Email"]',
            'input[placeholder*="email"]',
            'input[type="text"]',
        ]
        password_selectors = [
            'input[type="password"]',
            'input[name="password"]',
            'input[placeholder*="Mật khẩu"]',
            'input[placeholder*="Password"]',
        ]
        submit_selectors = [
            'button[type="submit"]',
            'button:has-text("Đăng nhập")',
            'button:has-text("Login")',
            'input[type="submit"]',
        ]

        email_filled = False
        for selector in email_selectors:
            try:
                page.locator(selector).first.fill(SALEWORK_EMAIL, timeout=3000)
                email_filled = True
                break
            except Exception:
                continue

        if not email_filled:
            browser.close()
            raise RuntimeError("Không tìm thấy ô nhập email trên trang login Salework.")

        password_filled = False
        for selector in password_selectors:
            try:
                page.locator(selector).first.fill(SALEWORK_PASSWORD, timeout=3000)
                password_filled = True
                break
            except Exception:
                continue

        if not password_filled:
            browser.close()
            raise RuntimeError("Không tìm thấy ô nhập mật khẩu trên trang login Salework.")

        submitted = False
        for selector in submit_selectors:
            try:
                page.locator(selector).first.click(timeout=3000)
                submitted = True
                break
            except Exception:
                continue

        if not submitted:
            browser.close()
            raise RuntimeError("Không tìm thấy nút đăng nhập trên trang login Salework.")

        # Chờ chuyển trang / load app
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            pass

        # Mở màn cần kích hoạt request finance
        try:
            page.goto(APP_URL, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass

        start = time.time()
        while time.time() - start < 30:
            if token_holder["token"]:
                break
            page.wait_for_timeout(1000)

        if not token_holder["token"]:
            content = page.content()[:2000]
            browser.close()
            raise RuntimeError(
                "Không bắt được Bearer token sau khi đăng nhập. "
                "Có thể selector login chưa khớp hoặc flow đăng nhập có bước phụ."
            )

        cookies = context.cookies()
        cookie_str = "; ".join([f'{c["name"]}={c["value"]}' for c in cookies])
        cookie_holder["cookie"] = cookie_str

        browser.close()

    return token_holder["token"], cookie_holder["cookie"]


# ==============================
# API
# ==============================

def build_request_headers(auth_token: str, cookie_str: str) -> Dict[str, str]:
    return {
        "Accept": "*/*",
        "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        "Authorization": auth_token,
        "companycode": SALEWORK_COMPANYCODE,
        "Content-Type": "application/json",
        "Cookie": cookie_str,
        "Origin": "https://finance.salework.net",
        "platform": "web",
        "Referer": "https://finance.salework.net/adsExpenseTransaction",
        "User-Agent": "Mozilla/5.0",
    }


def fetch_one_page(
    http_session: requests.Session,
    auth_token: str,
    cookie_str: str,
    date_from: str,
    date_to: str,
    start_page: int = 0,
    page_size: int = 500,
) -> Dict[str, Any]:
    payload = {
        "startPage": start_page,
        "sizePage": page_size,
        "startDate": date_from,
        "endDate": date_to,
        "channel": CHANNEL,
    }

    headers = build_request_headers(auth_token=auth_token, cookie_str=cookie_str)

    print("📤 PAYLOAD:", payload)
    resp = http_session.post(API_URL, headers=headers, json=payload, timeout=60)

    print("📥 STATUS:", resp.status_code)
    print("📥 RESPONSE TEXT:", resp.text[:500])

    try:
        return resp.json()
    except Exception:
        return {
            "success": False,
            "message": "response is not valid json",
            "raw_text": resp.text,
        }


def fetch_all_data(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    http_session = requests.Session()
    all_rows: List[Dict[str, Any]] = []
    start_page = 0
    page_size = 500

    print("🔐 Đang đăng nhập Salework để lấy token mới...")
    auth_token, cookie_str = get_salework_token_and_cookie()
    print("✅ Đã lấy token và cookie mới")

    while True:
        print(f"📥 Fetch page {start_page}")

        data = fetch_one_page(
            http_session=http_session,
            auth_token=auth_token,
            cookie_str=cookie_str,
            date_from=date_from,
            date_to=date_to,
            start_page=start_page,
            page_size=page_size,
        )

        if not data.get("success"):
            message = str(data.get("message", ""))

            # token hết hạn giữa chừng thì login lại 1 lần
            if "expired" in message.lower():
                print("♻️ Token hết hạn, đang login lại...")
                auth_token, cookie_str = get_salework_token_and_cookie()
                data = fetch_one_page(
                    http_session=http_session,
                    auth_token=auth_token,
                    cookie_str=cookie_str,
                    date_from=date_from,
                    date_to=date_to,
                    start_page=start_page,
                    page_size=page_size,
                )

            if not data.get("success"):
                raise RuntimeError(f"API lỗi: {data.get('message')}")

        payload_data = data.get("data", {})
        items = payload_data.get("tableData", [])
        total = payload_data.get("total")

        print(f"✅ items len: {len(items)} | total: {total}")

        if not items:
            print("✅ No more data, stop")
            break

        all_rows.extend(items)

        if total and len(all_rows) >= total:
            print("✅ Reached total records, stop")
            break

        start_page += 1

    return all_rows


# ==============================
# BIGQUERY
# ==============================

def ensure_table_exists() -> None:
    try:
        bq_client.get_table(table_ref)
        print(f"✅ Table exists: {table_ref}")
    except Exception:
        schema = [
            bigquery.SchemaField("shopId", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("vat", "FLOAT64"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"✅ Created table: {table_ref}")


def delete_recent_rows() -> None:
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE date >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 1 DAY)
    """
    print("🧹 Deleting yesterday and today...")
    bq_client.query(query).result()
    print("✅ Delete done")


def load_to_bigquery(rows: List[Dict[str, Any]]) -> None:
    normalized_rows = []

    for row in rows:
        normalized_rows.append({
            "shopId": str(row.get("shopId", "")),
            "date": extract_date(row),
            "amount": safe_float(row.get("amount")),
            "vat": safe_float(row.get("vat")),
        })

    ensure_table_exists()

    job = bq_client.load_table_from_json(
        normalized_rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("shopId", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("amount", "FLOAT64"),
                bigquery.SchemaField("vat", "FLOAT64"),
            ],
        ),
    )
    job.result()

    print(f"✅ Loaded {len(normalized_rows)} rows into {table_ref}")


# ==============================
# MAIN
# ==============================

def main():
    print("🚀 START SALEWORK SHOPEE ETL")
    print("📌 TABLE:", table_ref)

    validate_config()

    date_from, date_to = build_date_range()
    print("📅 DATE_FROM:", date_from)
    print("📅 DATE_TO  :", date_to)

    rows = fetch_all_data(date_from=date_from, date_to=date_to)
    print(f"📦 Total rows fetched: {len(rows)}")

    if not rows:
        print("⚠️ No data")
        return

    delete_recent_rows()
    load_to_bigquery(rows)

    print("🎉 DONE")


if __name__ == "__main__":
    main()
