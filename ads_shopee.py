import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.cloud import bigquery
from google.oauth2 import service_account


# ==============================
# BIGQUERY CONFIG
# ==============================

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "rhysman-data-warehouse-488306")
DATASET_ID = os.getenv("BQ_DATASET_ID", "rhysman")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "fact_ads_shopee")

gcp_key_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not gcp_key_raw:
    raise RuntimeError("Thiếu biến môi trường GOOGLE_SERVICE_ACCOUNT_JSON")

gcp_key = json.loads(gcp_key_raw)
credentials = service_account.Credentials.from_service_account_info(gcp_key)

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{BQ_TABLE_ID}"


# ==============================
# APP CONFIG
# ==============================

PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "30000"))

STATE_FILE = Path("salework_state.json")
SESSION_FILE = Path("salework_session.json")

TOKEN_TRIGGER_URLS = [
    "https://finance.salework.net/adsExpenseTransaction",
]

API_URL = "https://finance.salework.net/api/saleExpense/getAdsExpenseTransactionsByDays"

CHANNEL = os.getenv("SALEWORK_CHANNEL", "Shopee")

VN_TZ = timezone(timedelta(hours=7))


# ==============================
# HELPERS
# ==============================

def now_vn() -> datetime:
    return datetime.now(VN_TZ)


def build_date_range() -> tuple[str, str]:
    """
    Lấy dữ liệu từ hôm qua 00:00:00 đến hôm nay 23:59:59 theo giờ Việt Nam,
    rồi convert sang UTC ISO format có đuôi Z để gửi API.
    """
    today_vn = now_vn().date()
    start_day_vn = today_vn - timedelta(days=1)
    end_day_vn = today_vn

    start_dt_vn = datetime.combine(start_day_vn, datetime.min.time(), tzinfo=VN_TZ)
    end_dt_vn = datetime.combine(
        end_day_vn,
        datetime.max.time().replace(microsecond=0),
        tzinfo=VN_TZ
    )

    date_from = start_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    date_to = end_dt_vn.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return date_from, date_to


def save_session(session_data: Dict[str, Any]) -> None:
    SESSION_FILE.write_text(
        json.dumps(session_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def load_session() -> Optional[Dict[str, Any]]:
    if not SESSION_FILE.exists():
        return None
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_cookie_string(cookies: List[Dict[str, Any]]) -> str:
    return "; ".join(
        f'{c["name"]}={c["value"]}'
        for c in cookies
        if c.get("name") and c.get("value")
    )


def normalize_bigquery_value(value: Any) -> Any:
    """
    Chuẩn hóa value trước khi insert vào BigQuery JSON.
    """
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    return str(value)


def normalize_row_for_bq(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: normalize_bigquery_value(v) for k, v in row.items()}


def infer_bq_schema_from_rows(rows: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    """
    Tạo schema linh hoạt từ dữ liệu trả về.
    Mặc định:
    - int -> INT64
    - float -> FLOAT64
    - bool -> BOOL
    - còn lại -> STRING
    """
    field_types: Dict[str, str] = {}

    for row in rows:
        for key, value in row.items():
            if value is None:
                field_types.setdefault(key, "STRING")
                continue

            if isinstance(value, bool):
                inferred = "BOOL"
            elif isinstance(value, int) and not isinstance(value, bool):
                inferred = "INT64"
            elif isinstance(value, float):
                inferred = "FLOAT64"
            else:
                inferred = "STRING"

            current = field_types.get(key)
            if current is None:
                field_types[key] = inferred
            elif current != inferred:
                field_types[key] = "STRING"

    # thêm cột date nếu chưa có
    if "date" not in field_types:
        field_types["date"] = "DATE"

    schema = [bigquery.SchemaField(name, field_type) for name, field_type in field_types.items()]
    return schema


# ==============================
# PLAYWRIGHT SESSION CAPTURE
# ==============================

def get_session_from_playwright(timeout_ms: int = PLAYWRIGHT_TIMEOUT_MS) -> Dict[str, Any]:
    """
    Lấy session thực tế từ browser:
    - authorization header (nếu có, ví dụ Bearer 1)
    - companycode
    - cookie string
    """
    session_holder: Dict[str, Any] = {
        "authorization": None,
        "companycode": None,
        "cookie": None,
        "captured": False,
    }

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
                url = request.url
                if "getAdsExpenseTransactionsByDays" not in url:
                    return

                headers = request.all_headers()

                auth = headers.get("authorization")
                companycode = headers.get("companycode")

                cookies = context.cookies()
                cookie_string = build_cookie_string(cookies)

                session_holder["authorization"] = auth
                session_holder["companycode"] = companycode
                session_holder["cookie"] = cookie_string
                session_holder["captured"] = True

                print("✅ FOUND API URL:", url)
                print("✅ authorization:", repr(auth))
                print("✅ companycode:", repr(companycode))
                print("✅ cookie len:", len(cookie_string))

            except Exception as e:
                print("handle_request error:", e)

        page.on("request", handle_request)

        try:
            for url in TOKEN_TRIGGER_URLS:
                try:
                    print(f"🔄 Mở URL trigger: {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    page.wait_for_timeout(5000)

                    start = time.time()
                    while time.time() - start < (timeout_ms / 1000):
                        if session_holder["captured"]:
                            context.storage_state(path=str(STATE_FILE))
                            save_session(session_holder)
                            return session_holder
                        page.wait_for_timeout(500)

                except PlaywrightTimeoutError:
                    print(f"⚠️ Timeout khi mở: {url}")
                    continue
                except Exception as e:
                    print(f"⚠️ Lỗi khi mở {url}: {e}")
                    continue

            print("⏳ Chưa bắt được session, chờ thêm...")
            start = time.time()
            while time.time() - start < max(60, timeout_ms / 1000):
                if session_holder["captured"]:
                    context.storage_state(path=str(STATE_FILE))
                    save_session(session_holder)
                    return session_holder
                page.wait_for_timeout(500)

        finally:
            try:
                context.storage_state(path=str(STATE_FILE))
            except Exception:
                pass
            context.close()
            browser.close()

    raise RuntimeError(
        "Không lấy được session từ Playwright. "
        "Khả năng cao salework_state.json đã hết hạn hoặc URL trigger chưa đúng."
    )


def load_or_create_session() -> Dict[str, Any]:
    try:
        print("🔄 Ưu tiên lấy session mới từ Playwright...")
        session_data = get_session_from_playwright()
        print("DEBUG session_data:", session_data)
        print("DEBUG cookie truthy:", bool(session_data.get("cookie")))

        if session_data.get("cookie"):
            return session_data

        raise RuntimeError("Session lấy từ Playwright chưa đủ dữ liệu.")
    except Exception as e:
        print("⚠️ Playwright lấy session lỗi:", e)

    cached = load_session()
    print("DEBUG cached session:", cached)
    if cached and cached.get("cookie"):
        print("✅ Dùng session từ file")
        return cached

    raise RuntimeError("Không có session hợp lệ")


# ==============================
# API CALL
# ==============================

def build_request_headers(session_data: Dict[str, Any]) -> Dict[str, str]:
    headers = {
        "accept": "*/*",
        "accept-language": "vi,en-US;q=0.9,en;q=0.8",
        "content-type": "application/json",
        "origin": "https://finance.salework.net",
        "platform": "web",
        "priority": "u=1, i",
        "referer": "https://finance.salework.net/adsExpenseTransaction",
        "cookie": session_data["cookie"],
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    if session_data.get("authorization"):
        headers["authorization"] = session_data["authorization"]

    if session_data.get("companycode"):
        headers["companycode"] = session_data["companycode"]

    return headers


def fetch_one_page(
    http_session: requests.Session,
    session_data: Dict[str, Any],
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

    headers = build_request_headers(session_data)

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
            "raw_text": resp.text
        }


def fetch_all_data(
    session_data: Dict[str, Any],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    http_session = requests.Session()
    all_rows: List[Dict[str, Any]] = []
    start_page = 0
    page_size = 500

    while True:
        print(f"📥 Fetch page {start_page}")

        data = fetch_one_page(
            http_session=http_session,
            session_data=session_data,
            date_from=date_from,
            date_to=date_to,
            start_page=start_page,
            page_size=page_size,
        )

        if not data.get("success"):
            message = data.get("message")
            raise RuntimeError(f"API lỗi: {message}")

        items = data.get("data") or []
        total = data.get("total")

        print(f"✅ items len: {len(items)} | total: {total}")

        if not items:
            print("✅ No more data, stop")
            break

        all_rows.extend(items)

        if len(items) < page_size:
            print("✅ Trang cuối, stop")
            break

        start_page += 1

    return all_rows


# ==============================
# BIGQUERY
# ==============================

def ensure_table_exists(rows: List[Dict[str, Any]]) -> None:
    try:
        bq_client.get_table(table_ref)
        print(f"✅ Table exists: {table_ref}")
    except Exception:
        print(f"⚠️ Table chưa tồn tại, tạo mới: {table_ref}")

        schema = infer_bq_schema_from_rows(rows)

        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"✅ Created table: {table_ref}")


def delete_recent_rows() -> None:
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 1 DAY)
    """
    print("\n🧹 Deleting yesterday and today...")
    bq_client.query(query).result()
    print("✅ Delete done")


def load_to_bigquery(rows: List[Dict[str, Any]]) -> None:
    normalized_rows = []

    today_vn = now_vn().date()
    yesterday_vn = today_vn - timedelta(days=1)

    for row in rows:
        row_out = normalize_row_for_bq(row)

        # nếu API chưa trả cột date thì cố gắng map thêm
        if "date" not in row_out or not row_out.get("date"):
            if "createdAt" in row_out and row_out["createdAt"]:
                row_out["date"] = str(row_out["createdAt"])[:10]
            elif "transactionDate" in row_out and row_out["transactionDate"]:
                row_out["date"] = str(row_out["transactionDate"])[:10]
            else:
                row_out["date"] = str(today_vn)

        normalized_rows.append(row_out)

    ensure_table_exists(normalized_rows)

    errors = bq_client.insert_rows_json(table_ref, normalized_rows)
    if errors:
        print("❌ BigQuery insert errors:")
        print(json.dumps(errors, ensure_ascii=False, indent=2))
        raise RuntimeError("Insert BigQuery thất bại")
    else:
        print(f"✅ Inserted {len(normalized_rows)} rows into {table_ref}")


# ==============================
# MAIN
# ==============================

def main():
    print("🚀 START SALEWORK SHOPEE ETL")
    print("📌 TABLE:", table_ref)

    date_from, date_to = build_date_range()
    print("📅 DATE_FROM:", date_from)
    print("📅 DATE_TO  :", date_to)

    session_data = load_or_create_session()

    print("✅ SESSION READY")
    print("   - authorization:", repr(session_data.get("authorization")))
    print("   - companycode  :", repr(session_data.get("companycode")))
    print("   - cookie len   :", len(session_data.get("cookie", "")))

    rows = fetch_all_data(
        session_data=session_data,
        date_from=date_from,
        date_to=date_to,
    )

    print(f"📦 Total rows fetched: {len(rows)}")

    if not rows:
        print("⚠️ No data")
        return

    delete_recent_rows()
    load_to_bigquery(rows)

    print("🎉 DONE")


if __name__ == "__main__":
    main()
