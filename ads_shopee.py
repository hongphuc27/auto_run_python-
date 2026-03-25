import os
import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

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
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{BQ_TABLE_ID}"


# ==============================
# SALEWORK CONFIG
# ==============================

API_URL = "https://finance.salework.net/api/saleExpense/getAdsExpenseTransactionsByDays"
CHANNEL = os.getenv("SALEWORK_CHANNEL", "Shopee")

SALEWORK_AUTHORIZATION = os.getenv("SALEWORK_AUTHORIZATION", "").strip()
SALEWORK_COMPANYCODE = os.getenv("SALEWORK_COMPANYCODE", "").strip()
SALEWORK_COOKIE = os.getenv("SALEWORK_COOKIE", "").strip()

if SALEWORK_AUTHORIZATION and not SALEWORK_AUTHORIZATION.startswith("Bearer "):
    SALEWORK_AUTHORIZATION = f"Bearer {SALEWORK_AUTHORIZATION}"

VN_TZ = timezone(timedelta(hours=7))


# ==============================
# HELPERS
# ==============================

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


def validate_config() -> None:
    missing = []
    if not SALEWORK_AUTHORIZATION:
        missing.append("SALEWORK_AUTHORIZATION")
    if not SALEWORK_COMPANYCODE:
        missing.append("SALEWORK_COMPANYCODE")
    if not SALEWORK_COOKIE:
        missing.append("SALEWORK_COOKIE")

    if missing:
        raise RuntimeError(f"Thiếu biến môi trường: {', '.join(missing)}")


def normalize_bigquery_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def normalize_row_for_bq(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: normalize_bigquery_value(v) for k, v in row.items()}


def infer_bq_schema_from_rows(rows: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
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

    if "date" not in field_types:
        field_types["date"] = "DATE"

    return [bigquery.SchemaField(name, field_type) for name, field_type in field_types.items()]


# ==============================
# API CALL
# ==============================

def build_request_headers() -> Dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "vi,en-US;q=0.9,en;q=0.8",
        "authorization": SALEWORK_AUTHORIZATION,
        "companycode": SALEWORK_COMPANYCODE,
        "content-type": "application/json",
        "cookie": SALEWORK_COOKIE,
        "origin": "https://finance.salework.net",
        "platform": "web",
        "priority": "u=1, i",
        "referer": "https://finance.salework.net/adsExpenseTransaction",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }


def fetch_one_page(
    http_session: requests.Session,
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

    headers = build_request_headers()

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

    while True:
        print(f"📥 Fetch page {start_page}")

        data = fetch_one_page(
            http_session=http_session,
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
    print("🧹 Deleting yesterday and today...")
    bq_client.query(query).result()
    print("✅ Delete done")


def load_to_bigquery(rows: List[Dict[str, Any]]) -> None:
    normalized_rows = []

    today_vn = now_vn().date()

    for row in rows:
        row_out = normalize_row_for_bq(row)

        if "date" not in row_out or not row_out.get("date"):
            if "createdAt" in row_out and row_out["createdAt"]:
                row_out["date"] = str(row_out["createdAt"])[:10]
            elif "transactionDate" in row_out and row_out["transactionDate"]:
                row_out["date"] = str(row_out["transactionDate"])[:10]
            else:
                row_out["date"] = str(today_vn)

        normalized_rows.append(row_out)

    ensure_table_exists(normalized_rows)

    job = bq_client.load_table_from_json(
        normalized_rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    
    job.result()  # đợi job chạy xong
    
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
