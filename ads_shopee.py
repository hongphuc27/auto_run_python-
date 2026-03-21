import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import json




# ==============================
# SALEWORK CONFIG
# ==============================

BASE_URL = "https://finance.salework.net/api/saleExpense/getAdsExpenseTransactionsByDays"

COMPANY_ID = "sw30871"
CHANNEL = "Shopee"


# DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
# DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# DATE_FROM = "2026-01-01T17:00:00.000Z"
# DATE_TO   = "2026-03-18T16:59:59.000Z"

now_utc = datetime.now(timezone.utc)
now_vn = now_utc + timedelta(hours=7)

today_vn = now_vn.date()
yesterday_vn = today_vn - timedelta(days=7)

DATE_FROM = datetime(yesterday_vn.year, yesterday_vn.month, yesterday_vn.day, 0, 0, 0, tzinfo=timezone.utc) - timedelta(hours=7)
DATE_TO = datetime(today_vn.year, today_vn.month, today_vn.day, 23, 59, 59, tzinfo=timezone.utc) - timedelta(hours=7)

DATE_FROM = DATE_FROM.strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = DATE_TO.strftime("%Y-%m-%dT%H:%M:%S.000Z")


PAGE_SIZE = 500


HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "companycode": COMPANY_ID,
    "platform": "web",
    "origin": "https://finance.salework.net",
    "referer": "https://finance.salework.net/",
    "user-agent": "Mozilla/5.0",
    "cookie": "_gcl_au=1.1.837264109.1772181731; _fbp=fb.1.1772181731484.722746009400298125; _ati=617753047877; dvct=9OHGDOBuFYPp20Gaa2RVkZHq3uPkUmH50onTLJwl83eZn2k0YPRKSRFWE9hCce8y; _ga_CP5XM6MNMB=GS2.1.s1774056612$o11$g1$t1774056621$j51$l0$h0; _gid=GA1.2.297360471.1774061730; _ga_K5SYCPWLCZ=GS2.2.s1774061730$o2$g0$t1774061730$j60$l0$h0; _ga_B9KBK8PCW6=GS2.1.s1774061730$o2$g0$t1774061735$j55$l0$h0; _ga=GA1.1.1227519468.1772181731; evn-token=cldVdm1OJTJCMzlnd3dHdHk3d2ZhJTJGUWclM0QlM0Q6a09vTHJNUlM3UTVvVnpaZ09FRXkydyUzRCUzRA; JSESSIONID=NjNhYTczMmUtM2JhMi00MWNjLWEyZDEtMjBjYjAxNThlODQy; _ga_Y2Y1836HKY=GS2.1.s1774056607$o38$g1$t1774061758$j31$l0$h0; _ga_Y9C49DPJZW=GS2.1.s1774057359$o37$g1$t1774062434$j44$l0$h0"
}


# ==============================
# BIGQUERY CONFIG
# ==============================

PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_ads_shopee"

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ==============================
# FETCH DATA
# ==============================

def fetch_orders():
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

        r = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()
            # print(resp)
            # print("resp keys:", list(resp.keys()))
        

        payload_data = resp.get("data", {})
        items = payload_data.get("tableData", [])
        total = payload_data.get("total", total)

        print(f"items len: {len(items)} | total: {total}")

        if not items:
            print("✅ No more data, stop")
            break

        all_items.extend(items)
        print(f"👉 Fetched: {len(all_items)} / {total}")

        # nếu đã lấy đủ theo total thì dừng
        if total is not None and len(all_items) >= total:
            print("✅ Reached total records, stop")
            break

        # # nếu page cuối có ít hơn PAGE_SIZE thì dừng
        # if len(items) < PAGE_SIZE:
        #     print("✅ Last page reached, stop")
        #     break

        page += 1
        time.sleep(0.1)

    return all_items


# ==============================
# MAIN ETL
# ==============================

def main():
    print("\n🚀 START SALEWORK SHOPEE → BIGQUERY ETL\n")

    orders = fetch_orders()
    print(f"\n📦 Total orders: {len(orders)}")

    if not orders:
        print("⚠️ No data")
        return

    # ==============================
    # BUILD DATAFRAME THEO YÊU CẦU
    # ==============================

    rows = []

    for item in orders:

        rows.append({
        "shopId": str(item["shopId"]),
        "date": item["date"],
        "amount": item["amount"],
        "vat": item["vat"]
    })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["shopId", "date", "amount", "vat"])

    # Convert datatype
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["vat"] = pd.to_numeric(df["vat"], errors="coerce")

    print("\n📊 Columns:", list(df.columns))
    print("🧾 Rows:", len(df))

    # DELETE 20 ngày gần nhất
    print("\n🧹 Deleting last 7 days...")

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    """

    client.query(delete_query).result()

    print("✅ Delete done")

    # LOAD DATA
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


if __name__ == "__main__":
    main()
