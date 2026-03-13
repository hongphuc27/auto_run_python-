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

BASE_URL = "https://stock.salework.net/api/v2/order"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InJoeXNtYW4va2V0b2FudGh1ZSIsImNvdW50cnkiOiJWTiIsImNvbXBhbnlDb2RlIjoic3czMDg3MSIsImNvbXBhbnlOYW1lIjoiUmh5cyBNYW4iLCJwZXJtaXNzaW9uIjoiMTExIiwiYXZhdGFyIjoicmVzaXplXzE2ODM3Njg0MzM1MTRfxJBFTi5wbmciLCJjcmVhdGVkQnkiOiJzeXN0ZW0iLCJzaG9wQWNjZXNzIjp7InNob3BlZSI6WzI5NDA0MTY3MCw3ODk0MTczMDcsNzU4MTExNDI3LDI5Mzk0NDY3Nl0sImxhemFkYSI6WyJyaHlzbWFuLndvcmtAZ21haWwuY29tIiwicGhhbWxlYmFuZ3RhbTk2QGdtYWlsLmNvbSIsImJhbmd0YW0yMDIyeEBnbWFpbC5jb20iLCJjc2toLnJoeXNtYW5AZ21haWwuY29tIiwiNG1hbi5vZmZpY2lhbHZuQGdtYWlsLmNvbSJdLCJ0aWtpIjpbXSwidGlrdG9rIjpbIjc0OTQ3NTgzMjAwMjM4MzMyNjgiLCI3NDk0NzQzMTM0NjExMjEzMDc3IiwiNzQ5NDU0NTYzMDAyMjI0MDQ4MSJdLCJzdG9jayI6WyJTYWxld29yayIsIlNhbGV3b3JrV2FyZWhvdXNlIl19LCJyb2xlQWNjb3VudCI6eyJvcmRlcnNfdmlldyI6dHJ1ZSwicHJvZHVjdHNfdmlldyI6dHJ1ZSwiZXhwb3J0X2ltcG9ydF92aWV3Ijp0cnVlLCJlY29tbWVyY2VfbGlua192aWV3Ijp0cnVlLCJvcmRlcnNfZWNvbW1lcmNlX2hhbmRsZSI6dHJ1ZSwib3JkZXJzX25vdF9lY29tbWVyY2VfaGFuZGxlIjp0cnVlLCJzdG9ja19jcnVkIjp0cnVlLCJzdG9ja19zeW5jX2Vjb21tZXJjZSI6dHJ1ZSwicHJvZHVjdHNfY3J1ZCI6dHJ1ZSwicHJvZHVjdHNfY29weV9lY29tbWVyY2UiOnRydWUsInJlcG9ydF9leHBvcnRfaW1wb3J0X3Byb2R1Y3RzIjp0cnVlLCJyZXBvcnRfb3JkZXJzIjp0cnVlLCJjb3N0X3ZpZXciOmZhbHNlLCJzZXR0aW5nX2NydWQiOmZhbHNlLCJvcmRlcnNfYXNzaWduZWQiOmZhbHNlLCJvcmRlcnNfdmlydHVhbF92aWV3Ijp0cnVlLCJwcm9kdWN0c19naWZ0X3ZpZXciOnRydWV9LCJhY3RpdmVVc2VyIjpmYWxzZSwiaWF0IjoxNzczNDEyMjU1LCJleHAiOjE3NzQwMTcwNTV9.i7yot1GR66TZBP7Y3G6eJ1hoaq0yiYOoA59IpB_NEN4"

COMPANY_ID = "sw30871"
CHANNEL = "Tiktok"


now_utc = datetime.now(timezone.utc)

DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# DATE_FROM = datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc) \
#     .strftime("%Y-%m-%dT%H:%M:%S.000Z")

# DATE_TO = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc) \
#     .strftime("%Y-%m-%dT%H:%M:%S.000Z")


PAGE_SIZE = 550


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://stock.salework.net",
    "referer": "https://stock.salework.net/",
    "user-agent": "Mozilla/5.0"
}


KEEP_COLUMNS = [
    "_id","code","tiktokShopId","totalPrice","state","createdAt",
    "channel","reconciled","estimateRevenue","customer_state",

    "tiktok_payment_info_original_shipping_fee",
    "tiktok_payment_info_original_total_product_price",
    "tiktok_payment_info_platform_discount",
    "tiktok_payment_info_seller_discount",
    "tiktok_payment_info_shipping_fee",
    "tiktok_payment_info_shipping_fee_platform_discount",
    "tiktok_payment_info_shipping_fee_seller_discount",
    "tiktok_payment_info_sub_total",
    "tiktok_payment_info_taxes",
    "tiktok_payment_info_total_amount"
]


# ==============================
# BIGQUERY CONFIG
# ==============================

PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_orders_salework_tiktok"


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

        r = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=30)
        data = r.json()

        orders = data.get("orders", [])

        if not orders:
            break

        all_orders.extend(orders)
        start += PAGE_SIZE
        time.sleep(0.05)

    return all_orders


# ==============================
# MAIN ETL
# ==============================

def main():
    print("\n🚀 START SALEWORK → SQL SERVER ETL\n")

    orders = fetch_orders()
    print(f"\n📦 Total orders: {len(orders)}")

    df = pd.json_normalize(orders, sep="_")

    df = df[[c for c in KEEP_COLUMNS if c in df.columns]]
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
    df["createdAt"] = df["createdAt"].dt.tz_convert("Asia/Ho_Chi_Minh")

    print("\n📊 Columns:", list(df.columns))
    print("🧾 Rows:", len(df))

    # 🔥 XÓA DỮ LIỆU 20 NGÀY GẦN NHẤT
    print("\n🧹 Deleting last 20 days data in SQL...")

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
    """

    client.query(delete_query).result()

    print("✅ Delete done")

    # 💾 INSERT LẠI
    print("\n💾 Writing to SQL Server...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("createdAt", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )

    job.result()

    print("\n✅ DONE LOAD SQL SERVER")



if __name__ == "__main__":
    main()







