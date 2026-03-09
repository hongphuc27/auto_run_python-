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
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6IjA5NDYzNTYxODAiLCJjb3VudHJ5IjoiVk4iLCJjb21wYW55Q29kZSI6InN3MzA4NzEiLCJjb21wYW55TmFtZSI6IlJoeXMgTWFuIiwicGVybWlzc2lvbiI6IjExMTEiLCJhdmF0YXIiOiJyZXNpemVfMTY4Mzc2ODQzMzUxNF_EkEVOLnBuZyIsImNyZWF0ZWRCeSI6InN5c3RlbSIsInNob3BBY2Nlc3MiOnsic2hvcGVlIjpbMjk0MDQxNjcwLDc4OTQxNzMwNyw3NTgxMTE0MjcsMjkzOTQ0Njc2XSwibGF6YWRhIjpbInJoeXNtYW4ud29ya0BnbWFpbC5jb20iLCJwaGFtbGViYW5ndGFtOTZAZ21haWwuY29tIiwiYmFuZ3RhbTIwMjJ4QGdtYWlsLmNvbSIsImNza2gucmh5c21hbkBnbWFpbC5jb20iLCI0bWFuLm9mZmljaWFsdm5AZ21haWwuY29tIl0sInRpa2kiOlsiRUM5NDYyMUVEMURDNTVGRTI0RjcwRDRENjg5NzQxOTkyRDQ3QThBMyJdLCJ0aWt0b2siOlsiNzQ5NDc1ODMyMDAyMzgzMzI2OCIsIjc0OTQ3NDMxMzQ2MTEyMTMwNzciLCI3NDk0NTQ1NjMwMDIyMjQwNDgxIl0sInN0b2NrIjpbIlNhbGV3b3JrIiwiU2FsZXdvcmtXYXJlaG91c2UiXX0sInJvbGVBY2NvdW50Ijp7Im9yZGVyc192aWV3Ijp0cnVlLCJwcm9kdWN0c192aWV3Ijp0cnVlLCJleHBvcnRfaW1wb3J0X3ZpZXciOnRydWUsImVjb21tZXJjZV9saW5rX3ZpZXciOnRydWUsIm9yZGVyc19lY29tbWVyY2VfaGFuZGxlIjp0cnVlLCJvcmRlcnNfbm90X2Vjb21tZXJjZV9oYW5kbGUiOnRydWUsInN0b2NrX2NydWQiOnRydWUsInN0b2NrX3N5bmNfZWNvbW1lcmNlIjp0cnVlLCJwcm9kdWN0c19jcnVkIjp0cnVlLCJwcm9kdWN0c19jb3B5X2Vjb21tZXJjZSI6dHJ1ZSwicmVwb3J0X2V4cG9ydF9pbXBvcnRfcHJvZHVjdHMiOnRydWUsInJlcG9ydF9vcmRlcnMiOnRydWUsImNvc3RfdmlldyI6dHJ1ZSwic2V0dGluZ19jcnVkIjp0cnVlLCJvcmRlcnNfYXNzaWduZWQiOmZhbHNlLCJvcmRlcnNfdmlydHVhbF92aWV3Ijp0cnVlLCJwcm9kdWN0c19naWZ0X3ZpZXciOnRydWV9LCJhY3RpdmVVc2VyIjpmYWxzZSwiaWF0IjoxNzcyNTk1NzgzLCJleHAiOjE3NzMyMDA1ODN9.hmLa14CoECA84JnELEuLn3c41lquU7rn4FJYM-YhjxM"

COMPANY_ID = "sw30871"
CHANNEL = "Shopee"


now_utc = datetime.now(timezone.utc)

DATE_FROM = (now_utc - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
DATE_TO = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# DATE_FROM = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc) \
#     .strftime("%Y-%m-%dT%H:%M:%S.000Z")

# DATE_TO = datetime(2026, 2, 24, 23, 59, 59, tzinfo=timezone.utc) \
#     .strftime("%Y-%m-%dT%H:%M:%S.000Z")


PAGE_SIZE = 500


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://stock.salework.net",
    "referer": "https://stock.salework.net/",
    "user-agent": "Mozilla/5.0"
}


KEEP_COLUMNS = [
    "_id",
    "code",
    "shopeeShopId",
    "city",
    "state",
    "createdAt",
    "price"
]


# ==============================
# BIGQUERY CONFIG
# ==============================

PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_orders_salework_shopee"


gcp_key = json.loads(os.getenv("GCP_SERVICE_ACCOUNT"))
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

    for order in orders:

        # Lấy cost_of_goods_sold từ escrowDetails
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

    # Convert datatype
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    print("\n📊 Columns:", list(df.columns))
    print("🧾 Rows:", len(df))

    # DELETE 20 ngày gần nhất
    print("\n🧹 Deleting last 20 days...")

    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
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




