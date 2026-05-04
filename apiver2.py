import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text, types as satypes
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from google.cloud import bigquery
import os

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ======================================================
# BIGQUERY CONFIG
# ======================================================
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_engagement"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

bq_client = bigquery.Client(project=PROJECT_ID)

# ======================================================
# API CONFIG
# ======================================================
BASE_URL = "https://pancake.vn/api/v1/statistics/customer_engagements"
ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiTMOqIEjhu5NuZyBQaMO6YyIsImV4cCI6MTc4NDc5MzA1MiwiYXBwbGljYXRpb24iOjEsInVpZCI6ImM5ZDFjNzM5LWIyNWItNGYxMy1hZmIyLTE0ZjcxYjdhYTFjMSIsInNlc3Npb25faWQiOiJhZDhiYzVlNy1iZTI1LTQ4OGQtYjhlNS1lNzVhNDcwMDYyNGQiLCJpYXQiOjE3NzcwMTcwNTIsImZiX2lkIjoiMzA0MTg2NzkwNDA3OTk4IiwibG9naW5fc2Vzc2lvbiI6bnVsbCwiZmJfbmFtZSI6IkzDqiBI4buTbmcgUGjDumMifQ.7b-7O7GOBa4Oi1TEvQzufrMHmI3dkI_t2Nj55O6S8eI"
PAGE_IDS = [
  "483749328145950",
  "265906426604834",
  "igo_17841477359067398",
  "430528060147110",
  "255510857642520",
  "251053074766192",
  "224738417378758",
  "121714277580247",
  "145395531984747",
  "349812045684771",
  "381980068330807",
  "354194711117810",
  "188795517644918",
  "214917205035149",
  "395429883648163",
  "212231978650825",
  "131316066738629",
  "274122955780423",
  "222735497586333",
  "319779584562905",
  "171739112679050",
  "103275816074231",
  "102100034727215",
  "108428895560206",
  "270215889488883",
  "109991845394030"
]

# ======================================================
# DATE RANGE (BACKFILL)
# ======================================================
# TODAY = datetime.now().strftime("%Y-%m-%d")
# START_DATE = TODAY
# END_DATE = TODAY

END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ======================================================
# REQUEST SESSION WITH RETRY
# ======================================================
def create_session():
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    return session

session = create_session()

# ======================================================
# UTIL: GENERATE DATE LIST
# ======================================================
def generate_date_list(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates

# ======================================================
# FETCH DATA (ONE PAGE / REQUEST)
# ======================================================
def fetch_engagement_by_page(page_id, date):
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    date_range = (
        f"{date_obj.strftime('%d/%m/%Y')} 00:00:00 - "
        f"{date_obj.strftime('%d/%m/%Y')} 23:59:59"
    )

    params = {
        "page_id": page_id,
        "date_range": date_range,
        "access_token": ACCESS_TOKEN
    }

    response = session.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json().get("users_engagements", [])

# ======================================================
# PROCESS PAGE DATA (SAFE SCHEMA)
# ======================================================
def process_users(users, page_id, date):
    if not users:
        return pd.DataFrame()

    df = pd.DataFrame(users)

    rename_map = {
        "user_id": "ma_nhan_vien",
        "name": "ten_nhan_vien",
        "new_customer_replied_count": "khach_moi_da_tra_loi",
        "inbox_count": "so_tin_nhan",
        "comment_count": "so_comment",
        "total_engagement": "tong_tuong_tac",
        "order_count": "so_don_hang",
        "old_order_count": "so_don_hang_cu",
    }
    df = df.rename(columns=rename_map)

    numeric_cols = [
        "khach_moi_da_tra_loi",
        "so_tin_nhan",
        "so_comment",
        "tong_tuong_tac",
        "so_don_hang",
        "so_don_hang_cu",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0

    df["page_id"] = page_id
    df["ngay"] = pd.to_datetime(date)

    return df[
        [
            "page_id",
            "ngay",
            "ma_nhan_vien",
            "ten_nhan_vien",
            "khach_moi_da_tra_loi",
            "so_tin_nhan",
            "so_comment",
            "tong_tuong_tac",
            "so_don_hang",
            "so_don_hang_cu",
        ]
    ]

# ======================================================
# MAIN
# ======================================================
def main():
    logger.info("🚀 START PANCAKE BACKFILL JOB")
    date_list = generate_date_list(START_DATE, END_DATE)
    logger.info(f"📅 TOTAL DAYS: {len(date_list)}")

    dtype_map = {
        "page_id": satypes.NVARCHAR(20),
        "ngay": satypes.DATE,
        "ma_nhan_vien": UNIQUEIDENTIFIER,
        "ten_nhan_vien": satypes.NVARCHAR(100),
        "khach_moi_da_tra_loi": satypes.INTEGER,
        "so_tin_nhan": satypes.INTEGER,
        "so_comment": satypes.INTEGER,
        "tong_tuong_tac": satypes.INTEGER,
        "so_don_hang": satypes.INTEGER,
        "so_don_hang_cu": satypes.INTEGER,
    }

    for date in date_list:
        logger.info(f"\n📆 PROCESS DATE: {date}")
        all_users_data = []

        for page_id in PAGE_IDS:
            users = fetch_engagement_by_page(page_id, date)
            df_page = process_users(users, page_id, date)
            if not df_page.empty:
                all_users_data.append(df_page)
            time.sleep(1)

        if not all_users_data:
            logger.warning(f"⚠️ No data for {date}")
            continue

        df_final = pd.concat(all_users_data, ignore_index=True)

        # ===== MERGE CROSS PAGE (MATCH PANCAKE UI) =====
        df_final = (
            df_final
            .groupby(
                ["ngay", "ma_nhan_vien", "ten_nhan_vien"],
                as_index=False
            )
            .agg({
                "khach_moi_da_tra_loi": "sum",
                "so_tin_nhan": "sum",
                "so_comment": "sum",
                "tong_tuong_tac": "sum",
                "so_don_hang": "sum",
                "so_don_hang_cu": "sum"
            })
        )
        df_final["page_id"] = "ALL"

        # ===== FIX DATA TYPE BEFORE LOAD =====
        df_final["ngay"] = pd.to_datetime(df_final["ngay"]).dt.date
        df_final["ma_nhan_vien"] = df_final["ma_nhan_vien"].astype(str)

        int_cols = [
            "khach_moi_da_tra_loi",
            "so_tin_nhan",
            "so_comment",
            "tong_tuong_tac",
            "so_don_hang",
            "so_don_hang_cu"
        ]

        for col in int_cols:
            df_final[col] = df_final[col].fillna(0).astype("int64")

        # ===== DELETE DATA OF DATE =====
        logger.info(f"🧹 Delete data of {date} in BigQuery")

        delete_query = f"""
        DELETE FROM `{FULL_TABLE_ID}`
        WHERE ngay = @date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", date)
            ]
        )

        bq_client.query(delete_query, job_config=job_config).result()

        # ===== INSERT DATA =====
        logger.info(f"⬆️ Insert {len(df_final)} rows for {date} into BigQuery")

        load_job = bq_client.load_table_from_dataframe(
            df_final,
            FULL_TABLE_ID,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
        )

        load_job.result()

    logger.info("✅ BACKFILL + DAILY SNAPSHOT DONE")

# ======================================================
# ENTRY POINT
# ======================================================
if __name__ == "__main__":
    main()
    
