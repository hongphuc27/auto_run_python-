import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
import time
import logging
from sqlalchemy import create_engine, text, types as satypes
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ======================================================
# SQL SERVER CONFIG
# ======================================================
SERVER = "THOMTRAN"
DATABASE = "rhysman"
TABLE_NAME = "dbo.fact_engagement"
params = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(params),
    fast_executemany=True
)

# ======================================================
# API CONFIG
# ======================================================
BASE_URL = "https://pancake.vn/api/v1/statistics/customer_engagements"
ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbmZvIjp7Im9zIjozLCJjbGllbnRfaXAiOiIxMjMuMjQuMTMyLjI0NyIsImJyb3dzZXIiOjEsImRldmljZV90eXBlIjozfSwibmFtZSI6IkzDqiBI4buTbmcgUGjDumMiLCJleHAiOjE3Nzc0NTk1NTUsImFwcGxpY2F0aW9uIjoxLCJ1aWQiOiJjOWQxYzczOS1iMjViLTRmMTMtYWZiMi0xNGY3MWI3YWExYzEiLCJzZXNzaW9uX2lkIjoiMTZlMDQ3NDUtN2M0MC00YWI2LWIwZTUtMmE3ZWQ5MzNlZTk0IiwiaWF0IjoxNzY5NjgzNTU1LCJmYl9pZCI6IjMwNDE4Njc5MDQwNzk5OCIsImxvZ2luX3Nlc3Npb24iOm51bGwsImZiX25hbWUiOiJMw6ogSOG7k25nIFBow7pjIn0.TO94se84HphdY_PkCEXrNU3GFDchC2Tiu_tCodNeaIM"

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
TODAY = datetime.now().strftime("%Y-%m-%d")
START_DATE = TODAY
END_DATE = TODAY

# END_DATE = datetime.now().strftime("%Y-%m-%d")
# START_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")



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

        with engine.begin() as conn:
            logger.info(f"🧹 Delete data of {date}")
            conn.execute(
                text(f"""
                    DELETE FROM {TABLE_NAME}
                    WHERE ngay = :date
                """),
                {"date": date}
            )

            logger.info(f"⬆️ Insert {len(df_final)} rows for {date}")
            df_final.to_sql(
                name=TABLE_NAME.split(".")[1],
                schema=TABLE_NAME.split(".")[0],
                con=conn,
                if_exists="append",
                index=False,
                dtype=dtype_map
            )

    logger.info("✅ BACKFILL + DAILY SNAPSHOT DONE")

# ======================================================
# ENTRY POINT
# ======================================================
if __name__ == "__main__":
    main()
