import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
import os
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json


# =====================================================
# BIGQUERY  CONFIG (THEO CỦA BẠN)
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_creator_tiktok"


gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


# =====================================================
# TIKTOK API (DÁN TỪ F12 → COPY AS CURL)
# =====================================================
URL = (
    "https://affiliate.tiktok.com/api/v1/affiliate/orders"
    "?user_language=vi-VN"
    "&aid=4331"
    "&app_name=i18n_ecom_alliance"
    "&device_platform=web"
    "&browser_language=vi"
    "&browser_platform=Win32"
    "&browser_name=Mozilla"
    "&timezone_name=Asia%2FSaigon"
    "&shop_region=VN"
    "&oec_seller_id=7494545630022240481"
    "&msToken=ihykpW25F6VdjhjQU1DDg51GKyXup16943IkNc397v0qH4XA_n3jc1xbKm0aR1LIl43zjhxntq9Azpj1XfQYIulASgIHxSHaBT4HYdlWshBzB1TFg4Ju0ZALi-YVa3Y0ZDAqrg=="
    "&X-Bogus=DFSzswVOU8YoToKzCi9B6QVRr3Nw"
    "&X-Gnarly=MauTAkwV9g4WTiyTuFWfK/n0AeNkOgRztKODNVSh6M9DIaOzlP3WoSjeAy/7LGNmU-WY7dBUzWLsXgLZKbUjRgCXX3yuT58Y7/mL/XxgfRSDWMtjgNsMGWe/KVLGtvJ73b5BMFbDHUcdoxcA2RxI63nnVg1HWVd4JEEIsqCkAkXXveeMrRAxxXjzdxRyhsJYj9c5jW0nZtrXcLnRuMxCo0Wcx7ULSD4w9FqcVli1Sg6QbR7UXxmUe8M-cG8dybuoD0n2KrzV7wrY"
)


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://affiliate.tiktok.com",
    "referer": "https://affiliate.tiktok.com/product/order?shop_region=VN",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI.tt.1; uid_tt_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; uid_tt_ss_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; sid_tt_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_ss_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; uid_tt_ads=e3bc43581cb194c430ed109d02655cd34e916fbcd08f304a371cb735191c81d7; uid_tt_ss_ads=e3bc43581cb194c430ed109d02655cd34e916fbcd08f304a371cb735191c81d7; sid_tt_ads=28c9e866d0a9f385ee340f4869f64f3d; sessionid_ads=28c9e866d0a9f385ee340f4869f64f3d; sessionid_ss_ads=28c9e866d0a9f385ee340f4869f64f3d; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzcwMTY2MjQsIm5iZiI6MTc3NjkyOTIyNH0.f28hQ3yYl_6_3M7TWN-BolwSlReD_0ob0yxiSPehk3g; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3MDE2NjI0LCJuYmYiOjE3NzY5MjkyMjR9.w4dpd_AbQ-v9FfK18xouZ7ZV1JvxWmTPbcLdmFHOei0; _m4b_theme_=new; sid_guard_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27%7C1776930278%7C259200%7CSun%2C+26-Apr-2026+07%3A44%3A38+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C4%7Cq03uHjMyIYq84OhzSustJ__________PlmloK-AJApbeS5SoqwEzey9NKrlqBEf6JUI3pOWf5VA%3D; sid_ucp_v1_tiktokseller=1.0.1-KDgyYTNmYTBlMzlmZDUxMDkzYzA1NmVlY2M1MDlmYmQ3OTlmODU1Y2IKHAiUiN7g9dSegGkQ5punzwYY5B8gDDgBQOsHSAQQAxoCbXkiIGFiNGRlZTFlMzMzMjIxOGFiY2UwZTg3MzRhZWIyZDI3Mk4KILNAam_gmfmoyOaYARCd0iIqmwWjhURtD4EYcaBcr5UUEiD1bV1H675cOzeqSG4OWsxZZXt9BO9zds44C-Mi8pFFmRgBIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDgyYTNmYTBlMzlmZDUxMDkzYzA1NmVlY2M1MDlmYmQ3OTlmODU1Y2IKHAiUiN7g9dSegGkQ5punzwYY5B8gDDgBQOsHSAQQAxoCbXkiIGFiNGRlZTFlMzMzMjIxOGFiY2UwZTg3MzRhZWIyZDI3Mk4KILNAam_gmfmoyOaYARCd0iIqmwWjhURtD4EYcaBcr5UUEiD1bV1H675cOzeqSG4OWsxZZXt9BO9zds44C-Mi8pFFmRgBIgZ0aWt0b2s; FPLC=X9wniw9cLWp94thlXyb95VurcDwrtROu6I8C8mBxHJt%2BLHFA0c8Dx5LgPcqUnPm6Sb62rxNfp%2FGiwJzwwB7dBmBNzVIS6%2FptdYMTu0nSJSA1cuwd4IMTraCsTRKItQ%3D%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1776930284079::Texc8anno72WVi_7JRwm.6.1776930284345.0; ttcsid=1776930284080::NMMVyHRVRIwZL1zSHIBG.6.1776930284345.0::1.-7212.0::1330.1.656.282::0.0.0; _ga_BZBQ2QHQSP=GS2.1.s1776930282$o6$g0$t1776930285$j57$l0$h271757427; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; tt_session_tlb_tag_ads=sttt%7C2%7CKMnoZtCp84XuNA9IafZPPf_________TLuK4qpuGrGftjfxYhWVnDAvXk8dKPzCluDsAbYjte-Q%3D; sid_ucp_v1_ads=1.0.1-KDk3Mzc0MTViZTM2MGJkNjRhNjc3ZGNkMWEwNDAwMzQyNzg1ZGU5YjQKHAiUiN7g9dSegGkQ5qinzwYYrwwgDDgBQOsHSAQQAxoDc2cxIiAyOGM5ZTg2NmQwYTlmMzg1ZWUzNDBmNDg2OWY2NGYzZDJOCiBi7A2RCy2VKRxCi17BCvtiXHWHnaC9wx_uY0Na13Wc1xIg2KupdwG3Q03D__dg2DYGCeIpjmqZ7NyHsZrf5en-eHIYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDk3Mzc0MTViZTM2MGJkNjRhNjc3ZGNkMWEwNDAwMzQyNzg1ZGU5YjQKHAiUiN7g9dSegGkQ5qinzwYYrwwgDDgBQOsHSAQQAxoDc2cxIiAyOGM5ZTg2NmQwYTlmMzg1ZWUzNDBmNDg2OWY2NGYzZDJOCiBi7A2RCy2VKRxCi17BCvtiXHWHnaC9wx_uY0Na13Wc1xIg2KupdwG3Q03D__dg2DYGCeIpjmqZ7NyHsZrf5en-eHIYASIGdGlrdG9r; pre_country=VN; part=stable; msToken=LCbCR_2xF7TuEsQP0wH7aHy52xgsEXX0FEKw8vio8ZudaQdNi83dA7Tztx17-id_7iRonQwp8HUsuyf8mMpUSwmxk2whnczXZGZgnmq6jjakweSanE_ARnSBvlsdk4-JqpSAnJw=; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776994731%7C0355507dcdda017ae22b6e08c75789768f8da43dc43f4e378457a4410ae788ea; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; odin_tt=1f3ab6e2fbb1551a941c95642b46238122d75ee19c98a3b9b40107e9ec57817c6169b631019855d016b1ea3d9b70712d9cf2cf0b066b9b2bfbc19202da8e4a07; user_oec_info=0a5317fb35140a72719ac59797c417f04b1578f0ec7d969b785923e1efe87112c9ae088494ab5aca232e31264db9adee0304c57ab8fc4924471c79f7f55c2d9f2c87a0724809448154965cc608223ba4a0467e0b191a490a3c000000000000000000005057a8851ad134cd02d74663b6df34c4ba4039b5e142a1d954473382d3bf09ee5d430c429be028178a68688c2a95f37ae63310e7d38f0e1886d2f6f20d2201043478f54e; msToken=AmDUi-7DGmf8XZxQmvSccHqJLIgjYQeKvUEbXPsAqVr7GAyKYls-PfI_CZbcCY1AfyeOb0839N1YyD1JVVw4S67LKQpzFZrX_00bfH6DroLNM-shfyOxpUQt6KLLv6DSPFmORdw="
}

# =====================================================
# =====================================================
tz_vn = timezone(timedelta(hours=7))

# =====================================================
def fetch_page(page: int, start_time, end_time):
    payload = {
    "conditions": {
        "time_period": {
            "beginning_time": str(start_time),
            "ending_time": str(end_time)
        }
    },
        "page": page,
        "page_size": 100
    }

    r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# =====================================================
def run():
    all_rows = []

    now_vn = datetime.now(tz=tz_vn)
    start_date = (now_vn - timedelta(days=25)).replace(tzinfo=None)
    end_date = now_vn.replace(tzinfo=None)

    current_date = start_date

    while current_date <= end_date:

        print("Fetching date:", current_date.date())

        START_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            0, 0, 0,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        END_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            23, 59, 59,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        page = 1
        empty_retry = 0

        while True:

            try:
                data = fetch_page(page, START_TIME, END_TIME)
            except Exception as e:
                print("API error retry:", e)
                time.sleep(5)
                continue

            orders = data.get("orders", [])

            print(f"Page {page} | Orders: {len(orders)}")

            if not orders:

                empty_retry += 1

                if empty_retry >= 3:
                    print("No more data for this day.")
                    break

                print("Empty page -> retry")
                time.sleep(2)
                continue

            empty_retry = 0

            for o in orders:

                main_order_id = o.get("main_order_id")
                create_time_ms = o.get("create_time")

                if not main_order_id or not create_time_ms:
                    continue

                create_time = datetime.fromtimestamp(
                    create_time_ms / 1000
                ).replace(tzinfo=None)

                sku_details = o.get("sku_detail", [])

                for sku in sku_details:

                    creator_nickname = sku.get("creator_nickname")
                    creator_username = sku.get("creator_username")

                    cos_ratio = sku.get("cos_ratio")
                    estimated_cos_fee = sku.get("estimated_cos_fee")

                    shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
                    estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")

                    promotion_position_type = (
                        sku.get("promotion_position", {})
                        .get("promotion_position_type")
                    )

                    all_rows.append((
                        int(main_order_id),
                        creator_nickname,
                        creator_username,
                        promotion_position_type,
                        create_time,
                        cos_ratio,
                        estimated_cos_fee,
                        shop_ads_commission_ratio,
                        estimated_shop_ads_commission
                    ))

            page += 1
            time.sleep(0.3)

        # sang ngày tiếp theo
        current_date += timedelta(days=1)
    
    print("TOTAL ROWS TO INSERT:", len(all_rows))

    if not all_rows:
        print("NO DATA TO INSERT")
        return

    df = pd.DataFrame(all_rows, columns=[
    "main_order_id",
    "creator_nickname",
    "creator_username",
    "promotion_position_type",
    "create_time",
    "cos_ratio",
    "estimated_cos_fee",
    "shop_ads_commission_ratio",
    "estimated_shop_ads_commission"
])

    df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

    def to_decimal(x):
        try:
            if pd.isna(x):
                return None
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None


    df["cos_ratio"] = df["cos_ratio"].apply(to_decimal)
    df["estimated_cos_fee"] = df["estimated_cos_fee"].apply(to_decimal)
    df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
    df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

    # drop row lỗi
    df = df.dropna(subset=["main_order_id"])
        
    # df = df.drop_duplicates(
    # subset=["main_order_id", "promotion_position_type", "cos_ratio", "estimated_cos_fee", "shop_ads_commission_ratio", "estimated_shop_ads_commission"]
    # )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # ==============================
    # DELETE DATA TODAY + YESTERDAY
    # ==============================
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 25 DAY)
    """

    client.query(delete_query).result()

    print("Old data (today + yesterday) deleted.")

    # ==============================

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )

    job.result()

    print(f"✅ Loaded {len(df)} rows into BigQuery")



# =====================================================
if __name__ == "__main__":
    run()




























