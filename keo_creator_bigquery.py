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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; FPLC=hCYsDdYrlOpV7jaIbQ0VSE0p0cUXJpvtBcDArFugYTHIbkc87Wl8lz97Z8ggwwRntDZsQBt7%2Fm0fdl034VkyGPrYBPwwizYhwjT1YLJWSVvfhay1Xz2ElKDr3eQ6%2Fw%3D%3D; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; ttcsid_CMSS13RC77U1PJEFQUB0=1775537660817::aRA4G-_EmTu_ToyOpsFu.1.1775537723207.1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; ttcsid=1775537660818::bhcc8U1LpZTuFD306i5E.1.1775537723206.0::1.-18488.0::67734.24.732.495::0.0.0; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; sso_uid_tt_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_uid_tt_ss_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_user_ads=9689c113a5f4913dbb255ae04a102874; sso_user_ss_ads=9689c113a5f4913dbb255ae04a102874; sid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1775537644$o1$g1$t1775537731$j60$l0$h890508184; sid_guard_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2%7C1775537732%7C259200%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; uid_tt_ss_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; sid_tt_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_ss_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C7QbhYA1urHAjAfvQ4Qmfwv__________QPF9zItUF2qAhZaIzMbvfm4jp09mDdlugUUrP303PR0%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; _tt_ticket_crypt_doamin=2; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU2MjQxMzUsIm5iZiI6MTc3NTUzNjczNX0.UXXPVwe67zdp_oi66apV9KQuMu_8tfTOtspqFqltwe8; SHOP_ID=7075901688577638662; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1NjI0MTM1LCJuYmYiOjE3NzU1MzY3MzV9.sLE87zt-dPEuRf5yrqO7-0xwfpl-EVDOzpoAzh79WUg; i18next=vi-VN; part=stable; tta_attr_id_mirror=0.1775537767.7625876643680124948; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1775537770%7C259162%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; tt_session_tlb_tag_ads=sttt%7C2%7CiHhd0_VTVzXYzAY5HVf8hf_________176bHukUfGOuuG88-YidTfBynpHWGzETuNiYJeLzceqc%3D; sid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; ac_csrftoken=3a6953b4381b4b159f9c60b73dadf820; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; pre_country=VN; msToken=_UGYSwS3AuCLwlIvgzUISPjWan8OESQmWEqf1Yux9yqRh7wRU54k2nXYMPveUDHmE4H31a-JGN-doElmjaWAn05TWfpLmt11AdqLq2pAolrH8FIVJUr-5pzXetYCUyZdXptP3vnlZX5bxoO7y4FuCRg=; msToken=x6jxXEFskmQIxlckmdb_7E00BejohrZ6LUVj7ykAJcCpg7XGphAdbEL7HPgUKbLsiPxdjyzD5NOU81-V-GWsEYzSw-nR897hMv5lc0Fj3FyCkexOoJjsZ1I_qbkMAsFtHLtZLcV-; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1775547467%7Cf11f280082951da673e6f3dd6dbe47a170671d76089876f1aa5d5619eaa66714; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; user_oec_info=0a53f02a0d9efb166e6094375768c8bc8118740e0652caf7dac649617da4f769f121c8c76fa25b3c805c7b515e378bb10d07e6abb52e840ba245ac52beee73191f2447c93c0811656bdc162363a67a175a4541098a1a490a3c00000000000000000000504620155e3e86acde7a6bcf3f73a51b040a29279db6556dc54e9adf13e208d0ed62d0254d5cb416e0a77ce407d2ccdde7d510d6948e0e1886d2f6f20d22010485379552; odin_tt=68d3c76b0b06bdf7f45c9fafaf47b42d45a612a9f683d27638fa71a5dd6d54ba06ca9cdcf1efb5d3d4aa780782158197693bb9da797f4fddabd341dedb716975"
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
    start_date = (now_vn - timedelta(days=6)).replace(tzinfo=None)
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

    # # ==============================
    # # DELETE DATA TODAY + YESTERDAY
    # # ==============================
    # delete_query = f"""
    # DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    # WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # """

    # client.query(delete_query).result()

    # print("Old data (today + yesterday) deleted.")

    # # ==============================

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







