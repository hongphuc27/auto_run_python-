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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _ttp=3AvIaixPN8VfHyK5OTkTyJrsKKD.tt.1; _m4b_theme_=new; ttcsid_C97F14JC77U63IDI7U40=1773635979896::IHQwRRA_YETJxxAS0PmA.3.1773636022342.1; _ga_Y2RSHPPW88=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h640968867; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; i18next=vi-VN; part=stable; uid_tt_ads=ed3399099171552ba3a2970110b15bd620fd463caa0ea8aee84b3b62bd821430; uid_tt_ss_ads=ed3399099171552ba3a2970110b15bd620fd463caa0ea8aee84b3b62bd821430; sid_tt_ads=9a93d6faa80e177431e5cabd65508e65; sessionid_ads=9a93d6faa80e177431e5cabd65508e65; sessionid_ss_ads=9a93d6faa80e177431e5cabd65508e65; _ga_HV1FL86553=GS2.1.s1773829245$o5$g0$t1773829245$j60$l0$h359415048; pre_country=VN; FPLC=l%2Fz79w5do%2F0HqSmU428VsTEkC66IGxkJ7nqKsIlo0%2BYlLV9%2BAvPwZ71l0GWq%2F2DHtBYeTKiaLfkjOE8UHebQEwJZnXjtLw86DvdpJSVioNbTWW%2BTofbzLeY9SX%2B0Tg%3D%3D; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; sso_uid_tt_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_uid_tt_ss_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_user_ads=14d85f2b0adb9b74de123cec9b36ea8d; sso_user_ss_ads=14d85f2b0adb9b74de123cec9b36ea8d; sid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; uid_tt_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; uid_tt_ss_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; sid_tt_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_ss_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQwMDMyMDgsIm5iZiI6MTc3MzkxNTgwOH0.Z0e1l9NWRGB5FU9FB-IfrbDkBMYZe0XjPyB9gW4PqcQ; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0MDAzMjA4LCJuYmYiOjE3NzM5MTU4MDh9.tRueupzlz2Psmfi-hGZ74sMbQgpTzat8xSiizVsShhw; sid_guard_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf%7C1773917200%7C258806%7CSun%2C+22-Mar-2026+10%3A40%3A06+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CtE6h4jZD57IHTJyD6AGvv__________ofSyY124wjArfuNJz-lYs0-mN9doAGDJk-7L10m36hZo%3D; sid_ucp_v1_tiktokseller=1.0.1-KGEzN2JiMWUxNmM4NGY1NzNmNWQ3M2FmODQ4MzQ4MjkwODFlNzcyNzQKHAiUiN7g9dSegGkQkKjvzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIGma_ijqiCSBW2aLJUk4r9_q0DI3z_YE0RCgGUeAkg1mEiB8GioGo0fq54sW_R0MUakAvFNBK1LgRflEyZKBs6xlkxgFIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KGEzN2JiMWUxNmM4NGY1NzNmNWQ3M2FmODQ4MzQ4MjkwODFlNzcyNzQKHAiUiN7g9dSegGkQkKjvzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIGma_ijqiCSBW2aLJUk4r9_q0DI3z_YE0RCgGUeAkg1mEiB8GioGo0fq54sW_R0MUakAvFNBK1LgRflEyZKBs6xlkxgFIgZ0aWt0b2s; sid_guard_ads=9a93d6faa80e177431e5cabd65508e65%7C1773971489%7C259200%7CMon%2C+23-Mar-2026+01%3A51%3A29+GMT; tt_session_tlb_tag_ads=sttt%7C4%7CmpPW-qgOF3Qx5cq9ZVCOZf_________DaE7IXPWkJDVV_pZ6t4OelGj1nMjRtTBX7UfnWo0j3rg%3D; sid_ucp_v1_ads=1.0.1-KGY1OTE0YTY0MGFhOWY1ZDc1NGE1ODUwMTU2OTk1M2E5ZDExNjVhMmEKHAiUiN7g9dSegGkQodDyzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiA5YTkzZDZmYWE4MGUxNzc0MzFlNWNhYmQ2NTUwOGU2NTJOCiD3cjoNj8BAD_XxKmcgV93PLzmpLQ7Wh4VtfOjsGrp1fxIg9fZBZVm84rBKvokX_84ZHNl9yU1NH0DLSqnedZeOj60YAyIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGY1OTE0YTY0MGFhOWY1ZDc1NGE1ODUwMTU2OTk1M2E5ZDExNjVhMmEKHAiUiN7g9dSegGkQodDyzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiA5YTkzZDZmYWE4MGUxNzc0MzFlNWNhYmQ2NTUwOGU2NTJOCiD3cjoNj8BAD_XxKmcgV93PLzmpLQ7Wh4VtfOjsGrp1fxIg9fZBZVm84rBKvokX_84ZHNl9yU1NH0DLSqnedZeOj60YAyIGdGlrdG9r; FPGSID=1.1773972908.1773973084.G-BZBQ2QHQSP.ptUxYtwFy4yuR_YuKqL1Vg; ttcsid=1773972914915::1uzqfv2rYEiPymKjf9zq.12.1773973086478.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773972914914::L8HykAtDISpj945YDcYg.9.1773973086478.1; _ga_BZBQ2QHQSP=GS2.1.s1773972909$o9$g1$t1773973090$j55$l0$h1257713795; msToken=hirGtE7RwmiPSRzOjawHSlL9d0h7Z8t6xCoue_G4c_Hhyd90im6ius_--Aj4FtXWmmrCW7iGwpfRj7pT6x8WVpAz0CSqfoHaqqGOy06QPaAjp_5iAKJyQNicuzuo; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773973380%7C1dd2948dca3f317dffc4c3dfd448cdecbce205a074b1f16f7e83b48e306bfa9a; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=1dd606778557dff0bce66f09cf897375d27e2ec397cd12403c1bb3c29eab8735d39bcf99967ac9e28b0915fae236c55cfa9a4e707967b92d557fe5aea14b02ba; user_oec_info=0a53562d4b45e861ac1e97aafbc5554641f5ce12f29e7dd21f954515ca2f5fef4fda0f85c4e913d167582f5037439ea4a50b37fdffbd49371f3e517de2d993954bb4bc8f0b9d8d18d5cdd09c07b3df0a3d7447d74c1a490a3c00000000000000000000503385c0a1d626a937d474655138e9932934920ceb8789d28b83a4ffa94ca8593985f285aba04f79e8bdef455c0133130b8910e0c88c0e1886d2f6f20d2201046e0df09d; msToken=OmpvDn8-zmFHcGvnXtrrZ6X8D8j4tK12d_4Ggp6hErjgAASR8HFpy0xCF3Am_12eo3XkiiBR287hc-S4nhjy1xYldVtsubCw2Zyp7JAGpBhsFlo8i7v2BeBcCB1ZoSm4axHQSz4="""
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

    # start_date = (now_vn - timedelta(days=1)).replace(tzinfo=None)
    # end_date = now_vn.replace(tzinfo=None)

    start_date = now_vn.replace(day=1).replace(tzinfo=None)
    end_date = now_vn.replace(tzinfo=None)

    # start_date = datetime(2026, 2, 1)
    # end_date = datetime(2026, 2, 28)

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







