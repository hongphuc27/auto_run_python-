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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; _m4b_theme_=new; sid_guard_ads=8befa66f0da6ecc549dc6781bc35e632%7C1779273067%7C259178%7CSat%2C+23-May-2026+10%3A30%3A45+GMT; oec_lucifer=AQEBAIW4e0LzJkLGlwXM6lVdaNVAdWgSww2E30v6v2GyC7J6hoItGN42fAPQNeqsqyjlBwyF34c/J0OrhSBpaOFmBuLpod1VLQ==; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde; d_ticket_ads=40b4f7f55431d4013801acad214d89a237e1d; SHOP_ID=7075901688577638662; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; store-country-sign=MEIEDN7KYgcYAmICdqEsyQQgtFaCrRfOPLDlTZGURIW9cRywYA1K8dcKDbqAfLEhNqgEEAngrutLQDckLM5p67Dx5HI; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzk2MTk0MzgsIm5iZiI6MTc3OTUzMjAzOH0.RO4W5jDJKthkKev-rAMmnFF4xfVKEClwPtNduPEH3gE; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc5NjE5NDM4LCJuYmYiOjE3Nzk1MzIwMzh9.TI6vqw-2UaQY863POV-ugH65DH3TueaHbdbCi3c25O8; FPGSID=1.1779592402.1779592402.G-BZBQ2QHQSP.fBd-tKSI1CKGOaHaCEGimA; _ttp=3E3lDCYg6aUPbihCwDLmwB1Adif.tt.1; FPLC=SYLsQXmzdNPxLDdTJ%2BDLuIu%2BAZPKO8X8XaE8wjSeAedd%2F97aTMCs5BQ0FOnNjtQ4BJWXycGax5zhx%2FB9ACg%2BobkOTKymIgvv7RjNVJNNIkdBxK3ftNgR4veTXOIG%2BA%3D%3D; ttcsid=1779592402588::0qhPlraYGPQfMqKc8UWy.18.1779592421109.0::1.-5587.0::18519.7.769.496::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1779592402587::aa_IaWO5jTVs76_DCcrA.18.1779592421110.1; sso_uid_tt_ads=e36a9dd4d48e72669897311ac594b57d855a28d597f622a3acfc8f4650371f1c; sso_uid_tt_ss_ads=e36a9dd4d48e72669897311ac594b57d855a28d597f622a3acfc8f4650371f1c; sso_user_ads=83c7cdd25bf381bac80cef61eb8486e0; sso_user_ss_ads=83c7cdd25bf381bac80cef61eb8486e0; sid_ucp_sso_v1_ads=1.0.1-KGUwMmRiMGFlMTAzMWY0NGJhNjdiM2UyNmMzMzAyZWIwMTgzODhhODIKIgiUiN7g9dSegGkQ6NnJ0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA4M2M3Y2RkMjViZjM4MWJhYzgwY2VmNjFlYjg0ODZlMDJOCiADVoLO7-7mPjxWhebajj9MukEwXiWnVIlyGa2IpkLznxIgKgya4CiqO8LMjf_l56y6L9hS_GXdkq-TQdtsM6Xm7g4YBCIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGUwMmRiMGFlMTAzMWY0NGJhNjdiM2UyNmMzMzAyZWIwMTgzODhhODIKIgiUiN7g9dSegGkQ6NnJ0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA4M2M3Y2RkMjViZjM4MWJhYzgwY2VmNjFlYjg0ODZlMDJOCiADVoLO7-7mPjxWhebajj9MukEwXiWnVIlyGa2IpkLznxIgKgya4CiqO8LMjf_l56y6L9hS_GXdkq-TQdtsM6Xm7g4YBCIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1779592402$o18$g1$t1779592424$j38$l0$h1792137946; sid_guard_tiktokseller=69d4a1b33d5f77c415c49b9b08b6a9d3%7C1779592424%7C259200%7CWed%2C+27-May-2026+03%3A13%3A44+GMT; uid_tt_tiktokseller=07b9b53b5072fd43707e9c402217665b6d2e2ededd0ea7fad278c910a2ea45dc; uid_tt_ss_tiktokseller=07b9b53b5072fd43707e9c402217665b6d2e2ededd0ea7fad278c910a2ea45dc; sid_tt_tiktokseller=69d4a1b33d5f77c415c49b9b08b6a9d3; sessionid_tiktokseller=69d4a1b33d5f77c415c49b9b08b6a9d3; sessionid_ss_tiktokseller=69d4a1b33d5f77c415c49b9b08b6a9d3; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CadShsz1fd8QVxJubCLap0__________ekAZqHA7AmB9vaqH7ycWlD7LVQUhepDhcYqfuBvpoyBA%3D; sid_ucp_v1_tiktokseller=1.0.1-KDU4NzlhZWM0ZmEwOWZhNGY3MDk3MTIwODEwOTk4M2RkNThjOGI5NzAKHAiUiN7g9dSegGkQ6NnJ0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2OWQ0YTFiMzNkNWY3N2M0MTVjNDliOWIwOGI2YTlkMzJOCiA9QC6jQxtWt90giYSyOwKIHVWjdCyVh3bhmwwcHR16ZRIgfoUvR2zedgyjC_zhHMWivA-woF_vgXsAHfPTsh54G4MYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDU4NzlhZWM0ZmEwOWZhNGY3MDk3MTIwODEwOTk4M2RkNThjOGI5NzAKHAiUiN7g9dSegGkQ6NnJ0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2OWQ0YTFiMzNkNWY3N2M0MTVjNDliOWIwOGI2YTlkMzJOCiA9QC6jQxtWt90giYSyOwKIHVWjdCyVh3bhmwwcHR16ZRIgfoUvR2zedgyjC_zhHMWivA-woF_vgXsAHfPTsh54G4MYASIGdGlrdG9r; msToken=9OSngYgk6ZaLrAiX586jhJ6MDprtXXMNNJpMzfsaDgknUohind9Qk24pB4EsBHwbzom30ncFqbvD4qvRhBGF-8GFcWh5oziOSpCoIVSQNWNYo7BcspARLBOJTPTzTg==; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1779592426%7C282a3da7fba7543272b71fcd484d9468a76723adcccc206f89ff6558b78da8fb; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; odin_tt=91a60c472533c27e29b9cc7a1a767c72e6687c250258eed7c64ebaaad44c8a274d1a0944ac789e3acb750515acfeaa153ea0715e0b0a8c15bd060d70fea12b6d; user_oec_info=0a530eb17fdf1f9d9cb46aa9126cb09ea7c92bf954f0570f95ac9e6eadbe192bff585579bef823c4fead2b4528179e5581c596971487b2d324df68664f568dab45318f99f369f87db47f85da50fdbd7feeaf02622d1a490a3c000000000000000000005075be8e4664ecefd3cd0c0d1c8b273eb4ef84c93988711e489aa945c2f6a6efc3df767c824cb9e7cf5dd562ad1ba1fc3b5310e2a4920e1886d2f6f20d2201042d3b3638; msToken=i6PWJBxKVYW-H5FvmZLii2nuTNd0uWNpgFl69mwdtgL_tQjmh1GbeUM2RhQUfLriQRHCE2NLAsNGWBz2e05iOVeU4_0D2jJRdfu8x_Cj91lnA2WaYTthSJ0I59efSlUpOruToRyBFQ=="
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
    start_date = (now_vn - timedelta(days=26)).replace(tzinfo=None)
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
                        "7494545630022240481",
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
    "id_shop",
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
    WHERE id_shop = '7494545630022240481' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 26 DAY)  
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




























