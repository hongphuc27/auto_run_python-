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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; s_v_web_id=verify_mq33huor_5oZ7YWSi_XmTd_45S5_8mjj_aDZEAlcFLTer; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; FPLC=PBM9YP5WDlIeVpPaXfqBGxlZwhO7kRI84BVEtjMPEbaqvPN05tFNcZpbBg6d%2BRlKMhfIytEJpSLjy%2FuyYXFqoy7bBSmitSfMAJ%2BdFPEfkQ6TN6JK%2B%2FS4lkmIqyhnHQ%3D%3D; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2; app_id_unified_seller_env=4068; _m4b_theme_=new; oec_lucifer=AQEBAAR9HRsseehRRB1E25dAbPQwuthBLKnxjivqx+CPuUgCuEvQcCeh7imh86ADRbpnT7xm73NpyEYEWG6B4mDaFWcxd4iOgg==; _ttp=3EpngyoACPAtnskmFDB413Vgv2i.tt.1; store-country-sign=MEIEDOBdiyreOS8suMgNuAQguvxXqVS7WRjmFjGBLXQEcKL0L30mXKL8lmXEg33_2fMEEMy5dGA9tJIbMf5VVKjqDic; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; ttcsid=1780887087929::mq20Hy6ZJEpYj-AuR9py.24.1780887256829.0::1.52358.53795::168898.63.920.574::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1780887087929::jVbvylfCROGFCgeMyPEp.24.1780887256829.1; sso_uid_tt_ads=f38b98b03546dad5587412bd092f5d7f59b1fc6c68143be33d5cec00246e2efe; sso_uid_tt_ss_ads=f38b98b03546dad5587412bd092f5d7f59b1fc6c68143be33d5cec00246e2efe; sso_user_ads=0b9d1addb1632b8b4770ebc69b7662dd; sso_user_ss_ads=0b9d1addb1632b8b4770ebc69b7662dd; sid_ucp_sso_v1_ads=1.0.1-KDEyYzE4YjVmNDcxOGFiMDg3M2ZkMDY3Mzc3YmY3NDMxYzllZThkZGMKIgiViLWa9-_oimkQ0t2Y0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAwYjlkMWFkZGIxNjMyYjhiNDc3MGViYzY5Yjc2NjJkZDJOCiBahBagHh00m8nWH4ZXRD-h8LDjiVm7sMu05rqf47vTnRIgTCo7c_GKyY0C2royLCxwV2K5E-mtdgQu5_wE5sf6F3oYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDEyYzE4YjVmNDcxOGFiMDg3M2ZkMDY3Mzc3YmY3NDMxYzllZThkZGMKIgiViLWa9-_oimkQ0t2Y0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAwYjlkMWFkZGIxNjMyYjhiNDc3MGViYzY5Yjc2NjJkZDJOCiBahBagHh00m8nWH4ZXRD-h8LDjiVm7sMu05rqf47vTnRIgTCo7c_GKyY0C2royLCxwV2K5E-mtdgQu5_wE5sf6F3oYASIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1780887087$o24$g1$t1780887259$j60$l0$h1123907633; FPGSID=1.1780887081.1780887250.G-BZBQ2QHQSP.weoCXQSB_2TsNWEuC0Ehzw; sid_guard_tiktokseller=0c30898f4842eeb05bfd76441049cdb2%7C1780887250%7C259200%7CThu%2C+11-Jun-2026+02%3A54%3A10+GMT; uid_tt_tiktokseller=b5775f52103025e19c86012f97094c4f9fa05980012e6fbf79b1a69b4f5b97c0; uid_tt_ss_tiktokseller=b5775f52103025e19c86012f97094c4f9fa05980012e6fbf79b1a69b4f5b97c0; sid_tt_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; sessionid_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; sessionid_ss_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CDDCJj0hC7rBb_XZEEEnNsv_________3sbeDZCJmSUHksag_gaPrj6hdxM2ktS9WJ6ViY-0FC3k%3D; sid_ucp_v1_tiktokseller=1.0.1-KGRjZGE4MzJlYjI3YWFkZmQzYTU5NDQ4YTc5OTkzOWZjN2Q0NGRiZjgKHAiViLWa9-_oimkQ0t2Y0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwYzMwODk4ZjQ4NDJlZWIwNWJmZDc2NDQxMDQ5Y2RiMjJOCiB4TKxClobqf7EDVxOizR5ChMprvZjnjnUafeHuLLSaURIgjqtiFirRZMX6EBUmDheA2J-sjKPHET2q9192g-cPScMYBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGRjZGE4MzJlYjI3YWFkZmQzYTU5NDQ4YTc5OTkzOWZjN2Q0NGRiZjgKHAiViLWa9-_oimkQ0t2Y0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwYzMwODk4ZjQ4NDJlZWIwNWJmZDc2NDQxMDQ5Y2RiMjJOCiB4TKxClobqf7EDVxOizR5ChMprvZjnjnUafeHuLLSaURIgjqtiFirRZMX6EBUmDheA2J-sjKPHET2q9192g-cPScMYBCIGdGlrdG9r; msToken=rqSQU_N2vyv8Xl1jeqwIyG8X7QkUIedUGugcchMqHDaopqQh_sI9R802w0EDICyRSS0JNBUauLBW4qPFD2qSFCm79P0Q0f1AQ39MDBlxdMDH5ZnGXyAsnY4RutHkgKQ-Z3M1BcQ=; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1780887251%7C63cccacc5683ef3cf8c477b960bfd2e2f6009b88c17a1e4f30b8c034ad9f0d35; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODA5NzM2NTIsIm5iZiI6MTc4MDg4NjI1Mn0.OVioRrwuXHHHhtodxcGDi3ZWJMsNBSdGnYXz-Z5xE-8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgwOTczNjUyLCJuYmYiOjE3ODA4ODYyNTJ9.gOzDdOrV_3QRuZ-sYoxynMKPV61T7vl1rs7R6ukBcEc; odin_tt=02260a005d4717886bfc32ee78e3404755b4048bc3511c86c6b4c6977682f3c6c3ca3f7ec1e0c2017045c1905ae069ea46ff11afeca7c246cbd64eccf3b98e19; user_oec_info=0a534e85bf0f3866bc7aba9713474418c010489fa2efaa553b41f85cb5fd848f27f9325c4172f31194c8b995c86a7f6c8c8f4229b502df9a58632eea7b39055a8834cf16e904c176e2a7a5a765eac3a588a8ae97a61a490a3c000000000000000000005084547225e20a04559c1531620314dc7a329d20e2473407a14985745cae9ccac4ec699642e2340f60df7941eeceff782d5610abce930e1886d2f6f20d22010401dcabea; msToken=v-6XPe5aQ115CnCNoxraJg0QYmouiKhgN87pxhxR5Ax_YTqcZslmmoj7uAodvwHkTBMXYSQ072End5zng-m_awV9SBhLBfLHtUrZY58aWst75Po9qwDtbsJ32KWa2KOKL8dfbP8="
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
    start_date = (now_vn - timedelta(days=31)).replace(tzinfo=None)
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
    WHERE id_shop = '7494545630022240481' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 31 DAY)  
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




























