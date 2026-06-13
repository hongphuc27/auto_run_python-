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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; _m4b_theme_=new; store-country-sign=MEIEDOBdiyreOS8suMgNuAQguvxXqVS7WRjmFjGBLXQEcKL0L30mXKL8lmXEg33_2fMEEMy5dGA9tJIbMf5VVKjqDic; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; ttcsid=1781162754752::gVRtD7tzs1BG60jHdONq.26.1781162824655.0::1.15428.17215::65042.34.1157.310::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1781162754758::2YL5uxGovyKKsKGtLZmP.26.1781162824655.1; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_uid_tt_ads=a50a056160da6709828b65d0ca6d18f4668db30dbec18b5c0522eabbe646a2ee; sso_uid_tt_ss_ads=a50a056160da6709828b65d0ca6d18f4668db30dbec18b5c0522eabbe646a2ee; sso_user_ads=fef6739bb9a7684c48a044112d8aeb5f; sso_user_ss_ads=fef6739bb9a7684c48a044112d8aeb5f; sid_ucp_sso_v1_ads=1.0.1-KDEwNDUyN2ZlOGFmMWZmYzI0ZGVlMzRlMzlmMDEyYTVkYWUzNjJiMjAKIgiViLWa9-_oimkQxsap0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiBmZWY2NzM5YmI5YTc2ODRjNDhhMDQ0MTEyZDhhZWI1ZjJOCiACFmjAYhUr73ubLx2QfmjWLj6Tsbsq-SXekO1Bu23CWxIgYHt1J2pb0kFlEEKwmntTn742ZE_MbNUAih1Iji1NX_wYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDEwNDUyN2ZlOGFmMWZmYzI0ZGVlMzRlMzlmMDEyYTVkYWUzNjJiMjAKIgiViLWa9-_oimkQxsap0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiBmZWY2NzM5YmI5YTc2ODRjNDhhMDQ0MTEyZDhhZWI1ZjJOCiACFmjAYhUr73ubLx2QfmjWLj6Tsbsq-SXekO1Bu23CWxIgYHt1J2pb0kFlEEKwmntTn742ZE_MbNUAih1Iji1NX_wYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1781162770$o26$g1$t1781162825$j5$l0$h1993338363; sid_guard_tiktokseller=64e2348d7daf3369584c875f6f42ede7%7C1781162823%7C259199%7CSun%2C+14-Jun-2026+07%3A27%3A02+GMT; uid_tt_tiktokseller=4ea8d5ad2f89cca3a4b1a8535cfdf61d3f4bd1f5fb4a63cb8e0bb2ed8dad0c06; uid_tt_ss_tiktokseller=4ea8d5ad2f89cca3a4b1a8535cfdf61d3f4bd1f5fb4a63cb8e0bb2ed8dad0c06; sid_tt_tiktokseller=64e2348d7daf3369584c875f6f42ede7; sessionid_tiktokseller=64e2348d7daf3369584c875f6f42ede7; sessionid_ss_tiktokseller=64e2348d7daf3369584c875f6f42ede7; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CZOI0jX2vM2lYTIdfb0Lt5__________nVfXlaQ8nFdcyHsTwCdKzEXQ2SHc3CFSAp7-6kbYab4k%3D; sid_ucp_v1_tiktokseller=1.0.1-KDY0Mjk1MjhjNzcyZjA3YjE1YzUxOGY5M2M1MjIzZGE2ZjRlZDg4ZGQKHAiViLWa9-_oimkQx8ap0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2NGUyMzQ4ZDdkYWYzMzY5NTg0Yzg3NWY2ZjQyZWRlNzJOCiCMCrswzIKNqtEjKPH4Y70dZNup9aYo9WhD9s_kOr2X7xIg1xGxSqfU7yb3tZ5KE0Cqjoe3WOtiXKIaiMhG8938wtcYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDY0Mjk1MjhjNzcyZjA3YjE1YzUxOGY5M2M1MjIzZGE2ZjRlZDg4ZGQKHAiViLWa9-_oimkQx8ap0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2NGUyMzQ4ZDdkYWYzMzY5NTg0Yzg3NWY2ZjQyZWRlNzJOCiCMCrswzIKNqtEjKPH4Y70dZNup9aYo9WhD9s_kOr2X7xIg1xGxSqfU7yb3tZ5KE0Cqjoe3WOtiXKIaiMhG8938wtcYBSIGdGlrdG9r; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNTE5NjU5OTg3NzQwNDgzM30=.Ejzck6SGP3teWm3E1ufPEP5fcYzuGR9ztpYKG9kQJAE=; _ttp=3EzK9xoZG7rUM6Bu7ZPYMDn8QBT; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dDRiX2FkcyIsInR0LXRpY2tldC1ndWFyZC1vcmlnaW4tY3J5cHQiOiJ7XCJlY19wcml2YXRlS2V5XCI6XCItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cXG5NSUdIQWdFQU1CTUdCeXFHU000OUFnRUdDQ3FHU000OUF3RUhCRzB3YXdJQkFRUWdXZkF5WUFQSEdpcklycEczdk9EWlFGQU1QZTA0dy9hSWdQRkRiTURrbEpDaFJBTkNBQVJUMzVkOUZtWlp6U2lMaWV6aDlKbHlublRsTDFNMXllM0JTZlNDWFpRRndEN0k1c1hUTmc3SmhWUGdrTGxTNmtZYmRuNU9POWZkMTVDR2IvWnArS0F0XFxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVwiLFwiZWNfcHVibGljS2V5XCI6XCItLS0tLUJFR0lOIFBVQkxJQyBLRVktLS0tLVxcbk1Ga3dFd1lIS29aSXpqMENBUVlJS29aSXpqMERBUWNEUWdBRVU5K1hmUlptV2Mwb2k0bnM0ZlNaY3A1MDVTOVROY250d1VuMGdsMlVCY0EreU9iRjB6WU95WVZUNEpDNVV1cEdHM1orVGp2WDNkZVFobS8yYWZpZ0xRPT1cXG4tLS0tLUVORCBQVUJMSUMgS0VZLS0tLS1cIixcImVjX2NzclwiOlwiXCJ9IiwidHQtdGlja2V0LWd1YXJkLXB1YmxpYy1rZXkiOiJCRlBmbDMwV1psbk5LSXVKN09IMG1YS2VkT1V2VXpYSjdjRko5SUpkbEFYQVBzam14ZE0yRHNtRlUrQ1F1VkxxUmh0MmZrNDcxOTNYa0ladjltbjRvQzA9IiwidHQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; msToken=xZ_6LOYx-oEgQd740QFSoAzQZr6bGxdgyATNpMq4Qbd6cn1xOiyylT5NWbzEDLMuLMupR5uIhX26hyvOhhaT33EzwBIKuoZ_eP5lm_6xI1MvcbQbe6GxNKmD_q-wGlYk74yn3WAzo90mKS0=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1781317090%7C303393fa497a3e015daf6c0b694a6b32a14399343f13dc3387ebbd89189c9f5f; lang=vi-VN; odin_tt=c2b1a46da583955cc1dbf6928efb844fa6b857fb4c2f2d1eb21f721a5e7a63d46218c9112a35fbe50c4aa36b662c121369321464cecce0fcceb3249fe3ac8c14; s_v_web_id=verify_mqbq7het_8rUQbaDS_jQAt_40ba_9tQK_bYtnY9f9uwur; oec_lucifer=AQEBAN0VLH5k6wqRqnHBh7Nl3vQGvPu+0kpHd8I4OaarhBbGFOZn+vqLQ9zZIXxPUN5opwSuU1i1f7UGqDfePt/bahwR+DWYqw==; user_oec_info=0a5379f78b9b47d52047a5f48c25484765da8377b0b0acd0e31af96805130cae452e2fb026d6d6112a7d3e0a459b410154b725b63fe6b5d6c1168a0d056f96f1c4e61c2d440ea7d312ca0849159a423f4ccd21cf9b1a490a3c000000000000000000005089c9468a6468ad138095b98a63aea20d896862f758ca7b4fc897fa23b53583b73264143e775e2cff71e5499561a8912abc108488940e1886d2f6f20d220104d75d187d; msToken=Kwk38q6xL4bt7nSGnSCPvWhazk_dalQgVJg2l4n93xvmLuprI0h-bLCzOudEU4KEvAtdrPhrJgrkKMCFeseh-8CJhOVrx15p8E-T8-zZlF_bBbyyjmunpCw5KF7g9dmPUtAUwmM="
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




























