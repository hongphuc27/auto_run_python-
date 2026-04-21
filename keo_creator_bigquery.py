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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; FPLC=m%2BbWtTwLgm2Gjk1B5ajmnlcKmVWPTC6YfpOmore7wkf5txgFasXSL1TCoHOltuX%2FBjCm0pUxCGcFbpSnrKC8xRuCn8DMgslfhTM%2FUiuCGXWXfUrIwKg9Fx9HXx7%2FkQ%3D%3D; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI.tt.1; ttcsid=1776703789312::Zqs_hQMECBQnDIxcCCnB.5.1776703822619.0::1.-5027.0::33302.11.673.477::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1776703789312::RTb_vX0skd9ZysWHByR1.5.1776703822619.1; sso_uid_tt_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_uid_tt_ss_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_user_ads=a92756a08698fcc35544bb54e270ef04; sso_user_ss_ads=a92756a08698fcc35544bb54e270ef04; sid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776703788$o5$g1$t1776703825$j23$l0$h328177141; sid_guard_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27%7C1776703825%7C259200%7CThu%2C+23-Apr-2026+16%3A50%3A25+GMT; uid_tt_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; uid_tt_ss_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; sid_tt_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_ss_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; tt_session_tlb_tag_tiktokseller=sttt%7C5%7Cq03uHjMyIYq84OhzSustJ__________7hEyD0wF4ndOX2NXXZpZnPJwAQP9zGzJFzvwKgUMFezk%3D; sid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; msToken=lDBJbWpzw_j1ohK267hJAR_9vqo6mOAO89F3mifkSLhcVU9baY0rF5Ll3PsdhGcgJUwLzpjBNHM-y0ZkDvG15n6S4QyxXMVIWT06StwoD97jDN8sHzpye5NfVVIjPg==; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776703928%7Cf663a35b310c322ce2733d4dbc9dc8afe297e10297f4eadfcb995e027100410b; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=92aa5d4aa753744c4082431f0d62d44425c405b1651f5399bd47ecb4bef3c197e24a587dfd51ac54d282d6f27c7b2f41c144136efe523038455198636215aee1; msToken=8D2uYvtVpdUfK8DZ9YYWN2_eHyTyl3D1sKSeYjYUTuYCGm7qNHOUEAiaEx2EZNMCi0itzsALW2_dsxPpXj3xu8ykpC7EMlAv5RjwgkhRG0HvnCXsRfDdVQkFCRiCGQ==; user_oec_info=0a53130d3984bfaa0d4f169fe2c7ffebb2cef3c1af2d39fd21483b0f92feef3d21b599ea24cdefb4688b8b44a460a29f0b31b61c4f1a5a78e58d25433d0a8e393c7d0b003d7f213b0c7ece5428c068a5df165e87411a490a3c00000000000000000000505324332643f4aee5252bef7edf28561c41fb8d7931f6ec91cc8afacc63332a3cfc34ae73427ce40923de8edba1d3d6ceff108cb08f0e1886d2f6f20d22010453f6e489"
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




























