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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; part=stable; ac_csrftoken=8cd79cf1c74c45ccac78a07fc4a75c3e; pre_country=VN; app_id_unified_seller_env=4068; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0OTk5MjksIm5iZiI6MTc3NDQxMjUyOX0.eW9QIEO9DnGvlEaYJr_-8px2yVO-1LIgsgQ0Zxjs88k; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDk5OTI5LCJuYmYiOjE3NzQ0MTI1Mjl9.CDgMVWF2QU9q6eaOY_V3sWKcqo5TVcTML0WNkWZG-4M; FPLC=BKQ4ZRO1JwKeSGEvYCIImnFJsAUf8LTgQYMDKobiSim%2Fe51dBsyZ8n6CEJuFKylmMr%2FrJgvhks%2FPnGYnRXeJNgRqwR5O1KAT62R2ANLcsb5ggjEYBLlMu7P8p877Gg%3D%3D; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774435900719::0fHO-vrCqBgVRbGDlbk1.5.1774436089399.1; sid_guard_ads=9aa97d4ee3699fd68e07cc81035b8592%7C1774436090%7C259200%7CSat%2C+28-Mar-2026+10%3A54%3A50+GMT; uid_tt_ads=767a13768391ca11693594a32dc6df81cb848e1caf075f876ac63aff9de4e07b; uid_tt_ss_ads=767a13768391ca11693594a32dc6df81cb848e1caf075f876ac63aff9de4e07b; sid_tt_ads=9aa97d4ee3699fd68e07cc81035b8592; sessionid_ads=9aa97d4ee3699fd68e07cc81035b8592; sessionid_ss_ads=9aa97d4ee3699fd68e07cc81035b8592; sid_ucp_v1_ads=1.0.1-KGY4OWU3ODQ3ZTgxYjg2ZTJhNmE0MDY5OTgyODU4ZDQwOWM4NzI2YTAKHAiQiKz8zcvg22kQ-v2OzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDlhYTk3ZDRlZTM2OTlmZDY4ZTA3Y2M4MTAzNWI4NTkyMk4KIE_8QOvBy70BabZJr7gJAFodO1hi0jfazIGTU0KUGWTGEiD7j0gBXuxuAfilZc2gQNxpvg2Ed83SjmJOcUkut-wMMBgDIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KGY4OWU3ODQ3ZTgxYjg2ZTJhNmE0MDY5OTgyODU4ZDQwOWM4NzI2YTAKHAiQiKz8zcvg22kQ-v2OzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDlhYTk3ZDRlZTM2OTlmZDY4ZTA3Y2M4MTAzNWI4NTkyMk4KIE_8QOvBy70BabZJr7gJAFodO1hi0jfazIGTU0KUGWTGEiD7j0gBXuxuAfilZc2gQNxpvg2Ed83SjmJOcUkut-wMMBgDIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1774435922$o1$g1$t1774436092$j60$l0$h1896886948; _ga_Y2RSHPPW88=GS2.1.s1774435900$o6$g1$t1774436092$j60$l0$h1953927926; s_v_web_id=verify_mn67ng38_z1pyaG57_cutL_4ERY_AuJX_kM3TjRc2eXi5; tt_session_tlb_tag_ads=sttt%7C5%7C40e0lu76dpIa2kj1hvdM3v_________FImYpDDl683DVLvTT-53pxoI39xR54X0htEbr27H_VFM%3D; tt_ticket_guard_web_domain=2; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; FPGSID=1.1774453576.1774453576.G-BZBQ2QHQSP.Nsc-D4J5M6T1rZ3gyH0Q4Q; ttcsid=1774453289316::NF5U3uXHmdfcwUGZ_u1d.17.1774453592016.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; d_ticket_ads=cb73b14fab6aad9b8691e11bc72347f87856e; sso_uid_tt_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_uid_tt_ss_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_user_ads=9217a37eb4efc556ee17f0cf0366319c; sso_user_ss_ads=9217a37eb4efc556ee17f0cf0366319c; sid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774453594%7C259199%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CNl44rHhyf0iNxN90xYbMZ_________-_HNHIjdbW97XZvKbwDEBA_EjSBeseaOvDCI7tcnCGdfk%3D; sid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774453595%7C144fecb91e506bb7c417707e5e5f37732f2e97ff6273c4891c8bb6ca350e8801; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=ce3eab95829c840c3351008ed1b202ea4779faf45e0e2e5bdb976c2cf0f72ed626c1b01ba053479a529bba5460f25814d81c870a1905771ab23cc795510f159a; msToken=ZIF9yx_PPeq20eqRYxmYQt14240_hQ1bOrG6I2k5Do0-FvozAAQzLoxuYAPjdxOW6M4dg0TYrs7vjzIIEhSH84GLLL73t0EksCDUFymN0y4dScih9NqOwrSv_dkCdGRt2-vRNSFM; user_oec_info=0a537d9e78a6b10cdbaed83eedb1f3f8526d81abda5e5c50356fb678cf8b51a283407316e0aa5e6fd7aecebec36d7de216376640511108315146402c53608bc2c6f6314754a1c58620a1b885401d4e9cca902205c21a490a3c000000000000000000005039621330e6033bb570828975be2aa6364e4a37f34412e59262c31150074ea5b705ec630a3fb299f6ab31740fbf700a4fdb1098878d0e1886d2f6f20d2201040d0c51b6; msToken=GjVMpD7RWNo4OtYRnM2P16FepSEtXb7QiAoYT3ahK3cgboCT02eIR6isz3CqnuN_Edu_igm6fRJcwVI4biqW_iVnk2KJ8WGzqBBY-Tm9l9X2yAbfvJ2EQgO1zO7gNsOjjSK-Q0Jo7g=="""
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







