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
    "cookie": "tta_attr_id_mirror=0.1777215607.7633082911700287489; passport_csrf_token=63ea3d889eb0d0eeaaab0e5984a30479; passport_csrf_token_default=63ea3d889eb0d0eeaaab0e5984a30479; part=stable; _ga=GA1.1.1438146305.1777215645; FPID=FPID2.2.LXrw%2B1E7zDZp0p8S3fjDTPE8AO0Hrhv8RzQnjs91HHs%3D.1777215645; FPLC=c03a8BVA%2Bzq0hiQ1adNGW%2F%2BCJV1rG%2BuSPivCvWA9CwGcueYzsXvteq%2BMKMDlIIcQsFEM8EzptpnJHl90Ogy6Tbub2UcmMGfaK3SgxhL9GHkD5fQ7wJk8hR8ltAqB6w%3D%3D; _ttp=3CtrHuZIj7cqDujYFAbJjyjs6Q0.tt.1; _tt_enable_cookie=1; FPAU=1.2.811625642.1777215625; _fbp=fb.1.1777215624514.1056617089; _gcl_gs=2.1.k1$i1777215629$u242214610; FPGCLAW=2.1.kCj0KCQjw77bPBhC_ARIsAGAjjV_4-F5ntCxGTCXsacfMRuIXnctwnzcn1GozdajMGcjRjG9BwlznDWQaAqBxEALw_wcB$i1777215626; FPGCLGS=2.1.k1$i1777215609$u242214610; _hjSessionUser_6487441=eyJpZCI6ImI4ODE4MDk0LTFiMGUtNWNlYi05NjAzLTIyNTRlMTdmMDZlMSIsImNyZWF0ZWQiOjE3NzcyMTU2NDgwNDIsImV4aXN0aW5nIjp0cnVlfQ==; _hjSession_6487441=eyJpZCI6ImNmM2M0OGFiLTQyYzMtNGY0My04YWE3LWUzZDlmZTZiYTU1MyIsImMiOjE3NzcyMTU2NDgwNDksInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; d_ticket_ads=0587dc5d429c2fc26b66c405fac4ba2e5f6be; sso_uid_tt_ads=002a6c22337ab95b0736b5b5190ed602a407c7d2bb4f0594edd9acb7784fa1f3; sso_uid_tt_ss_ads=002a6c22337ab95b0736b5b5190ed602a407c7d2bb4f0594edd9acb7784fa1f3; sso_user_ads=c00738e37af3fae9e49aa95bb195c12b; sso_user_ss_ads=c00738e37af3fae9e49aa95bb195c12b; sid_ucp_sso_v1_ads=1.0.1-KDViNzdjYWFmZTAzZGVkNzVlNjk4NWM0YTZiNTc5MWQ5YWJmYjc5ZWEKIAiUiN7g9dSegGkQ19K4zwYYrwwgDDDN9YHIBjgBQOsHEAMaA215MiIgYzAwNzM4ZTM3YWYzZmFlOWU0OWFhOTViYjE5NWMxMmIyTgogutE88JNqM39e1CmigoQwJJot1VA7yHbg9heFsNM0joQSILnP80gbWf5m-1lFQMUNjTYrIvPAD2rTAb_3u9TeANqrGAIiBnRpa3Rvaw; ssid_ucp_sso_v1_ads=1.0.1-KDViNzdjYWFmZTAzZGVkNzVlNjk4NWM0YTZiNTc5MWQ5YWJmYjc5ZWEKIAiUiN7g9dSegGkQ19K4zwYYrwwgDDDN9YHIBjgBQOsHEAMaA215MiIgYzAwNzM4ZTM3YWYzZmFlOWU0OWFhOTViYjE5NWMxMmIyTgogutE88JNqM39e1CmigoQwJJot1VA7yHbg9heFsNM0joQSILnP80gbWf5m-1lFQMUNjTYrIvPAD2rTAb_3u9TeANqrGAIiBnRpa3Rvaw; sid_guard_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9%7C1777215831%7C259200%7CWed%2C+29-Apr-2026+15%3A03%3A51+GMT; uid_tt_ads=87c0fd1e98ea3eec64dc4d5fa01187825ab8fcb6f80e81d23f88c5e7d902161c; uid_tt_ss_ads=87c0fd1e98ea3eec64dc4d5fa01187825ab8fcb6f80e81d23f88c5e7d902161c; sid_tt_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sessionid_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sessionid_ss_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sid_ucp_v1_ads=1.0.1-KGJlYzIwMTQzY2VhOTNlMGEzMWUyNmEyMmYzNWMxZjM1ZWFjOTI2ODIKGgiUiN7g9dSegGkQ19K4zwYYrwwgDDgBQOsHEAMaA215MiIgMDQ0YTMxYmZlMWI5M2NiOWMxZjkyZTBhYjFlNmEzYjkyTgoglE_F24X5WvJRe04rizh8kduLIcIEH4gyjCOj22JNdtESIAFwaeuHM_76P-gkjrxy5XRzqub5EJYbxwVmLkEQoZlNGAIiBnRpa3Rvaw; ssid_ucp_v1_ads=1.0.1-KGJlYzIwMTQzY2VhOTNlMGEzMWUyNmEyMmYzNWMxZjM1ZWFjOTI2ODIKGgiUiN7g9dSegGkQ19K4zwYYrwwgDDgBQOsHEAMaA215MiIgMDQ0YTMxYmZlMWI5M2NiOWMxZjkyZTBhYjFlNmEzYjkyTgoglE_F24X5WvJRe04rizh8kduLIcIEH4gyjCOj22JNdtESIAFwaeuHM_76P-gkjrxy5XRzqub5EJYbxwVmLkEQoZlNGAIiBnRpa3Rvaw; ac_csrftoken=b4ee2928a7c54aef821542be30185d39; ttcsid_C97F14JC77U63IDI7U40=1777215645461::p6h_FDXSDxcKh-09e412.1.1777215853198.1; pre_country=VN; tt_ticket_guard_client_web_domain=2; tt_session_tlb_tag_ads=sttt%7C3%7CwAc443rz-unkmqlbsZXBK__________zzEmxgmagr-hC0N86DlOqhBBPqiFbNFiJHT8p5Ktadlk%3D; uid_tt_tiktokseller=d0c5939df306ac227d194b482ff97d9d6d165915cb75cd182a6b3061a48e4cdd; uid_tt_ss_tiktokseller=d0c5939df306ac227d194b482ff97d9d6d165915cb75cd182a6b3061a48e4cdd; sid_tt_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; sessionid_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; sessionid_ss_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzczMDIzMTMsIm5iZiI6MTc3NzIxNDkxM30.zqheM39GUCEsrUJgvhjHmN2x5qMSHEmO5qXHvJ5XGlw; SHOP_ID=7075901688577638662; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3MzAyMzEzLCJuYmYiOjE3NzcyMTQ5MTN9.piskn8aJEeGVxyeiI-vja-W13eHd7CmEb54RAPBBfxA; _m4b_theme_=new; sid_guard_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc%7C1777215918%7C259113%7CWed%2C+29-Apr-2026+15%3A03%3A51+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CK12h7NikWwmZyIpcNLaYvP________-2GgD548kse-F80jfcPXRHfSPkaxdrgAGevv5UM1loe3s%3D; sid_ucp_v1_tiktokseller=1.0.1-KDFlNDExODU5MDg2MGYzZmFmM2MyYWFmNTQ0OTZlNzQ2OWE2ZGZiN2IKHAiUiN7g9dSegGkQrtO4zwYY5B8gDDgBQOsHSAQQAxoCbXkiIDJiNWRhMWVjZDhhNDViMDk5OWM4OGE1YzM0YjY5OGJjMk4KINVOzpV2UrxlqJSPc33uCJT0a1q0Rm9J-IBv48gydozNEiAbHpwDSqe2oEZmBsE9cKk_20f7OVT-HVcJbB5yoRlTDRgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDFlNDExODU5MDg2MGYzZmFmM2MyYWFmNTQ0OTZlNzQ2OWE2ZGZiN2IKHAiUiN7g9dSegGkQrtO4zwYY5B8gDDgBQOsHSAQQAxoCbXkiIDJiNWRhMWVjZDhhNDViMDk5OWM4OGE1YzM0YjY5OGJjMk4KINVOzpV2UrxlqJSPc33uCJT0a1q0Rm9J-IBv48gydozNEiAbHpwDSqe2oEZmBsE9cKk_20f7OVT-HVcJbB5yoRlTDRgCIgZ0aWt0b2s; _tt_ticket_crypt_doamin=2; _gcl_aw=GCL.1777215942.Cj0KCQjw77bPBhC_ARIsAGAjjV-zCP_uZ1q-IkAQ8wuQAaqcvgY-cyK5c-8wENwlmIkEkD_O9s7MvNYaAsyuEALw_wcB; FPGSID=1.1777215921.1777215921.G-BZBQ2QHQSP.hXyIXwM6GrUdM3KJie52ow; _gtmeec=e30%3D; i18next=vi-VN; ttcsid_CMSS13RC77U1PJEFQUB0=1777215942904::HRsKDlMTJ-tBoRSV9e6D.1.1777215948035.0; _ga_BZBQ2QHQSP=GS2.1.s1777215942$o1$g0$t1777215948$j54$l0$h2004259644; ttwid=1%7CYV2NNRyt_v5nurwnWGe9UVmRB3RzTXAmbV6XiQ4T3JM%7C1777215957%7C17bce07fc7b34dec7c30963cb5ade85f483126fb4a8fcf411cc56fe7abc5294f; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJPUngxSTJnZEVYdTdqZGZlUHJybEVWYTRIOVJYcTJ5bmhnbG9hUDFoWXJpelJrblozMmRXQXZsSWNJOHBNQi9lTkJvMnhiSzM3LzlHVkRWd28xK2lucz0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; ttcsid=1777215645466::EcGEr7PYklXKjU6ZetYc.1.1777215853198.0::1.207688.0::226998.14.501.274::409190.17.23; _ga_ER02CH5NW5=GS1.1.1777215879.1.1.1777216131.0.0.186807175; odin_tt=4f5bcdf859edacf9bb5420dbb0092735a06bf8fcc53c2dfe1dd1269dfc4b38572dbaf8562c113d5750bb8ccf216c2f3743ba9926d53927942b1639d31854ca06; _ga_Y2RSHPPW88=GS2.1.s1777215644$o1$g1$t1777216366$j60$l0$h918202082; _ga_HV1FL86553=GS2.1.s1777215879$o1$g1$t1777216366$j60$l0$h191191230; msToken=jvOwvXCHYoXXO1Oqxhm_9ZgoH5NQvCBwvpHPk9g1MnAicwswed2zhe0QMYxCpdVBQ-7qKeG7zhudZeCzBu6swQ0ovf36xpBEUUp4CtzDW0vuCkkfXG4Krh0irXUF; user_oec_info=0a53c45acdaad0895fc736566745b3ca12166808531088243513bef7b6aa9414f05e2dd0aa7e44887de076b33aa45e8989705735c7d0f918d969cc36016fa199aaeed8924606ceebd97aea17037cc06b8a1798bcf31a490a3c0000000000000000000050593c183d0f60868cd5a393957b1532128ecf4c69e4d5ef25e192e1fbdee393d9ca979a3c15cab207cfce116938160a5de310d4ef8f0e1886d2f6f20d220104f1e5171b; msToken=8TwsfSxK2N1aOH4kT4jh1nbZZjcHdk_L6Ega2AH9G2iE6ryKZt95qquC6z155Ckd6V0LWc_ncKZuev9qerS3ObMQzOhUyJBpd6a6aOO-ipQO64YvvuyqkwzAuAN2MIG0oQ9DLipJ"
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
    start_date = (now_vn - timedelta(days=30)).replace(tzinfo=None)
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
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
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




























