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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; _m4b_theme_=new; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; sso_uid_tt_ads=f5b34683922d46c4c32543a17ebaa048aea64b6ba554920d96ad0ec995c4ef6e; sso_uid_tt_ss_ads=f5b34683922d46c4c32543a17ebaa048aea64b6ba554920d96ad0ec995c4ef6e; sso_user_ads=419fb6aaf2325c5a86427723f6a0e860; sso_user_ss_ads=419fb6aaf2325c5a86427723f6a0e860; sid_ucp_sso_v1_ads=1.0.1-KDE2NDZlZTQ5ZDAzMzQyNjExMmU3ZWIzYzhmZmI4ODRhZTYxNmZkYTEKIgiUiN7g9dSegGkQuqfAzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQxOWZiNmFhZjIzMjVjNWE4NjQyNzcyM2Y2YTBlODYwMk4KILqm024-yWNt6swAqovtLYCt6mZGtIoEZ0fmqCNRxpmmEiBWoKwzQcjUM9wc3BH0S2i4j1JDpIsCa7BxnAE2PsnMtxgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDE2NDZlZTQ5ZDAzMzQyNjExMmU3ZWIzYzhmZmI4ODRhZTYxNmZkYTEKIgiUiN7g9dSegGkQuqfAzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQxOWZiNmFhZjIzMjVjNWE4NjQyNzcyM2Y2YTBlODYwMk4KILqm024-yWNt6swAqovtLYCt6mZGtIoEZ0fmqCNRxpmmEiBWoKwzQcjUM9wc3BH0S2i4j1JDpIsCa7BxnAE2PsnMtxgEIgZ0aWt0b2s; uid_tt_tiktokseller=5a30e01c3ff717b4832ae0ebd2a830830ce320ea3c45936fd07f0a42058144d4; uid_tt_ss_tiktokseller=5a30e01c3ff717b4832ae0ebd2a830830ce320ea3c45936fd07f0a42058144d4; sid_tt_tiktokseller=482c874891b17862549cc86fd0e455c3; sessionid_tiktokseller=482c874891b17862549cc86fd0e455c3; sessionid_ss_tiktokseller=482c874891b17862549cc86fd0e455c3; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; sid_guard_tiktokseller=482c874891b17862549cc86fd0e455c3%7C1777341896%7C258673%7CFri%2C+01-May-2026+01%3A56%3A09+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CSCyHSJGxeGJUnMhv0ORVw__________WaPndaSONxtkx-8an1tarAl5B66cCmxbKbW28xUzS-Co%3D; sid_ucp_v1_tiktokseller=1.0.1-KGQ2ZmNlODQ3ZTdlMGRmMTRjYmUxMTRiZTk4ZTcyOTQ0MjExMmE4YWEKHAiUiN7g9dSegGkQyKvAzwYY5B8gDDgBQOsHSAQQAxoCbXkiIDQ4MmM4NzQ4OTFiMTc4NjI1NDljYzg2ZmQwZTQ1NWMzMk4KINCFBeYnfqLnXKmgClb9I79CUa5JJCTvfCTrJfehK_9KEiChhQBrP8UhirNb_88ttTulunuELu7LUf10IFPKDpiseBgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KGQ2ZmNlODQ3ZTdlMGRmMTRjYmUxMTRiZTk4ZTcyOTQ0MjExMmE4YWEKHAiUiN7g9dSegGkQyKvAzwYY5B8gDDgBQOsHSAQQAxoCbXkiIDQ4MmM4NzQ4OTFiMTc4NjI1NDljYzg2ZmQwZTQ1NWMzMk4KINCFBeYnfqLnXKmgClb9I79CUa5JJCTvfCTrJfehK_9KEiChhQBrP8UhirNb_88ttTulunuELu7LUf10IFPKDpiseBgCIgZ0aWt0b2s; ttcsid=1777341258980::4swowx2qT4KolwK0iowC.7.1777342154681.0::1.889358.895229::895602.64.69.681::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1777341258979::e5lnHc_0_xAI7055hpdX.7.1777342154681.1; _ga_BZBQ2QHQSP=GS2.1.s1777341258$o7$g1$t1777342154$j59$l0$h1356671933; _ttp=3CyL7hYzxSUswJVhHMF4n7UdSqx; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzc1MzIzMTQsIm5iZiI6MTc3NzQ0NDkxNH0.cjV78FEqG9EUAjmkxRMXK1d-uC5D6pSBsRSJ-z1vQZ8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3NTMyMzE0LCJuYmYiOjE3Nzc0NDQ5MTR9.iQ6TVoF7XUCR-AI7KM7TOKP2NWEBpr9VnQ7-gy2Be3Q; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; tt_csrf_token=Rrb2cl9X-j4fMIrPjcIxhIPfWxj3Nbo0HIaU; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1777450016%7C40758e86c34e08c954e7407c61f95c99666c985f88a2e4206b5effb8b73934a6; store-country-sign=MEIEDN6tHm8E3erEnsr0CwQgcOzcrTS46Zr5AjQQLW0yLCPFkZyW5_6xcdmW8YoDG2UEEDMpTD1D3MsaHHDr8e039fA; s_v_web_id=verify_mojruir4_aG08MoRL_jaV7_4q0r_AGo1_UKNA4ueR2Kap; msToken=rvMXO9yaSURYtAhcQa1pEM7LlMvb-a1e6wL-2whTibtplG8bM8_6Fk7pa1dAXaf6t5frbbPvPttK-f3TojT0r1AiBMvkb4srGd5fT-Ww6z49Sg7EgOBdRvNZouGZqd9YHBE9kxQ=; odin_tt=b821769e7ce3db0190c17b37eebe4c112a63eb117c8ef63cf2dbb878ccb0c20b0fb644e971845d91092afcc9bb9ae9c73ba47bf1e28f96a9d104811f43e9f880; user_oec_info=0a5341dfd327105856f119f1e28c109829e57b2a3f18b60302660dcb2ddbb786e61c03f176e9c3326c49a727586c057193b893470e643f0219ed6f3c6903989ebb98c0ee0257abc8161034d0e121c4efe550b1c8671a490a3c00000000000000000000505c536a1b4214827cfd0a23b023d04b6149318da8002f7a26b82ca564ffb824398eba890f86e352320dbbef1d68a9d4500610ea8f900e1886d2f6f20d220104e660b5e4; msToken=2NaETHV1zSoMNUDhVmSAn-HIdLVnYAbU2UkA6YE75Noj__fbtzDgMqIrA5xN8xxfmZdp7bHOzdRNaUZZmb43E3QxQ--wbSGPd8eMpuGLrSpP6zd9JUPgguUVkRTMN3gwLwGqzgM="
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
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 30 DAY)
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




























