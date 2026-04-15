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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; _m4b_theme_=new; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776044459%7C259200%7CThu%2C+16-Apr-2026+01%3A40%3A59+GMT; sid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C1%7CiHhd0_VTVzXYzAY5HVf8hf_________X6QF71wEIh_9vkjIn8RqTBc05Xf_hP2Jh66NFE5GtjZw%3D; s_v_web_id=verify_mny25c44_9XFZ4joY_2GKq_4VsG_BqRv_vj26CWCg9NfH; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776137098%7C1a8de1ed09325198e56bb5017ed49a0d35c99fa80c85156213961287bd599255; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; FPGSID=1.1776137102.1776137102.G-BZBQ2QHQSP.-t5dC0ONNJ8XNqCKcaXvbg; FPLC=Dedymv%2FPgKexpgm6MwQu70V2o0tCdLGm8%2FrUsRi4H5bfGgjFCyx%2F74WLsDtud1I4PGeiozilXmEdU7t0rP192Yb4pL67DBX1BaXkp2jbV5pwjgqyyf2HFDh6dqLmaQ%3D%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1776137101128::5q4gb__G7AmtOjDrYcpq.3.1776137126583.1; ttcsid=1776137101129::Dul7gHZIRT8MHOUSyB4R.3.1776137126583.0::1.-8026.0::31752.14.731.485::0.0.0; sso_uid_tt_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_uid_tt_ss_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_user_ads=3793b57f115dd5c737561e433a6cf308; sso_user_ss_ads=3793b57f115dd5c737561e433a6cf308; sid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776137100$o3$g1$t1776137135$j25$l0$h1248465928; tt_ticket_guard_server_data=eyJ0aWNrZXQiOiJlYWI4NmMyZmFlMDVlZmI5OTc2YjY1Yzc2MGRhMWEzYjA0MWM1NmJiNTAwNWVlNTZjMTIyNDBkYjkxZDZmNzUxIiwidHNfc2lnbiI6InRzLjEuN2U1ZGVmZGNmODhkNTU3YzU4MjUxYmUwNGNkZTNkZGEzNjQ5N2NmZDZjMDFkMGE2Y2Y3M2I5ODhiN2U5ZGVkNDBlNzBiNGJkYTgyYzEzODM2ZTVjZmExODM5NGQ3MDI0MGY4YWYxNjMxZjE2NWFlOTYwMTIyZWVmZmQ0NTMzZGQifQ%3D%3D; tt_ticket_guard_web_domain=2; sid_guard_tiktokseller=88550a7902dbadb4571676b145078738%7C1776137138%7C259199%7CFri%2C+17-Apr-2026+03%3A25%3A37+GMT; uid_tt_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; uid_tt_ss_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; sid_tt_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_ss_tiktokseller=88550a7902dbadb4571676b145078738; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CiFUKeQLbrbRXFnaxRQeHOP________-5u5Y_GrMYQ9o44TDdu9OnOdTJWSMfn3fYhgtNut3lcu8%3D; sid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; msToken=-kBdY9ClUtXa7FivYIVyE067baDRqtsssyYUU5S5ei_0g_4SFP1MGOjEy3Gp22Bq66xl7WziGHNS4ffzWZDd5voexqk2jl6hNMswk0AlcqYqCxHUAFTYSlmKsY9p; odin_tt=87af33c3c268c41de48ade1a80c2f8c16a8921fe2b4900d9fdf41f584d134811e57b07dd75aeac14175ec25818ff47ec23e5e7b40e7999debad1fa5c3701cacb; msToken=sh_V6ZGmzYJ-vSEp2lV8dFJbYeYv_wgMW6hqep66YlOe74XgA_c63yoRR4I95FimBL5_omuY3b_qMs4f_Ta3TMece7epU6vVJir1_QWmY2SgO9sbSsO50oGihEly07nd1xxz1g==; user_oec_info=0a537e014afc3b10bb311f0e9e207182e4530a4a672ce876609deeaf49a485d31ba73d6a043561854ca477f07e58ec1328a05255b1be490c74a6fac9b53d4d9bdc1fb86a4a9fb80bdc648b9378fd51d510bf0b6b0f1a490a3c00000000000000000000504cd4d776abec037f479626c51d387425acc365aed462f1f299903a0afd18110a8b6b476ffa747cfac35f26970c59e62be1109be18e0e1886d2f6f20d22010416376065"
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
    start_date = (now_vn - timedelta(days=24)).replace(tzinfo=None)
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
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL  DAY)
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




























