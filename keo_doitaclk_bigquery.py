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
TABLE_ID = "fact_creator_doitac_tiktok"


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
    "&msToken=N39EX-R9gz5-RWGhKsnkNcTiBj_s9RJywGz1KxvmlifSCTicC2Z84UP1BYyPLNipT9_XC7wx4EAflfJ5T5jO6fenzwTdNjSF3G6012rCC0dO5dOqVZ7TNutSbVVOKQXnQvfyQQ=="
    "&X-Bogus=DFSzswVOy4LxJoKzCix8ZcVRr3Em"
    "&X-Gnarly=McKhhGvnEty2jB08p73KTJbQRgneIjbdWoj7xQcJnriCp5iY-1M7t2TNGlq1bJ43yq8-wP19G5Bgd/Yr-O0zCrmRClC2rUAuU1uJXwrCm37Bbre0Fk9-Q6236es8-uDx6MOlYHkxtOFTWZbtip118dm8E7hwJj-5YztHcX8fpV5YdDEmGUbIkk8mF4STC4/ADhTgbFvFuXkQLfxRV3LbWiZKGdKrT9EU01e1j0Z9P1dxf65b6s0JO76J7SV86-v0Z-YWMq0J-UJw"
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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; d_ticket_ads=3efc70f037472f37f0a99d03fb4c56117856e; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; _ttp=3ASnmGtCNVoiR5Q0m1ZGjkJ1Nuz.tt.1; i18next=vi-VN; tta_attr_id_mirror=0.1772797845.7614108765217882120; ttcsid_C97F14JC77U63IDI7U40=1772797849194::GdOHloCKF5kUGxOaQEGn.1.1772797859207.1; sid_guard_ads=335356ce1cf65c60fa6c0d0e292dca5a%7C1772869877%7C89785%7CSun%2C+08-Mar-2026+08%3A47%3A42+GMT; _ga_HV1FL86553=GS2.1.s1772869878$o2$g1$t1772869878$j60$l0$h1122399611; _ga_Y2RSHPPW88=GS2.1.s1772869878$o2$g1$t1772869878$j60$l0$h1899419535; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; s_v_web_id=verify_mmnbqczx_8mO8y1sl_oiPW_4ODC_8Vm3_hl9SOAQ6L8jz; store-country-sign=MEIEDKVhBJsg9Svtt5syGQQgaQx_fDM-nYWpPnOidzMCOr-ZahIKe0MhH2bajzOFnkAEEIeFccMlNmcLKUlWJdEox28; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773311259%7C857d89851a65dbd9bd42725a3dd97d024639b2b4bf84745b123de5caaed201e6; FPLC=jaeddaTc8RimhRm0pu6uRJXbj8rcZ79pFJ223cmBQzSXReLbiNMLOL8p1xxofRBqLNiuWwTlgr4IXUY8GLXweWTI4CCleG1%2FPqGSASgN7fMILx%2FocIqwUh5IhaNVhw%3D%3D; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; ttcsid=1773311260360::xi1KAn7tzYg-Hk0tHwSK.5.1773311321429.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773311260360::q6CVLjDSysEYs4s8R7rR.4.1773311321429.1; sso_uid_tt_ads=3ef4a8c89e7c8d54a3ac42a48ae01820afd1480eee877c87307bba710564185a; sso_uid_tt_ss_ads=3ef4a8c89e7c8d54a3ac42a48ae01820afd1480eee877c87307bba710564185a; sso_user_ads=127acf422f4426dec6848eb92528569e; sso_user_ss_ads=127acf422f4426dec6848eb92528569e; sid_ucp_sso_v1_ads=1.0.1-KGVlZGEzYWY2YTVhMDA0ZmE3M2MxNTNhMzk5YjlhNDlmYTc3Y2JiYmQKIgiUiN7g9dSegGkQ2qrKzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxMjdhY2Y0MjJmNDQyNmRlYzY4NDhlYjkyNTI4NTY5ZTJOCiAF8JQLPD8r7Xb9eH_JXSHjSwAmjJ1oH6-LwhfESKnIchIgx_n8Ipadrv05UzTT1rNNaY3q93HdFpldCuX6jzorivYYAiIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGVlZGEzYWY2YTVhMDA0ZmE3M2MxNTNhMzk5YjlhNDlmYTc3Y2JiYmQKIgiUiN7g9dSegGkQ2qrKzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxMjdhY2Y0MjJmNDQyNmRlYzY4NDhlYjkyNTI4NTY5ZTJOCiAF8JQLPD8r7Xb9eH_JXSHjSwAmjJ1oH6-LwhfESKnIchIgx_n8Ipadrv05UzTT1rNNaY3q93HdFpldCuX6jzorivYYAiIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1773311260$o4$g1$t1773311322$j60$l0$h959578442; FPGSID=1.1773311260.1773311322.G-BZBQ2QHQSP.C-k4IlKAIIAYCji-Q13vBQ; tt_ticket_guard_server_data=eyJ0aWNrZXQiOiJkZGEzMjBkN2VjODdiMDA3MTA1OGQwMDNiYThhOGJiZDcxNTU2ZTAxYjEyZjUzODdlNjQ1ZjY4NDQwNzYxYjA0IiwidHNfc2lnbiI6InRzLjEuZDIzMTY3NWJhZWUyMDkxNzIxNmMwYWYzMmI3N2U2ODc2YzlhZjA2YjcwOTE3MmFiZjRiZTE3YTkzZGFkYmE2ZTBlNzBiNGJkYTgyYzEzODM2ZTVjZmExODM5NGQ3MDI0MGY4YWYxNjMxZjE2NWFlOTYwMTIyZWVmZmQ0NTMzZGQifQ%3D%3D; tt_ticket_guard_web_domain=2; sid_guard_tiktokseller=451bae7d6cc02b35e80035747b893ca9%7C1773311323%7C259199%7CSun%2C+15-Mar-2026+10%3A28%3A42+GMT; uid_tt_tiktokseller=6ca3783a36c8e7fad0380e9a02c7f32f0dcfea3b2f9c039d4570beb3527a767b; uid_tt_ss_tiktokseller=6ca3783a36c8e7fad0380e9a02c7f32f0dcfea3b2f9c039d4570beb3527a767b; sid_tt_tiktokseller=451bae7d6cc02b35e80035747b893ca9; sessionid_tiktokseller=451bae7d6cc02b35e80035747b893ca9; sessionid_ss_tiktokseller=451bae7d6cc02b35e80035747b893ca9; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CRRuufWzAKzXoADV0e4k8qf________-fP-VaOtcbQfbvs8rI4GwMNzIQBu-d-MGFVF8_Jiyvlrc%3D; sid_ucp_v1_tiktokseller=1.0.1-KDMzZDA5YzdhOWFlMDliN2Q2NjRkODNmNDk4MmE4NTVmODYyOTE2YjIKHAiUiN7g9dSegGkQ26rKzQYY5B8gDDgBQOsHSAQQAxoDc2cxIiA0NTFiYWU3ZDZjYzAyYjM1ZTgwMDM1NzQ3Yjg5M2NhOTJOCiD5HZtPb4Ni7IRz0yH_aXLMnP_PktJY2CWV2NezKWuaXhIgB7zu0g-QbE-0-SkxyouoE7nEonBAzP4CiD_lQ7Lkw4wYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDMzZDA5YzdhOWFlMDliN2Q2NjRkODNmNDk4MmE4NTVmODYyOTE2YjIKHAiUiN7g9dSegGkQ26rKzQYY5B8gDDgBQOsHSAQQAxoDc2cxIiA0NTFiYWU3ZDZjYzAyYjM1ZTgwMDM1NzQ3Yjg5M2NhOTJOCiD5HZtPb4Ni7IRz0yH_aXLMnP_PktJY2CWV2NezKWuaXhIgB7zu0g-QbE-0-SkxyouoE7nEonBAzP4CiD_lQ7Lkw4wYAyIGdGlrdG9r; msToken=5zU_KUR7CyH308GUxSuVrTgqAtuK9LnklhM0qMqyUcTkkhSURSE8RfezxtAuj6DuMJlT3JrDdESk3W_jdEcclyGrVAYSJkNxZDvK_JxVqNbL62l0-FGGLteWrrPvHn07Ed1VjNQY; odin_tt=beb8f40d848f033f7f246ce5e06ba41fadcc50666d45e40f254fca39272bca630b11ea6c084fd49fdfca65125b7cd40df2e8b35a4f0e119790c6bfbecc53743f; msToken=3Tt--HP1xoYcbkPDJ-sknufncIUL1znylyjra6mkclGNAZGxSSLoA4u77RKIPSEnn_vSimTVlYq6Hegq3DSw6sNVQHPDTQS9orUL9AQW3CdIdpWA1gRa1LLclVwgk9eTzsJe8bk9; user_oec_info=0a5318a9f08833798a3e17b5e911a973c980239dd1c7d0a066a7bd267b4531ad07ad0cdc06de0d1ba4179ba81ded6e3d85c6318eaf5ca32281ede3c1d3a8c3b08753be773205fa2aa269400669e588fdffcd7daad11a490a3c00000000000000000000502cc07eb070bbbfb8136b6e3e2d723767a3419da52581219cec13cb0c3d895b2732cdbf9117dcfb6e0634ad31fbfd0080d910b2f28b0e1886d2f6f20d22010465cf7189"""
}


# =====================================================
tz_vn = timezone(timedelta(hours=7))

# =====================================================
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

    # start_date = datetime(2026, 2, 1)
    # end_date = datetime(2026, 2, 28)

    # Kéo từ đầu tháng
    start_date = now_vn.replace(day=1).replace(tzinfo=None)
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
                    create_time_ms / 1000, tz=tz_vn
                )

                sku_details = o.get("sku_detail", [])
                for sku in sku_details:

                    sponsor_id_raw = sku.get("sponsor_id")
                    sponsor_name = sku.get("sponsor_name")

                    # cast sponsor_id an toàn
                    try:
                        sponsor_id = int(sponsor_id_raw) if sponsor_id_raw else None
                    except:
                        sponsor_id = None

                    # ❗ SKIP nếu BOTH sponsor_id và sponsor_name đều trống
                    if sponsor_id is None and (sponsor_name is None or str(sponsor_name).strip() == ""):
                        continue

                    sponsor_service_ratio = sku.get("sponsor_service_ratio")
                    estimated_sponsor_cos_fee = sku.get("estimated_sponsor_cos_fee")
                    actual_sponsor_cos_fee = sku.get("actual_sponsor_cos_fee")

                    shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
                    estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")
                    actual_shop_ads_commission = sku.get("actual_shop_ads_commission")


                    all_rows.append((
                        int(main_order_id),
                        sponsor_id,
                        sponsor_name,
                        create_time,
                        sponsor_service_ratio,
                        estimated_sponsor_cos_fee,
                        actual_sponsor_cos_fee,
                        shop_ads_commission_ratio,
                        estimated_shop_ads_commission,
                        actual_shop_ads_commission
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
        "sponsor_id",
        "sponsor_name",
        "create_time",
        "sponsor_service_ratio",
        "estimated_sponsor_cos_fee",
        "actual_sponsor_cos_fee",
        "shop_ads_commission_ratio",
        "estimated_shop_ads_commission",
        "actual_shop_ads_commission"
    ])

    # =====================================================
    # FIX DATETIME

    df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

    def to_decimal(x):
        try:
            if pd.isna(x):
                return None
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None


    # =====================================================
    # CONVERT NUMERIC
    # =====================================================

    df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
    df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

    df["sponsor_service_ratio"] = df["sponsor_service_ratio"].apply(to_decimal)
    df["estimated_sponsor_cos_fee"] = df["estimated_sponsor_cos_fee"].apply(to_decimal)
    df["actual_sponsor_cos_fee"] = df["actual_sponsor_cos_fee"].apply(to_decimal)

    df["actual_shop_ads_commission"] = df["actual_shop_ads_commission"].apply(to_decimal)


    # =====================================================
    # DROP DUPLICATE

    df = df.dropna(subset=["main_order_id"])
    # df = df.drop_duplicates(
    #     subset=[
    #         "main_order_id",
    #         "sponsor_id", 
    #         "create_time",
    #         "sponsor_service_ratio",
    #         "estimated_sponsor_cos_fee",
    #         "actual_sponsor_cos_fee",
    #         "shop_ads_commission_ratio",
    #         "estimated_shop_ads_commission",
    #         "actual_shop_ads_commission"
    #     ]
    # )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # # ==============================
    # # DELETE DATA TODAY + YESTERDAY
    # # ==============================
    # delete_query = f"""
    # DELETE FROM `{table_ref}`
    # WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # """

    # client.query(delete_query).result()

    # print("Old data (today + yesterday) deleted.")


    # =====================================================
    # BIGQUERY LOAD
    # =====================================================

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







