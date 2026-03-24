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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; sid_guard_ads=3847856ccedf5c11a59bc9d9ef5b272a%7C1773990826%7C185179%7CSun%2C+22-Mar-2026+10%3A40%3A05+GMT; _ga_HV1FL86553=GS2.1.s1773990828$o6$g1$t1773990828$j60$l0$h1985046469; _ga_Y2RSHPPW88=GS2.1.s1773990828$o5$g1$t1773990828$j60$l0$h1334679047; ttcsid_C97F14JC77U63IDI7U40=1773990829246::_nFQcqJTBcfmOF3E39x_.4.1773990829644.0; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0MDgyMDQsIm5iZiI6MTc3NDMyMDgwNH0.Ni3ylC1N93XB_6cC1yvjZ6hav86bMjM1eb2CmQe-YBY; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDA4MjA0LCJuYmYiOjE3NzQzMjA4MDR9.nH0dlZnU1VzI820IllswWL71as-I7odm_2wGOkpRim0; FPLC=xW%2BDoGjXAUETgRL4EvSRGsLe%2FivaqHtlUHUXjste25I4lwEC5PZ4OxOdRA2i9MEnLoXZ7CUik5WWKs81x%2F0Lh6UUoYNvSmPgJNTviYErWq%2BiNogtoPl9ya0spk8pag%3D%3D; ttcsid=1774328558618::vo01iITGmxLRpspmTycH.15.1774328581572.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774328558618::yaGX_UIJG4xKpNNcedlG.12.1774328581572.1; sso_uid_tt_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_uid_tt_ss_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_user_ads=28923db567af453c44258468669f6c64; sso_user_ss_ads=28923db567af453c44258468669f6c64; sid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774328558$o13$g1$t1774328584$j34$l0$h182365910; sid_guard_tiktokseller=89fc588555d8994266248c8b5c2794f7%7C1774328583%7C259200%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; uid_tt_ss_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; sid_tt_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_ss_tiktokseller=89fc588555d8994266248c8b5c2794f7; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CifxYhVXYmUJmJIyLXCeU9__________6r4d-Iyrp0mENXHEHHfoMCN3BQVGhnLrugOXdiVOSilI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; msToken=ChNONKAOs7uWOy0HnbY6FDHIh4-43xPDuioMzhzsvNWCG9y8vsfO18nKEVRdHVkhcP2xlHmP9eDcl5JMPd6P2aJkaF3UFp6hBY3cWBWvDQaHrA14JbwaIO9bD_TAU1qfs2OrkQE=; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774328634%7C1fbb02ec573f5b1256437a02213a3db6d19046dd1280d0d3f8dbb3afd489add5; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=622529418d67fee79f6a264fed7bff4fe12073684ea772ec922a95ffed07c8c34ca3a899d7df8786b359e057db737618e99ddb92c87895367e3557013a7872d7; user_oec_info=0a533f4efd260da5ea299fc7b10f2c80398fcb22c75673fb6661c275db576f42d9aa4957854ffc80077b6df319ce9214f460c51ba4b098dd71f4457af8748df5eb67187385222cb09ecc105af9deaa01952ae32f671a490a3c00000000000000000000503889cce5103bd02bedf8fafc1e8af5bf35b8513edd56f546dc7988b3d4f127dcbc9f1814a286311cda52dd942a2fadd2a410ddf78c0e1886d2f6f20d220104e42413aa; msToken=h2tIDhjvRypCdEFH45-wwh4i4d2fDBXk5y5i8hmlQGpRQXGEv7grvaXnetp4HZmglWMbgUiBPuq20aJ0_8EtFZ8qZAuysNpkhbNz1Armsyx8OijjDvk1UBajaQm2ggC6h3IBBU0="""
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







