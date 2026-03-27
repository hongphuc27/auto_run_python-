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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; app_id_unified_seller_env=4068; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774435900719::0fHO-vrCqBgVRbGDlbk1.5.1774436089399.1; _ga_HV1FL86553=GS2.1.s1774435922$o1$g1$t1774436092$j60$l0$h1896886948; _ga_Y2RSHPPW88=GS2.1.s1774435900$o6$g1$t1774436092$j60$l0$h1953927926; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; ttcsid=1774453289316::NF5U3uXHmdfcwUGZ_u1d.17.1774453592016.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; d_ticket_ads=cb73b14fab6aad9b8691e11bc72347f87856e; sso_uid_tt_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_uid_tt_ss_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_user_ads=9217a37eb4efc556ee17f0cf0366319c; sso_user_ss_ads=9217a37eb4efc556ee17f0cf0366319c; sid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774453594%7C259199%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CNl44rHhyf0iNxN90xYbMZ_________-_HNHIjdbW97XZvKbwDEBA_EjSBeseaOvDCI7tcnCGdfk%3D; sid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; sid_guard_ads=ee8cec8c67ab555af96a102ed407e9df%7C1774493563%7C219230%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_ads=286561c0e6be80e0c24bc365ea55921968182b72a86f809f65173281faabb454; uid_tt_ss_ads=286561c0e6be80e0c24bc365ea55921968182b72a86f809f65173281faabb454; sid_tt_ads=ee8cec8c67ab555af96a102ed407e9df; sessionid_ads=ee8cec8c67ab555af96a102ed407e9df; sessionid_ss_ads=ee8cec8c67ab555af96a102ed407e9df; tt_session_tlb_tag_ads=sttt%7C4%7C7ozsjGerVVr5ahAu1Afp3__________QHs122uATT314lLQrv_LbSNOu6GrPg8FvR6FfPfvUIv4%3D; sid_ucp_v1_ads=1.0.1-KGVhYzhhZjhkZDNmOTIzZmU0Y2ZkNWJiZjBiZTQwNmQ4NmVlNjhkYWEKHAiUiN7g9dSegGkQ-76SzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiBlZThjZWM4YzY3YWI1NTVhZjk2YTEwMmVkNDA3ZTlkZjJOCiDuOA2C1xsNbwwEOT1RbcFcgtumLuaCmj2EnXSJ-0RtpBIgSCkOXq1tMpio6mE6whVtIcuBrmtE910igoQ9C3rATXwYAyIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGVhYzhhZjhkZDNmOTIzZmU0Y2ZkNWJiZjBiZTQwNmQ4NmVlNjhkYWEKHAiUiN7g9dSegGkQ-76SzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiBlZThjZWM4YzY3YWI1NTVhZjk2YTEwMmVkNDA3ZTlkZjJOCiDuOA2C1xsNbwwEOT1RbcFcgtumLuaCmj2EnXSJ-0RtpBIgSCkOXq1tMpio6mE6whVtIcuBrmtE910igoQ9C3rATXwYAyIGdGlrdG9r; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ2OTMxNDEsIm5iZiI6MTc3NDYwNTc0MX0.NErhJnrvxPbOTjCACDH2RIr6syO5IWMkvQJ-cIxkwRc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NjkzMTQxLCJuYmYiOjE3NzQ2MDU3NDF9.N-tRMC6HiOxyej9_4jDje-B6hbuTneQsD1xbRRJZ3_A; msToken=DxzYYkIk_fqi7hrgH3R_XSskFza15XL-bhwi4XSyVlh_XlkMtJ1ATDz8Wz-rhn2F_CAjcT-gZ2tlwU26kckc4aJqHdKmdWAmjx6ipNPZsPuTDiMOt4FwdiSGVKnq; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774606767%7C1b3ce158109dafb2a0228e0c809dde217b3c7dcf3d6d2147dc45747599cc2a6d; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=e30971bfc02e0f3cd1a6e8f3881d537cb54620b903d9fc3dc91c527f5cb40d3fb5629d8ae89639795684eaa96f10b515a9e4c7c37dc599a9ce2d30af533154db; user_oec_info=0a53ce1b9e5aac5b19af16084dfefc609869b8fd859026f8de4f657d996cde009d5daa462247c53057b498bdaaac5d2b6458294ffd4219fd72be5aac17127a8dc5cbc29268a34a0a8d5d3051179425698b8911a9711a490a3c00000000000000000000503b2d531acf93088290310bf67cdb3fad4184e8b38301cf846972659ac79f2b913795ff2ed8ef83abd5044eca6011829b3010e89b8d0e1886d2f6f20d22010469d39a73; msToken=CMBSGIEAN-sPE3QlHtvDwalkEZz-mYNXxVo8r8bBzG_zS3uEqN-KVoG43y1TluGupll_5lu2tRBld2IExQF_kj4IUqgVwKsEe4M5GpZXhPCH7ThSLNjoy80RF9No1OXrd20Vr5c="
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







