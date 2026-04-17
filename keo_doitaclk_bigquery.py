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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; _m4b_theme_=new; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; tt_session_tlb_tag_ads=sttt%7C1%7CiHhd0_VTVzXYzAY5HVf8hf_________45GTTALo6OYZqErB1AFXnVjWyc9dXNVElF47eTkp0sDg%3D; sid_ucp_v1_ads=1.0.1-KDMwMTEwZWYwZDFlZmNjNGFhM2I4MTVkNWRjMzQ1MDU3NjRmZDRmZTUKHAiUiN7g9dSegGkQhJ34zgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiAgDseYWQI_-j8yu4HJk0ZkRg3ajKNINuET7HfTNESxZxIgjUAlOVzabKi_Svt7xhNCvUUkkvMg8kmgmpjMFyf0I20YBCIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDMwMTEwZWYwZDFlZmNjNGFhM2I4MTVkNWRjMzQ1MDU3NjRmZDRmZTUKHAiUiN7g9dSegGkQhJ34zgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiAgDseYWQI_-j8yu4HJk0ZkRg3ajKNINuET7HfTNESxZxIgjUAlOVzabKi_Svt7xhNCvUUkkvMg8kmgmpjMFyf0I20YBCIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzY0MDg0MDQsIm5iZiI6MTc3NjMyMTAwNH0.kZ_hOSqKniJlUZ092R2F6p3rRx7D-HpSRyO89YbA1yY; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2NDA4NDA0LCJuYmYiOjE3NzYzMjEwMDR9.tmKxmNx1Ny1nWYBlcEmQijUmHvZmDO6zk794VQPTFC4; _ttp=3CKo60ZAkHMPGpMXKFtLeXc4Fuy.tt.1; FPLC=DuqWqk90NZ9OtRB%2By5CgPlSWr2m1Mu30HC65%2Fpegc1aNONJ5BnyKcODoe4saulw0etXOabM07NIBblMvlImQHSgIHxUNnqOC6C7dtGD3ptsBKOO9y2CfiShj0kvi8w%3D%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1776396362719::btvc1oDwuuCD4SHmMMg3.4.1776396421402.1; ttcsid=1776396362720::eCO-JscRKJ1amZpKTiMW.4.1776396421402.0::1.44961.46765::58687.22.846.390::0.0.0; sso_uid_tt_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_uid_tt_ss_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_user_ads=454dba0da10f723807a2be83b8845e3c; sso_user_ss_ads=454dba0da10f723807a2be83b8845e3c; sid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; FPGSID=1.1776396362.1776396422.G-BZBQ2QHQSP.GLty-FoA2J5jqh_XJDjbQw; sid_guard_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913%7C1776396422%7C259200%7CMon%2C+20-Apr-2026+03%3A27%3A02+GMT; uid_tt_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; uid_tt_ss_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; sid_tt_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_ss_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C0K6SqcXt_t_VFpZqfP3JE__________cW5jmUlATYZWcde9YMLX3C86hQU8NH-ScWt71tyTLaXg%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776396362$o4$g1$t1776396423$j59$l0$h690599615; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776396672%7Cfa4ba8ed47e7169938893bff90cb4b7ddd98325ea5c2c4fdcd34465a17e2d372; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; msToken=RAVgsQOtd5FMe185pqmor9LbqWo2Hx6KzM3LPELaTjtDlAszcZ_gIrKzuUghM1_dJlAvzTUB3NJO_cSYYFTcsHQ0o6C8vSZbZ2bn9H83UQJY2Lr76M6LwbN5hUnyrT6Gy_0SU6ktMQ==; odin_tt=fe34121db5d0b5a3145350916fa9cfdc33fbe953fde89f46f9df1fdd7422f596df33e0b650828c9068de44235378a01b645a085e7117c3f8a1a0df344de477d5; user_oec_info=0a53ecce894e460a670f3ee9223c3216c17d0352cac180cdd4bdca8910f21983d9c8dcc032b347b1a61c3fc157b012ad3c9ed0744703eb31f3af1b8d2a827117d5ec99e1627825442ff428d9091b33a3d67a27275f1a490a3c000000000000000000005050bc9b3b9675d55939b744773eaab1c174b55f027fb12ce786088a6a4143e9c6f12c970eea0bb16ad2dd2da5882b95c61d10aa848f0e1886d2f6f20d22010472dd42ab; msToken=zFVdyI7x2tCH_1GorOoy_zw4J6IHSyeG3ZwaKD2UT6TWI_Er_y6RykeSSiy0Y5IBYeX-cymy3nfN1UqE7FgOMGIF-V68aVK-kWcdE_dt_wr6eflVpOXS_riYXLEBD1w8B7x5kaQ="
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
    
    # ==============================
    # DELETE DATA TODAY + YESTERDAY
    # ==============================
    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 DAY)
    """

    client.query(delete_query).result()

    print("Old data (today + yesterday) deleted.")


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







