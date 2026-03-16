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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _ttp=3AvIaixPN8VfHyK5OTkTyJrsKKD.tt.1; FPLC=B1ex7kmQeCSbgn%2FX0StrkmjiK8CWdKY7U0bvX13B3eVwcKLuvaSO7vy0ZH%2FZH8JCFxQntGj8fzJcnz0dM7MBIOGCxRa%2FrK7lGGuXpO8sGWKmOzvckEQfgxHPRyn9HA%3D%3D; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzM3MjAyNTksIm5iZiI6MTc3MzYzMjg1OX0.4NAcmKhWT29DQ_pMaOZhABiF7KWxpUsTVIweWMA4Oj8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzczNzIwMjYwLCJuYmYiOjE3NzM2MzI4NjB9.0yz6hFT0L5fUfIWoCrnt43FALY67H1TZLDUBIWYPo5w; _m4b_theme_=new; part=stable; ac_csrftoken=4e293a58ec4248ee90c06e156a186337; ttcsid_C97F14JC77U63IDI7U40=1773635979896::IHQwRRA_YETJxxAS0PmA.3.1773636022342.1; sid_guard_ads=e04a3f7caf24b5e89be320a0f121c4c1%7C1773636074%7C259200%7CThu%2C+19-Mar-2026+04%3A41%3A14+GMT; uid_tt_ads=af72cc7ff1263f8118268d9c989bd65328cea559970c4189c78145c45398273b; uid_tt_ss_ads=af72cc7ff1263f8118268d9c989bd65328cea559970c4189c78145c45398273b; sid_tt_ads=e04a3f7caf24b5e89be320a0f121c4c1; sessionid_ads=e04a3f7caf24b5e89be320a0f121c4c1; sessionid_ss_ads=e04a3f7caf24b5e89be320a0f121c4c1; sid_ucp_v1_ads=1.0.1-KGQxMTg4MzFmZmY2MmIyZTYzZDg5ZmZhMTI4MzZiNjc4ZmFiZWJiOWMKHAiQiKz8zcvg22kQ6pPezQYYrwwgDDgBQOsHSAQQAxoCbXkiIGUwNGEzZjdjYWYyNGI1ZTg5YmUzMjBhMGYxMjFjNGMxMk4KIDSCgQFxeor2EFw9EIiH4TivRFC0zDEn6mumsnaTBB7DEiDj4BEEJCXRb4BiPEk2q5d3cqpz5-EWqzz9CasXjrz8chgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KGQxMTg4MzFmZmY2MmIyZTYzZDg5ZmZhMTI4MzZiNjc4ZmFiZWJiOWMKHAiQiKz8zcvg22kQ6pPezQYYrwwgDDgBQOsHSAQQAxoCbXkiIGUwNGEzZjdjYWYyNGI1ZTg5YmUzMjBhMGYxMjFjNGMxMk4KIDSCgQFxeor2EFw9EIiH4TivRFC0zDEn6mumsnaTBB7DEiDj4BEEJCXRb4BiPEk2q5d3cqpz5-EWqzz9CasXjrz8chgEIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h1171674335; _ga_Y2RSHPPW88=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h640968867; pre_country=VN; tta_attr_id=0.1773636090.7617708817361666066; s_v_web_id=verify_mmsxd86n_ueWdkG95_ku5N_4QQH_9fEV_tL5Z9CnFwo5m; tt_session_tlb_tag_ads=sttt%7C5%7Cq04bIGogHNq-BZBhKsspNv________-gO3xCsRydn5k8XnQLVCKqRbSmLFoPvXHPrPqfJNQjWD0%3D; tt_ticket_guard_web_domain=2; app_id_unified_seller_env=4068; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; d_ticket_ads=13251634588e056625fdb16f92053bdf7856e; sso_uid_tt_ads=dac0fab2ffd0377f9ee99824c9fea7e7e34d052431421f08c3c81d2b54b4e500; sso_uid_tt_ss_ads=dac0fab2ffd0377f9ee99824c9fea7e7e34d052431421f08c3c81d2b54b4e500; sso_user_ads=78e27ddaff17e02961fbff0fa941e235; sso_user_ss_ads=78e27ddaff17e02961fbff0fa941e235; sid_ucp_sso_v1_ads=1.0.1-KGJkZDRiYTE5MmI0MDFmZmUwYzUzNjY3YzRmMmI2NDM4M2VjNTA4NjgKIgiUiN7g9dSegGkQ2IDfzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc4ZTI3ZGRhZmYxN2UwMjk2MWZiZmYwZmE5NDFlMjM1Mk4KIJaaEU4-XXfe4BZMG3K9889WdYhUvyhKTH10HvrcaGLXEiDG0iz3AxMzisWISYunUa-TiwPqHOLti6StrW3KB0TXWBgCIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGJkZDRiYTE5MmI0MDFmZmUwYzUzNjY3YzRmMmI2NDM4M2VjNTA4NjgKIgiUiN7g9dSegGkQ2IDfzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc4ZTI3ZGRhZmYxN2UwMjk2MWZiZmYwZmE5NDFlMjM1Mk4KIJaaEU4-XXfe4BZMG3K9889WdYhUvyhKTH10HvrcaGLXEiDG0iz3AxMzisWISYunUa-TiwPqHOLti6StrW3KB0TXWBgCIgZ0aWt0b2s; uid_tt_tiktokseller=2a257248ffeddbffb18d0571f662584c95c229d829eaa9bd417d02dacdaf5508; uid_tt_ss_tiktokseller=2a257248ffeddbffb18d0571f662584c95c229d829eaa9bd417d02dacdaf5508; sid_tt_tiktokseller=b1a808d4548d99acd66052430df2a272; sessionid_tiktokseller=b1a808d4548d99acd66052430df2a272; sessionid_ss_tiktokseller=b1a808d4548d99acd66052430df2a272; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773650066%7Cd6bb01a28188ac8aa2e82a1e28357c4ba37ce4452bc98f847dd4c1ed3d7a8231; sid_guard_tiktokseller=b1a808d4548d99acd66052430df2a272%7C1773650066%7C259142%7CThu%2C+19-Mar-2026+08%3A33%3A28+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CsagI1FSNmazWYFJDDfKicv_________jsO5tnyFhLGGwlDw1EZmAskdLjfS24kcpz1j8XN20xrc%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhkYmZlNzE0NWZkMGE1NTBjZjQ0YWEzZjdiZWRiODYxZWNjNzhlM2IKHAiUiN7g9dSegGkQkoHfzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGIxYTgwOGQ0NTQ4ZDk5YWNkNjYwNTI0MzBkZjJhMjcyMk4KIO-IJ6VQ_K0r_6dDh6RWAXIvPEbCrgp85AbxtDg7A7P1EiAYypS2pXL4loFZ9vIezyja2X-1o2gF9SzA0ALeF6yogxgDIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDhkYmZlNzE0NWZkMGE1NTBjZjQ0YWEzZjdiZWRiODYxZWNjNzhlM2IKHAiUiN7g9dSegGkQkoHfzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGIxYTgwOGQ0NTQ4ZDk5YWNkNjYwNTI0MzBkZjJhMjcyMk4KIO-IJ6VQ_K0r_6dDh6RWAXIvPEbCrgp85AbxtDg7A7P1EiAYypS2pXL4loFZ9vIezyja2X-1o2gF9SzA0ALeF6yogxgDIgZ0aWt0b2s; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=6eb64a02bcca55c779113f4b6a0f67bc4bee8afccdf35c6b13fa9c5455e43fac7b09df356a882ce0aff09bab65574d731c50ea224e3907cf27de4d2e23b1a10a; ttcsid=1773649947134::TXSB-fcfK0rLfbFcEgbR.9.1773650071228.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773649947134::plDTzXDLEuQ5yqohxBsB.6.1773650071228.1; FPGSID=1.1773649963.1773650071.G-BZBQ2QHQSP.FT983zZW9WHUGuGPv8V4XA; msToken=yRpYhibYqHLNWsEhDSwNpnayg3QaD9o6szWXgeXXfH4rIb-NtiG5K8Zu5gytjHhDJ8zueRDi72DP42AEnZR_jKicMLRIR8FCOKlHjFrZoKJ2u9esNSKsygJz0IgmzA==; _ga_BZBQ2QHQSP=GS2.1.s1773649946$o6$g1$t1773650085$j45$l0$h197460113; msToken=1_Yg0qGoGlzrS1MwJZVfHv0N3boVcIwUUg9UOja0_arIeJeFjH14banMjcnZmLwcmdV4fXw93U6Jbo3rA3SuHtEUfEfOJfcTOYRZhEN-zL2W-ChZJa9axJ2zDhGHIsE_Ax4LUZU=; user_oec_info=0a531ac2da10f067d1c539860b0ddab13b4239273ce80934b64bccb707cb8d6a71c872eeceb69f1283f68bf678a690e9257d2c854c709e46974b3a08a9cf73484d21a617b26d311e515d4a1eeaec83709d3d2ae4e71a490a3c0000000000000000000050305675498183b8e9edc60b53185463312e27fabd3748c32bad112cc09f7d97682c5dbb638c7df84502b43b452b930e77c210999f8c0e1886d2f6f20d22010490f1d66c; i18next=en"""
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







