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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU4NzI2MzEsIm5iZiI6MTc3NTc4NTIzMX0.guS1n_ccLfd06Gs2UTXS7J0lLWBCf4sG5gVjLbS_4so; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1ODcyNjMxLCJuYmYiOjE3NzU3ODUyMzF9.8GEGa-3BM5-KJ0SUaD4GvJGmyck3ips81s2x6yiwAlA; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1775786598%7C259200%7CMon%2C+13-Apr-2026+02%3A03%3A18+GMT; tt_session_tlb_tag_ads=sttt%7C5%7CiHhd0_VTVzXYzAY5HVf8hf________-0ghDCbQrwzh7hNCTIT8HFefZid-1VMg7jav4VToF9qic%3D; sid_ucp_v1_ads=1.0.1-KDI1OGM2ZDVkOTk5YjUyOWJjZjlmNDk3YWZiYjNmOGU5ZjE5ZDFlNmMKHAiUiN7g9dSegGkQ5rThzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiBmtBWN1T6nH2a3rGJmeI61TSnNx-3heBnlPOW2eSjSHRIgZzkgqW0soruk2R_Us6wdGvXRDeEmHEu_0hxLpWheS7gYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDI1OGM2ZDVkOTk5YjUyOWJjZjlmNDk3YWZiYjNmOGU5ZjE5ZDFlNmMKHAiUiN7g9dSegGkQ5rThzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiBmtBWN1T6nH2a3rGJmeI61TSnNx-3heBnlPOW2eSjSHRIgZzkgqW0soruk2R_Us6wdGvXRDeEmHEu_0hxLpWheS7gYASIGdGlrdG9r; pre_country=VN; part=stable; tta_attr_id=0.1775786611.7626945287505608722; FPLC=Wz4TDDvnKTkHMzaGm8IOmv60%2B4Zd1G1iPqp3Ma16KwRt9%2FeYUXLpLXbDIc6ISo2QJ6WvWl5niUwPcgvgR2AFfeCTYU4zuNXroZ9rHtdkdwAMpYRlQXyHVg0z465OLg%3D%3D; sso_uid_tt_ads=ff34ea72d144a8ab560b931be2247508692db542b3339b5bab8e6125fc4e0bda; sso_uid_tt_ss_ads=ff34ea72d144a8ab560b931be2247508692db542b3339b5bab8e6125fc4e0bda; sso_user_ads=3d6e2213c7af48d27758e54b562df619; sso_user_ss_ads=3d6e2213c7af48d27758e54b562df619; sid_ucp_sso_v1_ads=1.0.1-KGRkODczZDIwZTFjMzA5NjE2NGQzYzQwYzM3Mzg0MWU2MDhhYjY5ZmMKIgiUiN7g9dSegGkQpMzmzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDNkNmUyMjEzYzdhZjQ4ZDI3NzU4ZTU0YjU2MmRmNjE5Mk4KIAbeAOJHDN_EGkd4zTzKgH61tJigFzFdY82DCX1ISMkaEiDQXJ3UhpMgGzTFOYBgner5_WPBk0Ae60aGQWjA9DFH9xgDIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGRkODczZDIwZTFjMzA5NjE2NGQzYzQwYzM3Mzg0MWU2MDhhYjY5ZmMKIgiUiN7g9dSegGkQpMzmzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDNkNmUyMjEzYzdhZjQ4ZDI3NzU4ZTU0YjU2MmRmNjE5Mk4KIAbeAOJHDN_EGkd4zTzKgH61tJigFzFdY82DCX1ISMkaEiDQXJ3UhpMgGzTFOYBgner5_WPBk0Ae60aGQWjA9DFH9xgDIgZ0aWt0b2s; uid_tt_tiktokseller=a6cd6e85d6db6a6d9851ce2d7c09a3ed019b36657d01dcf47f0b135a281e246d; uid_tt_ss_tiktokseller=a6cd6e85d6db6a6d9851ce2d7c09a3ed019b36657d01dcf47f0b135a281e246d; sid_tt_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; sessionid_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; sessionid_ss_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; _m4b_theme_=new; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1775871565%7C0afebe9621fb5cce3f524cdd61a9053501f52a6241d7911158119f542a9de808; sid_guard_tiktokseller=9f3c144d072747e9ff4b1d209c97b044%7C1775871565%7C259159%7CTue%2C+14-Apr-2026+01%3A38%3A44+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CnzwUTQcnR-n_Sx0gnJewRP_________UA5xemqo1VyX9R0Fw-ov6eOL-gpNMGhv6ZBE52iJ0990%3D; sid_ucp_v1_tiktokseller=1.0.1-KDA3YTBiZjg4MWQ2N2QzMGI2ZjZlNjk5ODlmOTdiNzNhNWEwZGE4NjcKHAiUiN7g9dSegGkQzczmzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDlmM2MxNDRkMDcyNzQ3ZTlmZjRiMWQyMDljOTdiMDQ0Mk4KIF2DvHw_WX0-05Js_DughCVPPmUJZpDLN7BdfDOyXeLdEiABzK1-Fi1jMXFzZRzbUZ3KjojDqL-7LPBeHlgjukZdzRgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDA3YTBiZjg4MWQ2N2QzMGI2ZjZlNjk5ODlmOTdiNzNhNWEwZGE4NjcKHAiUiN7g9dSegGkQzczmzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDlmM2MxNDRkMDcyNzQ3ZTlmZjRiMWQyMDljOTdiMDQ0Mk4KIF2DvHw_WX0-05Js_DughCVPPmUJZpDLN7BdfDOyXeLdEiABzK1-Fi1jMXFzZRzbUZ3KjojDqL-7LPBeHlgjukZdzRgCIgZ0aWt0b2s; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; _ga_BZBQ2QHQSP=GS2.1.s1775871502$o2$g1$t1775871567$j60$l0$h921084537; FPGSID=1.1775871503.1775871568.G-BZBQ2QHQSP.ST3UVTTN6_dR7udmKsDfOg; ttcsid=1775871502961::U9BaWkt_NXTozqvztRrG.2.1775871568420.0::1.60090.65120::19208.13.994.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1775871502961::3lotr2RjRIb_eiNajBJf.2.1775871568420.1; msToken=x91LjIV1HGyOPHLKcytrXLqETsCA7MIWh4T-Eauks6md32XHAo2meDfPp2TcZNF07rtzRwUE9LFeDeBeIePo0uHS3oS-_yw24VfVNpsX39soOtrn0PH36M37TkBNwBjHHKkuDmY=; user_oec_info=0a53ad3ae2408fa42d0ff45b6894780ae2639cf74b72181a6d3e063dcdb9c1bc1f7144a33b6e00c38b80c5a708573089f1a37905a1e3f72beec7f013264adc567ba8be92d7fd28972715355bf52a7e8b2eb6b749c11a490a3c0000000000000000000050495766e10ebdc606b4ab0e3b1aa81f34025a624831f593d2906cde18a029893b8195aa6314b6d5fc8704e611db49fdeb9110a9bf8e0e1886d2f6f20d22010465d1a3ad; odin_tt=ce2c90ac37c8fbca731a12e6d06ada055ee88292ac80118d2b0614fba9bc821f555d7dbcb90392c525e55a5ee854a31f0d0e8fef05ad2961fdc6eae2eab5896d; msToken=IF_cqjTM-DFrNcHNxKMnZmUH9TUcqwQakXgkaMbc6BGcwA9ZPc3xQDldWDj5sqBjgasr-Dj93D4S8BFAgCLQn65lY7-26sGFI46mlCamdGGO2useQ_7fUU3j9QPK9vhKTjBaoEU="
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







