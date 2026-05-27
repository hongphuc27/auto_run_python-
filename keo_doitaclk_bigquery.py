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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; _m4b_theme_=new; oec_lucifer=AQEBAIW4e0LzJkLGlwXM6lVdaNVAdWgSww2E30v6v2GyC7J6hoItGN42fAPQNeqsqyjlBwyF34c/J0OrhSBpaOFmBuLpod1VLQ==; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde; d_ticket_ads=40b4f7f55431d4013801acad214d89a237e1d; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; tt_csrf_token=VHfpbhEh-zcmko6bmHhp_kaSiBS3VkE30JBI; store-country-sign=MEIEDEWmOX3IoDPDpTc13AQgUcZ_xnEheu3me-dJx0P9j5yrcQSYxhX1krnz4HPQUOkEEO70gswdtauJtA_HrVjKIak; s_v_web_id=verify_mpnjzi4w_74uIygEN_avRV_4ujB_AdE0_aEg59zmLQAJM; FPGSID=1.1779865181.1779865181.G-BZBQ2QHQSP.qPoWlMfG-pc_PafI9KMi7Q; _ttp=3EDQ0yHbwWwp9JfDz49lS7Y9nTV.tt.1; FPLC=DmjoQ4jub7EjjxKrCP9cnJ69VKn%2B8bnVxSWHsnBaI%2BaDTyQOOL6LHJpU0q9iXjMVmqGQNuNcfRDl93NR1JYo1JN%2BIVlEi8iB3fhEiee1jzeXwFyq6PnkW3aqX%2Fm%2BmA%3D%3D; ttcsid=1779865181975::TxJD2SY6LjzUQWyPZjxn.19.1779865203907.0::1.-2154.0::21929.16.1006.568::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1779865181975::Z32D4dG0_j7NOWX8JTDb.19.1779865203907.1; sso_uid_tt_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sso_uid_tt_ss_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sso_user_ads=e4b6187eabde1fe83f9a57de8e97b848; sso_user_ss_ads=e4b6187eabde1fe83f9a57de8e97b848; sid_ucp_sso_v1_ads=1.0.1-KGYzMzcyOGI2ZTg5NGE4YjljMWNiYzdmZGZmZWYwMjQ4OWQ4Y2RhY2IKIgiUiN7g9dSegGkQ9aza0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGU0YjYxODdlYWJkZTFmZTgzZjlhNTdkZThlOTdiODQ4Mk4KIIXfRVVwHaOFJl-48fRJhmpok515iFQyk9V06OjhpGoDEiAFNRES-rvjBwewgrRQ5m_yZni22FvfU0V3YTtOogtbChgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGYzMzcyOGI2ZTg5NGE4YjljMWNiYzdmZGZmZWYwMjQ4OWQ4Y2RhY2IKIgiUiN7g9dSegGkQ9aza0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGU0YjYxODdlYWJkZTFmZTgzZjlhNTdkZThlOTdiODQ4Mk4KIIXfRVVwHaOFJl-48fRJhmpok515iFQyk9V06OjhpGoDEiAFNRES-rvjBwewgrRQ5m_yZni22FvfU0V3YTtOogtbChgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1779865181$o19$g1$t1779865206$j35$l0$h1702510088; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; uid_tt_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; uid_tt_ss_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sid_tt_ads=e4b6187eabde1fe83f9a57de8e97b848; sessionid_ads=e4b6187eabde1fe83f9a57de8e97b848; sessionid_ss_ads=e4b6187eabde1fe83f9a57de8e97b848; tt_session_tlb_tag_ads=sttt%7C5%7C5LYYfqveH-g_mlfejpe4SP_________N5DwoCP13gdDcwROEaYCzYsa6Qtos4xdys8zlmwPJHKM%3D; sid_guard_tiktokseller=93f59619b994cc6b2a3b49247b4f3346%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; uid_tt_tiktokseller=68ffb5a326efe48a8019903eaaa8123d793c2d0a4db43462253bf7b016eaf638; uid_tt_ss_tiktokseller=68ffb5a326efe48a8019903eaaa8123d793c2d0a4db43462253bf7b016eaf638; sid_tt_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; sessionid_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; sessionid_ss_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; tt_session_tlb_tag_tiktokseller=sttt%7C2%7Ck_WWGbmUzGsqO0kke08zRv_________s-74QcVV1qADkPQQm9rMOwD_rHO9WlVqeOiaDdxAvh60%3D; sid_ucp_v1_tiktokseller=1.0.1-KGJiOTNjNmJlZTQ5NjdkNjYzMDE2YWU0OGI2MmRiMzQ4ODc1NTJkZmMKHAiUiN7g9dSegGkQ9qza0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA5M2Y1OTYxOWI5OTRjYzZiMmEzYjQ5MjQ3YjRmMzM0NjJOCiCGfj7beYuCkb-hg8Yz5zGjYO8XgxmJq_kjaxNdbi8zORIg2ZRA5LfZVGzRbpStT2E3qKdY0Lv_XKm1T3-B_B5jOt0YBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGJiOTNjNmJlZTQ5NjdkNjYzMDE2YWU0OGI2MmRiMzQ4ODc1NTJkZmMKHAiUiN7g9dSegGkQ9qza0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA5M2Y1OTYxOWI5OTRjYzZiMmEzYjQ5MjQ3YjRmMzM0NjJOCiCGfj7beYuCkb-hg8Yz5zGjYO8XgxmJq_kjaxNdbi8zORIg2ZRA5LfZVGzRbpStT2E3qKdY0Lv_XKm1T3-B_B5jOt0YBCIGdGlrdG9r; msToken=UYvwSlbFX_H33D8FC_osJXIxZ2PJMQ4hRHMSTsAm9vxoIrDyrP-HIcZVfyiCrRw7abIuWNJPUojZHHKZG_Vro1a4YZ9EbD4lzx_V2nGeJ73es5wgFu-WpgPjdcAKS5aFrnFUuKI=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1779865207%7C4a8e48c1b193bdddcbaa1c1bfe2cd5bc9031a6342abf8b3fcabc104ecd6a44dd; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzk5NTE2MDcsIm5iZiI6MTc3OTg2NDIwN30.rNYucBq8czRJJLPVGuBQyr92G56yFj1RS1dMRw8B1Pc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc5OTUxNjA3LCJuYmYiOjE3Nzk4NjQyMDd9.UxLrxcmb-MIzs0InVbdNF0sH1N7Unyz4STICaWJy4uE; msToken=IZwyNO9jWsCJ6S5iSPGw3WU9Al7dcF5dujaAoSWOgH82_0csAC4aDCqtl555H0pCC_fXdY3NzXjdKPEGMZSfefN87tAl1O72EoiKSD7Wf1BdBDpXtXC1iYJ3kafNdmx5f7pm6O4=; odin_tt=ae3c2b9573b31a6a6debdbc245b3eb6964c9eb29f8e82b44847a86b13aced3d7eeff36627288780e24b39b3eb00f591882df69df4cf741572a6aa9947e06cd94; user_oec_info=0a5303d5bacac5fe1263f3082a4edc3522210d88c7595c6b36b92163ebbdfdd9f10ff8a18e8cde3a4d4bd572e2e6aec4c0f7e983bdfade2cb955d10b01ab0a2d7450e98a34e7ce12807c79c1d5af1971ff9b95c0351a490a3c000000000000000000005078d1d3b5ad398dfbea793a4116acbcf991bfe3f2c47439ca3f21e664e96b8393f3bae9fdba4e7aa5b40817fd463595597e10a7c7920e1886d2f6f20d220104bf62e9c8"
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
    start_date = (now_vn - timedelta(days=26)).replace(tzinfo=None)
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
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 26 DAY)
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







