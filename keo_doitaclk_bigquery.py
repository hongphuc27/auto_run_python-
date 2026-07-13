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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; FPAU=1.2.1047425659.1783389422; sid_guard_ads=2a17390f694186c6010ee8f8f7a166fe%7C1783389462%7C259192%7CFri%2C+10-Jul-2026+01%3A57%3A34+GMT; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDII-KHBmquvpOqtf4wQg-94KOUzYFOJxKdZ8VOI2LTXmP27Cf89jSzEySM82X0sEEM8l9uY94VagX01oEOwy6ZQ; s_v_web_id=verify_mril6tn5_WtgfYXey_KnGZ_4McI_8XnC_EX7WeqNgqbz1; FPGSID=1.1783908717.1783908717.G-BZBQ2QHQSP.ffBZ97Uu4cRZXM8urZ23kg; _ttp=3GIybokwdkWA4AZ55aXGYvubeoM.tt.1; FPLC=WkOydoM5ZwwvBwn9%2F9Lcfb%2B4ELx274eYTAWf5ASIUF%2BqWkrmuidLesQAKpt0QECumisElA68ou60NwvlMajc0u00KXaiBWW54%2FJbZsIf%2BqZ6n%2Fgj0VbvoM9WdPcItg%3D%3D; msToken=-RnpBFPn6wgMPchAIbwadEL1uki5XorKOaVgFbDen1T4_25Yg2V6KI4cf-XVoX3eFeJKs_FspS-se_SvDAI8RaCijrhwY-sFVkJTxXodePJQ4oS9U0AI0jMEwSKanrt30jtkjLkj4vx68Tg=; ttcsid=1783908716182::nvX7X2tTKxOJPyxPHk0X.36.1783908753608.0::1.-3367.0::37424.17.990.578::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1783908716182::-zYu7bmbZ1aaCgfdeMLg.36.1783908753609.1; sso_uid_tt_ads=bbf12faaaf1501cfb7528d36a6abe3fc1c2fa3d8851136092f324da521c61c05; sso_uid_tt_ss_ads=bbf12faaaf1501cfb7528d36a6abe3fc1c2fa3d8851136092f324da521c61c05; sso_user_ads=24ddfef22d72e7c47126f3442f96069c; sso_user_ss_ads=24ddfef22d72e7c47126f3442f96069c; sid_ucp_sso_v1_ads=1.0.1-KDNiOWVlY2JhNmExOGI2MzlkY2EwNzcyZDYyMDU5ODM1YWVlZDIwOTYKIgiViLWa9-_oimkQlZPR0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDI0ZGRmZWYyMmQ3MmU3YzQ3MTI2ZjM0NDJmOTYwNjljMk4KIOLLPTN7SG2mk1VK07jcemxiAgf43yr5tYHNCDzX-jENEiAJeab-QoPKMW_4yHIWHLTvJzxIyW-gUu-wL27_B2fgDBgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDNiOWVlY2JhNmExOGI2MzlkY2EwNzcyZDYyMDU5ODM1YWVlZDIwOTYKIgiViLWa9-_oimkQlZPR0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDI0ZGRmZWYyMmQ3MmU3YzQ3MTI2ZjM0NDJmOTYwNjljMk4KIOLLPTN7SG2mk1VK07jcemxiAgf43yr5tYHNCDzX-jENEiAJeab-QoPKMW_4yHIWHLTvJzxIyW-gUu-wL27_B2fgDBgEIgZ0aWt0b2s; odin_tt=1c8b576389e830d344ad312d2d7d48a6578ee5dbd6ced912d71f7c2a8d256e76d35eb838e61980dcc20de3bfeb91b042b8794f380f620661fdc755c8ae1abf15; sid_guard_tiktokseller=c00440a6c0fdbbf17dbcc7ab98ae5caf%7C1783908757%7C259200%7CThu%2C+16-Jul-2026+02%3A12%3A37+GMT; uid_tt_tiktokseller=e1e68b24c3d8bb4b7d17e30b5829aee475e941b77c8e0bce3a075aa28a3bb295; uid_tt_ss_tiktokseller=e1e68b24c3d8bb4b7d17e30b5829aee475e941b77c8e0bce3a075aa28a3bb295; sid_tt_tiktokseller=c00440a6c0fdbbf17dbcc7ab98ae5caf; sessionid_tiktokseller=c00440a6c0fdbbf17dbcc7ab98ae5caf; sessionid_ss_tiktokseller=c00440a6c0fdbbf17dbcc7ab98ae5caf; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CwARApsD9u_F9vMermK5cr__________lLhbCRBPDzPEn7BGBUgXcrn05sGOVuVLGdkp_2ShJFao%3D; sid_ucp_v1_tiktokseller=1.0.1-KDU2M2JmZDliYWU2ZjcxZmRkODVlM2M1ZmFiMzUzMDRlYzk1MzM2ZjAKHAiViLWa9-_oimkQlZPR0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjMDA0NDBhNmMwZmRiYmYxN2RiY2M3YWI5OGFlNWNhZjJOCiAh_swJjy0sCpmbejNi83WV2UiCLcK2srLugnaTT2Tx5hIgral6JUMsUwUd6fAHM_ZpmX3DUcQKNvh2EHoCGYRgQ1oYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDU2M2JmZDliYWU2ZjcxZmRkODVlM2M1ZmFiMzUzMDRlYzk1MzM2ZjAKHAiViLWa9-_oimkQlZPR0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjMDA0NDBhNmMwZmRiYmYxN2RiY2M3YWI5OGFlNWNhZjJOCiAh_swJjy0sCpmbejNi83WV2UiCLcK2srLugnaTT2Tx5hIgral6JUMsUwUd6fAHM_ZpmX3DUcQKNvh2EHoCGYRgQ1oYBSIGdGlrdG9r; msToken=EWfucjt3OFTz-TrWPjpcG9HHcjo5Rgcf7oqbqH4XIUcBUwbug58O3Arf94OXPB5yI9eKVaptBo8H6VUEbVbmNwwJDuX-Ozsh1n_ArwH18HqtNb6AsNdWmrM5rU_aypeYzvoTjP_-fI_1V6s=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1783908715$o35$g1$t1783908757$j18$l0$h384035514; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODM5OTUxNjAsIm5iZiI6MTc4MzkwNzc2MH0.Ilcx3LScOoWYgt_6uY-n8iFOKXjscd2-aq7sLoTmTLY; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgzOTk1MTYwLCJuYmYiOjE3ODM5MDc3NjB9.TVneEE9RG7bGZsLA6CYOMO39d5I8NMhzbC1IC4hSE9Q; lang_type=vi; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; oec_lucifer=AQEBAD6XcmbaVlZYPmMkoYyVjJBOCxXKKU6pmbM95LFOnGjGQk4KFRZgzIIanjopHWubrnHM68uv2CSsJ3HX6c94EcD5RmfkRjZR; user_oec_info=0a53a0d6451cbab8bcd119ad456899f6ac32b56905baa3198eabaff9d76a593e97a2ebdd91ea4e6cc9fc95c2fcdea34544c853ea18ca3dc39930878ef68aa32165377c0de3081e2109235324a2105720b41aed94831a490a3c0000000000000000000050a7b3b04f0081b702a67d6282d6fcb87121a62d444098a0f49cfec562f9fa20a7cecb87b4a4f33715d4165f1bc9385e54121091d7960e1886d2f6f20d2201043a32348d; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1783908787%7Cfd7f57b3c66e4dd868b422d9e3d98127d951f6695472edbf9dd26ae6a4668c8b"
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
    start_date = (now_vn - timedelta(days=31)).replace(tzinfo=None)
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
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 31 DAY)
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







