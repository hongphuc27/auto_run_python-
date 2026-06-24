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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-sign=MEIEDLcaVFMNPJn5xf6ZMAQg-A-jYgq_pCZx4CR3_zfQNgWDydl8nod6lsQ_GJyoEIQEELXym9Z8EbwX5_FKsT5XGYk; store-country-code=vn; store-country-code-src=uid; _m4b_theme_=new; gs_seller_type_for_report=pop; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODIzMDU1NDYsIm5iZiI6MTc4MjIxODE0Nn0.gvo_kedzSRxjs8o0Pncd85g2J3L_B9ofHAz4VbL8jW0; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgyMzA1NTQ3LCJuYmYiOjE3ODIyMTgxNDd9.vmgIFAzRS_d0_S63pOO9qt_R32wjelHYMFX4DB9OeaU; s_v_web_id=verify_mqrefr2s_W0sqnvou_5gso_4Z1L_98ZW_5X1Wh31deXu4; _ttp=3FI4kgAM8T5pBpXv3gTZw41YKig.tt.1; FPGSID=1.1782264794.1782264794.G-BZBQ2QHQSP.6SQTQTbDwe1nu06Pfrc8dQ; FPLC=IzyAGyZx%2BJPOunH2LZkyM2g%2Fl7RQ7qvymHLaFkwM2x9KdS1TNRcaUHQMYTWRlJWWnldANxhAQPsWaGBGLlUF8L9PGAlpgs8svRLZfMmKr1obWVdvNuO9VKK8VEL1xg%3D%3D; msToken=8Z4cP8R45FclsQhd-zppNEghUeH5kf87GxtzxqHMDJ9XN54WWkkHhzyKhwdrXdAjdMqz2gxDHWyTwpQeCS_3YybOUvZHdLcackQOWPdQFKUCtNJ3hGtdQpB-aqVy_P1uSzm_CFg=; ttcsid=1782264790752::Dvi3INtL6uPTaa3vx-n7.29.1782264813643.0::1.-5338.0::22889.10.943.571::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1782264790752::ekviyJibt1tWvf-o0MFH.29.1782264813643.1; sso_uid_tt_ads=c149119b0cd04a094e57b0c6cefd03de1117e51b3f8b79c93e2d36436a783299; sso_uid_tt_ss_ads=c149119b0cd04a094e57b0c6cefd03de1117e51b3f8b79c93e2d36436a783299; sso_user_ads=16ee3ad9431b78a549bc0fdec4ae150b; sso_user_ss_ads=16ee3ad9431b78a549bc0fdec4ae150b; sid_ucp_sso_v1_ads=1.0.1-KDUzZjg5Zjg2MTNlMjU1NmMzNDc1MzE0NGY5N2JlZWJiZDg2NzgxN2YKIgiViLWa9-_oimkQ9efs0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAxNmVlM2FkOTQzMWI3OGE1NDliYzBmZGVjNGFlMTUwYjJOCiBYKP8-NXq0AdfwT_7RlZPxlE996mTrpYNEbI_zyT8gRxIgfv2grXZ2BKrR36Jpm94jcqrCoKlAKPveIqHZCwqpTbwYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDUzZjg5Zjg2MTNlMjU1NmMzNDc1MzE0NGY5N2JlZWJiZDg2NzgxN2YKIgiViLWa9-_oimkQ9efs0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAxNmVlM2FkOTQzMWI3OGE1NDliYzBmZGVjNGFlMTUwYjJOCiBYKP8-NXq0AdfwT_7RlZPxlE996mTrpYNEbI_zyT8gRxIgfv2grXZ2BKrR36Jpm94jcqrCoKlAKPveIqHZCwqpTbwYASIGdGlrdG9r; sid_guard_tiktokseller=800f155a447c44f3880daba446359b17%7C1782264821%7C259200%7CSat%2C+27-Jun-2026+01%3A33%3A41+GMT; uid_tt_tiktokseller=8010052edaaa2f6bedaa9d06b1252615b080d8ca7afa917b3becdc3e6944d14a; uid_tt_ss_tiktokseller=8010052edaaa2f6bedaa9d06b1252615b080d8ca7afa917b3becdc3e6944d14a; sid_tt_tiktokseller=800f155a447c44f3880daba446359b17; sessionid_tiktokseller=800f155a447c44f3880daba446359b17; sessionid_ss_tiktokseller=800f155a447c44f3880daba446359b17; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CgA8VWkR8RPOIDaukRjWbF__________YpKqedE__daNeW0q0OkFj6R8tQAR4gG4iCW8SV3Wx_WI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDk2ZDE4OWE1ZmZiZjU1YmVjMjlmYWZiMTNlODE0NjMwMjFiNGEzZDEKHAiViLWa9-_oimkQ9efs0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MDBmMTU1YTQ0N2M0NGYzODgwZGFiYTQ0NjM1OWIxNzJOCiA-mptb3kRPa35HYR0-qhGr5H6TiYwd8xCDgxEFS6BOyhIgqzhwiUrr5yu-g1FVwuBp-XjuSGcH_sYfYBe-o3fCHesYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDk2ZDE4OWE1ZmZiZjU1YmVjMjlmYWZiMTNlODE0NjMwMjFiNGEzZDEKHAiViLWa9-_oimkQ9efs0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MDBmMTU1YTQ0N2M0NGYzODgwZGFiYTQ0NjM1OWIxNzJOCiA-mptb3kRPa35HYR0-qhGr5H6TiYwd8xCDgxEFS6BOyhIgqzhwiUrr5yu-g1FVwuBp-XjuSGcH_sYfYBe-o3fCHesYAiIGdGlrdG9r; msToken=0qGXuhBJSkuhCIJtEMFx6BcbFFF9p2l_PwYdpZ9LGFDA8PxZtIB1rSK2SkUNfsUGTP84Rs5yXaQ--wz8siQZA_LdlLAyaxn0R8Kti9B1V_H55QWVpKHQ_oTCeP2YMHkPUkp8xJI=; _ga_BZBQ2QHQSP=GS2.1.s1782264791$o29$g1$t1782264819$j32$l0$h454299774; oec_lucifer=AQEBAMg2AAfv3iaR1r0nosp74LN89t5IvRujf2yMsHpExIONSLlfeZGAzf0ea8XV1sW7brnaQ1L4JFs/UaLkAhisCwKG03MkeQ==; lang_type=vi; odin_tt=8aba10458a2f8fa5288bcad2bd735e69621baa34e39ec22d2c7a5753fc4620f463b79dc2ef5df98d101ff7ac676064069b364ee98fae4a0967e8351094c9a324; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1782264848%7Cc623096e6cef9c3ed309c10ed7c62db44e935339f691657efc94b4033216a432; user_oec_info=0a530696085d51858626ca1e80234f2fad64b56719a31f9019fec6ec3b9fe789b59b8693038c2ac18db876cfa252129f4462292eeb4d981eb51eeb4165cd8e523cca2e9af20fc39c25714b8d508582d8714f383c301a490a3c0000000000000000000050948c98be887783e84e8ef4233b39ab07c6a85c40412d08708dbd869d38384cf330834f991cf636d8068bba87ee6d5fbbf410a081950e1886d2f6f20d220104c3768350; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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







