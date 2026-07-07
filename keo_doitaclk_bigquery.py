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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDGn2a3p3E0H2QlTdcAQgQ59vpwlpCePUZm51gAhTgs7hh0kpudj6n18IMjWCnBsEEHCwnzZ3VY8pcUqam-Pg2Fk; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODMzOTAyNDUsIm5iZiI6MTc4MzMwMjg0NX0.DWURWXo_L2Ew9Y9uIHjnI2Ga8qmEcoj1mx-ajUICfYY; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgzMzkwMjQ2LCJuYmYiOjE3ODMzMDI4NDZ9.HCeeA76VF2mcDdBynDy4FDF8FDjekw83e3dF_TGbBOc; s_v_web_id=verify_mra00jmm_afGgufRr_Xl5J_4eHE_BXIh_Q3bgmPk0I89Y; FPAU=1.2.1047425659.1783389422; FPGSID=1.1783389421.1783389421.G-BZBQ2QHQSP.xoOHZI2VnGinVCW32z1Row; _ttp=3G1l3QXGJhwQK1YauogkSf2xRki.tt.1; FPLC=e079Q2%2BfiMdCje6nySKjG%2Fd3G%2FP0pnCXWoIiJAhrT9%2BEnKC%2BVjudTc5zKv6PtDvJCi2aCQZR32Xit8j8QtRWwELbRKQjn3%2BHM8JLUvR%2BlyQVkeT0U%2FATF9g4nCn9Lg%3D%3D; msToken=VwkXmWyvcDcpaZVdeQVJhVydltWWVoKzJEROTCojSnYj2EVwTU1jPFyNJz91EgMN1CKn588qesRN80pMbQ4SvyrWg-jUE8o-qLLp_zvrhBJ-3xvEBDf9x1RfGpnzXsSgU9tpdR6JlX6xiQkGp20FFyTd; sso_uid_tt_ads=6306b3ad73711f32f70638577d1e56c76ca7a8fb08cdf2daf36ae548a20f1b4d; sso_uid_tt_ss_ads=6306b3ad73711f32f70638577d1e56c76ca7a8fb08cdf2daf36ae548a20f1b4d; sso_user_ads=2a17390f694186c6010ee8f8f7a166fe; sso_user_ss_ads=2a17390f694186c6010ee8f8f7a166fe; sid_ucp_sso_v1_ads=1.0.1-KDE3NjZjMjBhYzZjMGU3NTE4MzU0YTY3Mjg1MTBmNWVkZWM2ZDE2OTYKIgiViLWa9-_oimkQjrqx0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhMTczOTBmNjk0MTg2YzYwMTBlZThmOGY3YTE2NmZlMk4KIHLRkfx4FYz5Cex7dqfMpAFiEf5AqiS6R51kRS80NZNfEiBZ5S1z2jbqHndvxJomezu939ahw3JlQNJDwDtdbEQ3yRgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDE3NjZjMjBhYzZjMGU3NTE4MzU0YTY3Mjg1MTBmNWVkZWM2ZDE2OTYKIgiViLWa9-_oimkQjrqx0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhMTczOTBmNjk0MTg2YzYwMTBlZThmOGY3YTE2NmZlMk4KIHLRkfx4FYz5Cex7dqfMpAFiEf5AqiS6R51kRS80NZNfEiBZ5S1z2jbqHndvxJomezu939ahw3JlQNJDwDtdbEQ3yRgEIgZ0aWt0b2s; oec_lucifer=AQEBAHdiMXkKuqV/KiTWoa8Yp/EzH4AuSMM7evu1iiZwKbZOhIXBOnucjy0pvOBU2P16g1s3oay0OX1Xi9xE81PxWxS6vJjNmwn5; sid_guard_ads=2a17390f694186c6010ee8f8f7a166fe%7C1783389462%7C259192%7CFri%2C+10-Jul-2026+01%3A57%3A34+GMT; uid_tt_ads=6306b3ad73711f32f70638577d1e56c76ca7a8fb08cdf2daf36ae548a20f1b4d; uid_tt_ss_ads=6306b3ad73711f32f70638577d1e56c76ca7a8fb08cdf2daf36ae548a20f1b4d; sid_tt_ads=2a17390f694186c6010ee8f8f7a166fe; sessionid_ads=2a17390f694186c6010ee8f8f7a166fe; sessionid_ss_ads=2a17390f694186c6010ee8f8f7a166fe; tt_session_tlb_tag_ads=sttt%7C2%7CKhc5D2lBhsYBDuj496Fm_v_________itxukjuduh86svsxdl5BT-XWbxtDWwQIF7nYWYT0FMFM%3D; _ga_BZBQ2QHQSP=GS2.1.s1783389421$o33$g1$t1783389462$j19$l0$h1809851687; odin_tt=94a6b68550458d1fd753fae02fd0f05f5aa4be12059336351da0e7244822183052bdd9da45607bb1f3b40262fa98598c236a9cb93082b7cadf1b07269c6ad232; sid_guard_tiktokseller=cdac54ed1275f4d3063dca39cecc67b5%7C1783389462%7C259192%7CFri%2C+10-Jul-2026+01%3A57%3A34+GMT; uid_tt_tiktokseller=5d9caa4965dc8d74a4d56c8655f2eab58dbd773bdd9098218152c57b83b3f7a6; uid_tt_ss_tiktokseller=5d9caa4965dc8d74a4d56c8655f2eab58dbd773bdd9098218152c57b83b3f7a6; sid_tt_tiktokseller=cdac54ed1275f4d3063dca39cecc67b5; sessionid_tiktokseller=cdac54ed1275f4d3063dca39cecc67b5; sessionid_ss_tiktokseller=cdac54ed1275f4d3063dca39cecc67b5; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CzaxU7RJ19NMGPco5zsxntf_________d9TQ_O0on4y5obt1Oo-awrnV4c2JXs4bGcwrVaF0Pw3M%3D; sid_ucp_v1_tiktokseller=1.0.1-KGVmYjUwNDE3ODNkNWU0NjgyYzQxZDdkODZiNDEzOGQ5MDNlN2ZmOGUKHAiViLWa9-_oimkQlrqx0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjZGFjNTRlZDEyNzVmNGQzMDYzZGNhMzljZWNjNjdiNTJOCiAu3XD0Gbc4nz6iApHqY-rtD9kUQ-CZDXD5AmADqddvfRIgrCskFfqeUZ0AwWpRIU0MtIGkawqrKdT6b1dIYVeO51MYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGVmYjUwNDE3ODNkNWU0NjgyYzQxZDdkODZiNDEzOGQ5MDNlN2ZmOGUKHAiViLWa9-_oimkQlrqx0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjZGFjNTRlZDEyNzVmNGQzMDYzZGNhMzljZWNjNjdiNTJOCiAu3XD0Gbc4nz6iApHqY-rtD9kUQ-CZDXD5AmADqddvfRIgrCskFfqeUZ0AwWpRIU0MtIGkawqrKdT6b1dIYVeO51MYASIGdGlrdG9r; msToken=mqgyv6stUG4dwxj1ihQ8uambwSTXbhTqQYGfWoxgQUyvm-b6Q-Aos0-3K40XuEZrvpq5-lOZ-OdgP9OxeFq4a-tuZx0RI3ugj2wyYxH3OVC3URtLIhwJ9hSa7eqCRWUgA7PaSRFRT3dFs-xvGHiPN1ur; ttcsid=1783389421926::cgyG2vCPSxQ5GH8d_g9w.34.1783389462537.0::1.39055.40386::30048.23.703.492::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1783389421926::Lt231XptA6XFwcyjwUq0.34.1783389462537.1; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1783389473%7C6d065b67805f33539a653e33e4a3b850979538b0ca58253e38dcecc68e778004; user_oec_info=0a53f3461bec91c88cd877ef6d4b01a98fdfea810221abd89135e20e7ee831cbdb3189507a09435b3e26f1344eeb91b5bbde3968b0a757809fc3a5bd899dc42f95adcd9e4c43273566e83fe7cb3e79458dee25bbed1a490a3c0000000000000000000050a107855fce58786e71c08a0939052586fd2cfc578ec6e35683cfc7c8f2c908e44aaa383f0ac15af2fc31634eefdd4ba62f10ba93960e1886d2f6f20d220104ac004d15; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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







