import requests
import pandas as pd
import json
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os


# =====================================================
# 1. API CONFIG
# =====================================================

URL = "https://seller-vn.tiktok.com/oec_ads/shopping/v1/oec/stat/post_overview_stat"

PARAMS = {
    "locale": "vi",
    "language": "vi",
    "oec_seller_id": "7494545630022240481",
    "aadvid": "7628879252477231124"
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://seller-vn.tiktok.com",
    "referer": "https://seller-vn.tiktok.com/ads-creation/dashboard?shop_region=VN&type=live",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "x-csrftoken": "IEsxTIiBGAofRD43UXSQ2LACUc63dxAN",
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; oec_lucifer=AQEBAIW4e0LzJkLGlwXM6lVdaNVAdWgSww2E30v6v2GyC7J6hoItGN42fAPQNeqsqyjlBwyF34c/J0OrhSBpaOFmBuLpod1VLQ==; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde; d_ticket_ads=40b4f7f55431d4013801acad214d89a237e1d; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; tt_csrf_token=VHfpbhEh-zcmko6bmHhp_kaSiBS3VkE30JBI; store-country-sign=MEIEDEWmOX3IoDPDpTc13AQgUcZ_xnEheu3me-dJx0P9j5yrcQSYxhX1krnz4HPQUOkEEO70gswdtauJtA_HrVjKIak; s_v_web_id=verify_mpnjzi4w_74uIygEN_avRV_4ujB_AdE0_aEg59zmLQAJM; FPGSID=1.1779865181.1779865181.G-BZBQ2QHQSP.qPoWlMfG-pc_PafI9KMi7Q; _ttp=3EDQ0yHbwWwp9JfDz49lS7Y9nTV.tt.1; FPLC=DmjoQ4jub7EjjxKrCP9cnJ69VKn%2B8bnVxSWHsnBaI%2BaDTyQOOL6LHJpU0q9iXjMVmqGQNuNcfRDl93NR1JYo1JN%2BIVlEi8iB3fhEiee1jzeXwFyq6PnkW3aqX%2Fm%2BmA%3D%3D; msToken=5R-9HlpnhhucK6ki0YzVZhDdH5rI9KBKvHi6gwC6QSmI73QR8BDXe9NfrlR4JjzulPQa_zzypGmiqtDW8zOUN8qO85GgXT2WtOe-GpwjAsLPMNkv9sl9zEUlL6BFGbPiFWxGqE0=; ttcsid=1779865181975::TxJD2SY6LjzUQWyPZjxn.19.1779865203907.0::1.-2154.0::21929.16.1006.568::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1779865181975::Z32D4dG0_j7NOWX8JTDb.19.1779865203907.1; sso_uid_tt_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sso_uid_tt_ss_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sso_user_ads=e4b6187eabde1fe83f9a57de8e97b848; sso_user_ss_ads=e4b6187eabde1fe83f9a57de8e97b848; sid_ucp_sso_v1_ads=1.0.1-KGYzMzcyOGI2ZTg5NGE4YjljMWNiYzdmZGZmZWYwMjQ4OWQ4Y2RhY2IKIgiUiN7g9dSegGkQ9aza0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGU0YjYxODdlYWJkZTFmZTgzZjlhNTdkZThlOTdiODQ4Mk4KIIXfRVVwHaOFJl-48fRJhmpok515iFQyk9V06OjhpGoDEiAFNRES-rvjBwewgrRQ5m_yZni22FvfU0V3YTtOogtbChgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGYzMzcyOGI2ZTg5NGE4YjljMWNiYzdmZGZmZWYwMjQ4OWQ4Y2RhY2IKIgiUiN7g9dSegGkQ9aza0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGU0YjYxODdlYWJkZTFmZTgzZjlhNTdkZThlOTdiODQ4Mk4KIIXfRVVwHaOFJl-48fRJhmpok515iFQyk9V06OjhpGoDEiAFNRES-rvjBwewgrRQ5m_yZni22FvfU0V3YTtOogtbChgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1779865181$o19$g1$t1779865206$j35$l0$h1702510088; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; uid_tt_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; uid_tt_ss_ads=340c92a91b6bb74ce08c9a40b18f7e633d1a33ad9ecf7549ec4671125ab49721; sid_tt_ads=e4b6187eabde1fe83f9a57de8e97b848; sessionid_ads=e4b6187eabde1fe83f9a57de8e97b848; sessionid_ss_ads=e4b6187eabde1fe83f9a57de8e97b848; tt_session_tlb_tag_ads=sttt%7C5%7C5LYYfqveH-g_mlfejpe4SP_________N5DwoCP13gdDcwROEaYCzYsa6Qtos4xdys8zlmwPJHKM%3D; sid_guard_tiktokseller=93f59619b994cc6b2a3b49247b4f3346%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; uid_tt_tiktokseller=68ffb5a326efe48a8019903eaaa8123d793c2d0a4db43462253bf7b016eaf638; uid_tt_ss_tiktokseller=68ffb5a326efe48a8019903eaaa8123d793c2d0a4db43462253bf7b016eaf638; sid_tt_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; sessionid_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; sessionid_ss_tiktokseller=93f59619b994cc6b2a3b49247b4f3346; tt_session_tlb_tag_tiktokseller=sttt%7C2%7Ck_WWGbmUzGsqO0kke08zRv_________s-74QcVV1qADkPQQm9rMOwD_rHO9WlVqeOiaDdxAvh60%3D; sid_ucp_v1_tiktokseller=1.0.1-KGJiOTNjNmJlZTQ5NjdkNjYzMDE2YWU0OGI2MmRiMzQ4ODc1NTJkZmMKHAiUiN7g9dSegGkQ9qza0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA5M2Y1OTYxOWI5OTRjYzZiMmEzYjQ5MjQ3YjRmMzM0NjJOCiCGfj7beYuCkb-hg8Yz5zGjYO8XgxmJq_kjaxNdbi8zORIg2ZRA5LfZVGzRbpStT2E3qKdY0Lv_XKm1T3-B_B5jOt0YBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGJiOTNjNmJlZTQ5NjdkNjYzMDE2YWU0OGI2MmRiMzQ4ODc1NTJkZmMKHAiUiN7g9dSegGkQ9qza0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA5M2Y1OTYxOWI5OTRjYzZiMmEzYjQ5MjQ3YjRmMzM0NjJOCiCGfj7beYuCkb-hg8Yz5zGjYO8XgxmJq_kjaxNdbi8zORIg2ZRA5LfZVGzRbpStT2E3qKdY0Lv_XKm1T3-B_B5jOt0YBCIGdGlrdG9r; msToken=UYvwSlbFX_H33D8FC_osJXIxZ2PJMQ4hRHMSTsAm9vxoIrDyrP-HIcZVfyiCrRw7abIuWNJPUojZHHKZG_Vro1a4YZ9EbD4lzx_V2nGeJ73es5wgFu-WpgPjdcAKS5aFrnFUuKI=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzk5NTE2MDcsIm5iZiI6MTc3OTg2NDIwN30.rNYucBq8czRJJLPVGuBQyr92G56yFj1RS1dMRw8B1Pc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc5OTUxNjA3LCJuYmYiOjE3Nzk4NjQyMDd9.UxLrxcmb-MIzs0InVbdNF0sH1N7Unyz4STICaWJy4uE; lang_type=vi; odin_tt=a763456527292c02f8058f07e86a7678d664592f34be79062b946c45664482b57f56cf0a5be11ec106d69902da17ce0180cad629566472abfeb878fc705df363; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1779865325%7Ce93d7f8e63655b6dfbf0baacb34913b4a48600f0bc811e7e9f7575468f9a8373; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a532d347827a5760f5bd1cc83b65a97c135bc535ca760109e648f4fbf6674d1989d601558506d6c8a9b168b8736a7dcb1018e301569b692b7cb1621c0cfd5088005b85a3850f97a7074cc175a6948b16ffa8cc3e91a490a3c000000000000000000005078d1d3b5ad398dfbea793a4116acbcf991bfe3f2c47439ca3f21e664e96b8393f3bae9fdba4e7aa5b40817fd463595597e10a2c9920e1886d2f6f20d2201042367297d"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 5, 21).date()
api_date = today.strftime("%Y-%m-%d")

# =====================================================
# 3. PAYLOAD
# =====================================================

PAYLOAD = {
    "query_list": ["cost"],
    "start_time": api_date,
    "end_time": api_date,
    "stat_type": "day",
    "campaign_shop_automation_type": 2,
    "external_type_list": ["307", "304", "305"]
}

# =====================================================
# 4. CALL API
# =====================================================

resp = requests.post(
    URL,
    params=PARAMS,
    headers=HEADERS,
    json=PAYLOAD,
    timeout=30
)

print(f"Fetch daily ads cost | Status {resp.status_code}")
resp.raise_for_status()

data = resp.json()

# =====================================================
# 5. PARSE DAILY COST
# =====================================================

chart = data.get("data", {}).get("chart")

if not chart:
    print("API không trả chart")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    raise SystemExit()

dates = chart.get("categories", [])
series = chart.get("series", [])

rows = []

for s in series:
    if s.get("name") == "cost":
        for d, c in zip(dates, s.get("data", [])):
            rows.append({
                "date": d,
                "cost": float(c)
            })

df = pd.DataFrame(rows)

print(f"\nDAILY COST ({len(df)} dòng)")
print(df.head())

if df.empty:
    print("DataFrame rỗng -> STOP")
    raise SystemExit()

# =====================================================
# 6. FIX HOURLY -> DATE + HOUR
# =====================================================

if df["date"].astype(str).str.contains(":").any():
    print("Detect hourly data -> map vào (date, hour)")
    df["hour"] = df["date"].astype(str).str.slice(0, 2).astype(int)
    df["date"] = api_date 
    df = df[["date", "hour", "cost"]]
else:
    df["hour"] = pd.NA
    df = df[["date", "hour", "cost"]]

# convert kiểu dữ liệu đúng cho BigQuery
df["date"] = pd.to_datetime(df["date"]).dt.date
df["hour"] = df["hour"].astype("Int64")
df["cost"] = df["cost"].astype(float)

# =====================================================
# 7. BIGQUERY CONFIG
# =====================================================

PROJECT_ID = "rhysman-data-warehouse-488306"  
DATASET_ID = "rhysman"
TABLE_ID = "fact_cost_daily_tiktok"

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# =====================================================
# 8. DELETE OLD DATA
# =====================================================

bq_date = today

delete_sql = f"""
DELETE FROM `{table_ref}`
WHERE date = @date
"""

job_config_delete = bigquery.QueryJobConfig(
    query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE", bq_date),
    ]
)

delete_job = client.query(delete_sql, job_config=job_config_delete)
delete_job.result()

print(f"Deleted old data for {bq_date}")
# =====================================================
# 9. LOAD DATAFRAME TO BIGQUERY
# =====================================================

job_config_load = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    schema=[
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hour", "INT64"),
        bigquery.SchemaField("cost", "FLOAT64"),
    ]
)

load_job = client.load_table_from_dataframe(
    df,
    table_ref,
    job_config=job_config_load
)

load_job.result()

print(f"DONE | Loaded {len(df)} rows into BigQuery: {table_ref}")
