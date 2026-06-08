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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; s_v_web_id=verify_mq2e0co0_t3AXwswz_EMtf_4jYP_Biu0_kOuXEHo9l4BQ; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; FPLC=PBM9YP5WDlIeVpPaXfqBGxlZwhO7kRI84BVEtjMPEbaqvPN05tFNcZpbBg6d%2BRlKMhfIytEJpSLjy%2FuyYXFqoy7bBSmitSfMAJ%2BdFPEfkQ6TN6JK%2B%2FS4lkmIqyhnHQ%3D%3D; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2; app_id_unified_seller_env=4068; _m4b_theme_=new; oec_lucifer=AQEBAAR9HRsseehRRB1E25dAbPQwuthBLKnxjivqx+CPuUgCuEvQcCeh7imh86ADRbpnT7xm73NpyEYEWG6B4mDaFWcxd4iOgg==; _ttp=3EpngyoACPAtnskmFDB413Vgv2i.tt.1; store-country-sign=MEIEDOBdiyreOS8suMgNuAQguvxXqVS7WRjmFjGBLXQEcKL0L30mXKL8lmXEg33_2fMEEMy5dGA9tJIbMf5VVKjqDic; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; msToken=cU6MlF72DRbSzIBQcoDKL2N1WiIOFj33P42FI_lDVMfqog3Al0dWz9kiiFVoNGdE6GNsu_CF1dHKm5amOyvlcxh9rpB4mrzTN-M9n8ORRrs0zJm9kKY4rLxqkL1gPyTnUBhkjCY=; ttcsid=1780887087929::mq20Hy6ZJEpYj-AuR9py.24.1780887256829.0::1.52358.53795::168898.63.920.574::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1780887087929::jVbvylfCROGFCgeMyPEp.24.1780887256829.1; sso_uid_tt_ads=f38b98b03546dad5587412bd092f5d7f59b1fc6c68143be33d5cec00246e2efe; sso_uid_tt_ss_ads=f38b98b03546dad5587412bd092f5d7f59b1fc6c68143be33d5cec00246e2efe; sso_user_ads=0b9d1addb1632b8b4770ebc69b7662dd; sso_user_ss_ads=0b9d1addb1632b8b4770ebc69b7662dd; sid_ucp_sso_v1_ads=1.0.1-KDEyYzE4YjVmNDcxOGFiMDg3M2ZkMDY3Mzc3YmY3NDMxYzllZThkZGMKIgiViLWa9-_oimkQ0t2Y0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAwYjlkMWFkZGIxNjMyYjhiNDc3MGViYzY5Yjc2NjJkZDJOCiBahBagHh00m8nWH4ZXRD-h8LDjiVm7sMu05rqf47vTnRIgTCo7c_GKyY0C2royLCxwV2K5E-mtdgQu5_wE5sf6F3oYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDEyYzE4YjVmNDcxOGFiMDg3M2ZkMDY3Mzc3YmY3NDMxYzllZThkZGMKIgiViLWa9-_oimkQ0t2Y0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAwYjlkMWFkZGIxNjMyYjhiNDc3MGViYzY5Yjc2NjJkZDJOCiBahBagHh00m8nWH4ZXRD-h8LDjiVm7sMu05rqf47vTnRIgTCo7c_GKyY0C2royLCxwV2K5E-mtdgQu5_wE5sf6F3oYASIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1780887087$o24$g1$t1780887259$j60$l0$h1123907633; FPGSID=1.1780887081.1780887250.G-BZBQ2QHQSP.weoCXQSB_2TsNWEuC0Ehzw; sid_guard_tiktokseller=0c30898f4842eeb05bfd76441049cdb2%7C1780887250%7C259200%7CThu%2C+11-Jun-2026+02%3A54%3A10+GMT; uid_tt_tiktokseller=b5775f52103025e19c86012f97094c4f9fa05980012e6fbf79b1a69b4f5b97c0; uid_tt_ss_tiktokseller=b5775f52103025e19c86012f97094c4f9fa05980012e6fbf79b1a69b4f5b97c0; sid_tt_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; sessionid_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; sessionid_ss_tiktokseller=0c30898f4842eeb05bfd76441049cdb2; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CDDCJj0hC7rBb_XZEEEnNsv_________3sbeDZCJmSUHksag_gaPrj6hdxM2ktS9WJ6ViY-0FC3k%3D; sid_ucp_v1_tiktokseller=1.0.1-KGRjZGE4MzJlYjI3YWFkZmQzYTU5NDQ4YTc5OTkzOWZjN2Q0NGRiZjgKHAiViLWa9-_oimkQ0t2Y0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwYzMwODk4ZjQ4NDJlZWIwNWJmZDc2NDQxMDQ5Y2RiMjJOCiB4TKxClobqf7EDVxOizR5ChMprvZjnjnUafeHuLLSaURIgjqtiFirRZMX6EBUmDheA2J-sjKPHET2q9192g-cPScMYBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGRjZGE4MzJlYjI3YWFkZmQzYTU5NDQ4YTc5OTkzOWZjN2Q0NGRiZjgKHAiViLWa9-_oimkQ0t2Y0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwYzMwODk4ZjQ4NDJlZWIwNWJmZDc2NDQxMDQ5Y2RiMjJOCiB4TKxClobqf7EDVxOizR5ChMprvZjnjnUafeHuLLSaURIgjqtiFirRZMX6EBUmDheA2J-sjKPHET2q9192g-cPScMYBCIGdGlrdG9r; msToken=rqSQU_N2vyv8Xl1jeqwIyG8X7QkUIedUGugcchMqHDaopqQh_sI9R802w0EDICyRSS0JNBUauLBW4qPFD2qSFCm79P0Q0f1AQ39MDBlxdMDH5ZnGXyAsnY4RutHkgKQ-Z3M1BcQ=; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODA5NzM2NTIsIm5iZiI6MTc4MDg4NjI1Mn0.OVioRrwuXHHHhtodxcGDi3ZWJMsNBSdGnYXz-Z5xE-8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgwOTczNjUyLCJuYmYiOjE3ODA4ODYyNTJ9.gOzDdOrV_3QRuZ-sYoxynMKPV61T7vl1rs7R6ukBcEc; lang_type=vi; odin_tt=e62100b3c94dba23378d6a447a6034a5aed1351e136192ede60f21fe19c9b2150be576e431e00c57df371024a7eba535f29252269d7445016336dfa715d4f891; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1780887524%7C90ec264690ea31c04126b3bf8299a88271a776418e3941fe2b34728db9018719; user_oec_info=0a53324ef00fd317868afb4e8b4c176a111fb9ea72fe93490d7ccccb48067e5aea3662d30577a5ddec2e181057f0c98c6a10859375e3522a2f0d272712bb5410ea38078576a547e3466fa7e76100ac66bfbe426ae31a490a3c000000000000000000005084547225e20a04559c1531620314dc7a329d20e2473407a14985745cae9ccac4ec699642e2340f60df7941eeceff782d5610d5cd930e1886d2f6f20d220104e60a416d; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

# today = datetime.today().date()
today = datetime(2026, 6, 4).date()
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
