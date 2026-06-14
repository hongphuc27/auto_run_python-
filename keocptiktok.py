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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; _m4b_theme_=new; store-country-sign=MEIEDOBdiyreOS8suMgNuAQguvxXqVS7WRjmFjGBLXQEcKL0L30mXKL8lmXEg33_2fMEEMy5dGA9tJIbMf5VVKjqDic; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; oec_lucifer=AQEBANWIvnRfLMPiU2/EyV/V3h5nTuZshEtRP1Zz+7tRgXjDRykf1DiSiLuVu+YRzieP6EG4ltLqkZXVyq42cPFJz9ucrGLs7g==; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODE0MzI2NDEsIm5iZiI6MTc4MTM0NTI0MX0.jeB211fop0u2vJMUu01q-4cI55gMbfl9nxXEHNj19JA; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgxNDMyNjQxLCJuYmYiOjE3ODEzNDUyNDF9.DWlr70G8iwCCXfvuxkGmvzRGxnb55sqBu7X1Ki7hffU; s_v_web_id=verify_mqdk3cfr_2dvvmcLx_PPzK_4mKN_BwEv_lit56zbzKREX; FPLC=4sK9x2k6n6Myl%2Fh2JXv13yM5zByqoRew9rqvU5m06HYN%2BqE7Gc2OqG4fN5gR6G6ufQ868VHmKJvNIJ2%2FBCO64XJy7GOdtY%2FgywZROhk87gjI1dlRQ3%2B5iJr2xeBGqw%3D%3D; _ttp=3EzK9xoZG7rUM6Bu7ZPYMDn8QBT.tt.1; msToken=rWebQF3LeB1dmq_Pt9RCyaiL75vtc4vf64zIYZS8ISrQXbvZW4ZoROAeUWNa4UMcQdh37c9FVwfg8VDFEP6bIHWKURPayuVxG7peTjzQ7ACLmz4NC3wlIuYMt-OPUw==; ttcsid=1781427762685::gKAo0jk3MzZe2z0_mivE.27.1781427826143.0::1.-4602.0::60894.12.955.300::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1781427762684::VpgtF7YES5EcatmgkVWO.27.1781427826143.1; sso_uid_tt_ads=ab1ff18e854e62426724a16da046a11528b7ae74fcdc39b15922a81e3a24186a; sso_uid_tt_ss_ads=ab1ff18e854e62426724a16da046a11528b7ae74fcdc39b15922a81e3a24186a; sso_user_ads=5ad93f7f69655c85a73721cb17721482; sso_user_ss_ads=5ad93f7f69655c85a73721cb17721482; sid_ucp_sso_v1_ads=1.0.1-KDBmNDA5YmZmZmZkMmFmNmUyYWJjYTkwMDFhY2RiMDUyMGJiOTJjZTEKIgiViLWa9-_oimkQ8ty50QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDbXkyIiA1YWQ5M2Y3ZjY5NjU1Yzg1YTczNzIxY2IxNzcyMTQ4MjJOCiCfXL4jb-aPL89qOr22RCtnoKHt8SfoQD6wXlE0L-UfmBIgsE80K0kq9Gx_CcLQ5CBull6g5OCBB3RQ_6OkhLf77pcYAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDBmNDA5YmZmZmZkMmFmNmUyYWJjYTkwMDFhY2RiMDUyMGJiOTJjZTEKIgiViLWa9-_oimkQ8ty50QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDbXkyIiA1YWQ5M2Y3ZjY5NjU1Yzg1YTczNzIxY2IxNzcyMTQ4MjJOCiCfXL4jb-aPL89qOr22RCtnoKHt8SfoQD6wXlE0L-UfmBIgsE80K0kq9Gx_CcLQ5CBull6g5OCBB3RQ_6OkhLf77pcYAyIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1781427762$o27$g1$t1781427827$j60$l0$h1315281333; FPGSID=1.1781427761.1781427826.G-BZBQ2QHQSP.90fYQQUZI9ikA2nnt7Ql4A; sid_guard_tiktokseller=3d05796df6039867423a31e87513a75e%7C1781427827%7C259199%7CWed%2C+17-Jun-2026+09%3A03%3A46+GMT; uid_tt_tiktokseller=7290a18d394f5b05c190b1c755775e1b674f5d7ebd91631f81b356353427c8bc; uid_tt_ss_tiktokseller=7290a18d394f5b05c190b1c755775e1b674f5d7ebd91631f81b356353427c8bc; sid_tt_tiktokseller=3d05796df6039867423a31e87513a75e; sessionid_tiktokseller=3d05796df6039867423a31e87513a75e; sessionid_ss_tiktokseller=3d05796df6039867423a31e87513a75e; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CPQV5bfYDmGdCOjHodROnXv_________PgdwICQEcnMQXbxChW6EAh0e8UnzGNaqNWyd3qfJ00t0%3D; sid_ucp_v1_tiktokseller=1.0.1-KDNhMWYyNDRjM2IzZGYwZTAyMzcxOGI0NjMxNTVmMmY4NGZiMjUyNWYKHAiViLWa9-_oimkQ89y50QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzZDA1Nzk2ZGY2MDM5ODY3NDIzYTMxZTg3NTEzYTc1ZTJOCiAeeuxWn6q4nRkKvcy4-aCw8aIGNPglOH1wdU6QjQ5CfxIg_ZJa5fk3PoS0S49AhpMaI04k-OzrvOELFIvM3nQEmu8YASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDNhMWYyNDRjM2IzZGYwZTAyMzcxOGI0NjMxNTVmMmY4NGZiMjUyNWYKHAiViLWa9-_oimkQ89y50QYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzZDA1Nzk2ZGY2MDM5ODY3NDIzYTMxZTg3NTEzYTc1ZTJOCiAeeuxWn6q4nRkKvcy4-aCw8aIGNPglOH1wdU6QjQ5CfxIg_ZJa5fk3PoS0S49AhpMaI04k-OzrvOELFIvM3nQEmu8YASIGdGlrdG9r; msToken=9RYWY8k9IOxXgY1hIbJqsmdz5Py8XhJw3EtSUbdJE5bUCQqRo28RHBRt-2sIMHgZSHtyEoBlUYN5ovZkE_1d2Am5YT45FKbfIGysG8_NBQH0X6Qr5ix6D__Sy6JXmQ==; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1781427872%7C8a3f10f8782dc4dc97f4cbdd49fb3d61467197de9881ac43b92ab984f7211080; odin_tt=0214dcf93f3ce6bc81cb1449618531c1239c6f77394af902e74ef18822b09d0d96a7602867ceb5a0a5d2c5d5767741be8ad96ec4ec873afd31a31e30942e1b6f; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a5353d7a48a3fedec3040d1c351403f01c4655e31887db9c295405a117261665f69b515f2fe46b269408acc3a697c78c0b72be5fdb4c87216736b15d202a41fec9ec95a83bd87229cb150a5e84a3a89dc1c2171ee1a490a3c00000000000000000000508a43676bdd497a6ce886c50eee79fb978220b08ad295643baeaf41ac66d6c7b3a3d392bad2b80e52d737f56232fd1531ee10bc94940e1886d2f6f20d2201049bacfd87"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 6, 3).date()
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
