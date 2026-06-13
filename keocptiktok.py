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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; _m4b_theme_=new; store-country-sign=MEIEDOBdiyreOS8suMgNuAQguvxXqVS7WRjmFjGBLXQEcKL0L30mXKL8lmXEg33_2fMEEMy5dGA9tJIbMf5VVKjqDic; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; s_v_web_id=verify_mq62xs0b_jd9wNqyr_H13x_4T6L_BQjP_1kMyVEwQzIqL; lang_type=vi; ttcsid=1781162754752::gVRtD7tzs1BG60jHdONq.26.1781162824655.0::1.15428.17215::65042.34.1157.310::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1781162754758::2YL5uxGovyKKsKGtLZmP.26.1781162824655.1; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_uid_tt_ads=a50a056160da6709828b65d0ca6d18f4668db30dbec18b5c0522eabbe646a2ee; sso_uid_tt_ss_ads=a50a056160da6709828b65d0ca6d18f4668db30dbec18b5c0522eabbe646a2ee; sso_user_ads=fef6739bb9a7684c48a044112d8aeb5f; sso_user_ss_ads=fef6739bb9a7684c48a044112d8aeb5f; sid_ucp_sso_v1_ads=1.0.1-KDEwNDUyN2ZlOGFmMWZmYzI0ZGVlMzRlMzlmMDEyYTVkYWUzNjJiMjAKIgiViLWa9-_oimkQxsap0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiBmZWY2NzM5YmI5YTc2ODRjNDhhMDQ0MTEyZDhhZWI1ZjJOCiACFmjAYhUr73ubLx2QfmjWLj6Tsbsq-SXekO1Bu23CWxIgYHt1J2pb0kFlEEKwmntTn742ZE_MbNUAih1Iji1NX_wYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDEwNDUyN2ZlOGFmMWZmYzI0ZGVlMzRlMzlmMDEyYTVkYWUzNjJiMjAKIgiViLWa9-_oimkQxsap0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiBmZWY2NzM5YmI5YTc2ODRjNDhhMDQ0MTEyZDhhZWI1ZjJOCiACFmjAYhUr73ubLx2QfmjWLj6Tsbsq-SXekO1Bu23CWxIgYHt1J2pb0kFlEEKwmntTn742ZE_MbNUAih1Iji1NX_wYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1781162770$o26$g1$t1781162825$j5$l0$h1993338363; sid_guard_tiktokseller=64e2348d7daf3369584c875f6f42ede7%7C1781162823%7C259199%7CSun%2C+14-Jun-2026+07%3A27%3A02+GMT; uid_tt_tiktokseller=4ea8d5ad2f89cca3a4b1a8535cfdf61d3f4bd1f5fb4a63cb8e0bb2ed8dad0c06; uid_tt_ss_tiktokseller=4ea8d5ad2f89cca3a4b1a8535cfdf61d3f4bd1f5fb4a63cb8e0bb2ed8dad0c06; sid_tt_tiktokseller=64e2348d7daf3369584c875f6f42ede7; sessionid_tiktokseller=64e2348d7daf3369584c875f6f42ede7; sessionid_ss_tiktokseller=64e2348d7daf3369584c875f6f42ede7; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CZOI0jX2vM2lYTIdfb0Lt5__________nVfXlaQ8nFdcyHsTwCdKzEXQ2SHc3CFSAp7-6kbYab4k%3D; sid_ucp_v1_tiktokseller=1.0.1-KDY0Mjk1MjhjNzcyZjA3YjE1YzUxOGY5M2M1MjIzZGE2ZjRlZDg4ZGQKHAiViLWa9-_oimkQx8ap0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2NGUyMzQ4ZDdkYWYzMzY5NTg0Yzg3NWY2ZjQyZWRlNzJOCiCMCrswzIKNqtEjKPH4Y70dZNup9aYo9WhD9s_kOr2X7xIg1xGxSqfU7yb3tZ5KE0Cqjoe3WOtiXKIaiMhG8938wtcYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDY0Mjk1MjhjNzcyZjA3YjE1YzUxOGY5M2M1MjIzZGE2ZjRlZDg4ZGQKHAiViLWa9-_oimkQx8ap0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA2NGUyMzQ4ZDdkYWYzMzY5NTg0Yzg3NWY2ZjQyZWRlNzJOCiCMCrswzIKNqtEjKPH4Y70dZNup9aYo9WhD9s_kOr2X7xIg1xGxSqfU7yb3tZ5KE0Cqjoe3WOtiXKIaiMhG8938wtcYBSIGdGlrdG9r; msToken=cSKYvH2Nh7NxmK4RzekXe0GKA7ZtJFwqzHK79a87IrqHfjqMZobenaBtPZgMyPvVvOjRkozFSoCa6Sl7Bk30BnVOO_1t8ktOPnJRoAVr2n2eCFCh-s5z5d7Dy8oQngE09G2SJVs=; _ttp=3EzK9xoZG7rUM6Bu7ZPYMDn8QBT; msToken=xZ_6LOYx-oEgQd740QFSoAzQZr6bGxdgyATNpMq4Qbd6cn1xOiyylT5NWbzEDLMuLMupR5uIhX26hyvOhhaT33EzwBIKuoZ_eP5lm_6xI1MvcbQbe6GxNKmD_q-wGlYk74yn3WAzo90mKS0=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; oec_lucifer=AQEBANWIvnRfLMPiU2/EyV/V3h5nTuZshEtRP1Zz+7tRgXjDRykf1DiSiLuVu+YRzieP6EG4ltLqkZXVyq42cPFJz9ucrGLs7g==; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODE0MzI2NDEsIm5iZiI6MTc4MTM0NTI0MX0.jeB211fop0u2vJMUu01q-4cI55gMbfl9nxXEHNj19JA; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgxNDMyNjQxLCJuYmYiOjE3ODEzNDUyNDF9.DWlr70G8iwCCXfvuxkGmvzRGxnb55sqBu7X1Ki7hffU; odin_tt=de8d5fed6fc3588d878e418ce375fd4054b0bb3935b3c75d5be8c6dd632de801c9c3b9f66dbdc91dd9244ca70642611755f6f3e849e2589d4d16f6a28c644d9f; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1781346262%7Cd6de2403a053fdb4d9a1b04a41614b74f32dd16a4922a25a8f6d07e03240a98b; user_oec_info=0a535855e6475473db4a9e38cd1adae4d5134a0eab17ff493f67517442bceedb7e45da5f4efafc13e1ae0e5ebbd892c26ff5b5571e7f8f6f754acd6db3d690be2c4b75f51aac3016b221450c0b97fb0222a560659a1a490a3c000000000000000000005089c9468a6468ad138095b98a63aea20d896862f758ca7b4fc897fa23b53583b73264143e775e2cff71e5499561a8912abc10f789940e1886d2f6f20d220104a43e9d5e; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
