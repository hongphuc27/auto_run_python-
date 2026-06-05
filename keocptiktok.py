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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; oec_lucifer=AQEBAIW4e0LzJkLGlwXM6lVdaNVAdWgSww2E30v6v2GyC7J6hoItGN42fAPQNeqsqyjlBwyF34c/J0OrhSBpaOFmBuLpod1VLQ==; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde; d_ticket_ads=40b4f7f55431d4013801acad214d89a237e1d; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; store-country-sign=MEIEDL9xo__Wfy244k3mkgQgjE06ua-DT4Au8hBO2GIp2GLYfD-DwqguWXM530ojqlUEEIilSGTvPYQ2waY7OXvxEy8; ttcsid=1780472352747::ZO3uK8VVRzUEj09tP_o6.21.1780472370895.0::1.-3244.0::18145.9.673.490::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1780472352745::nRNIzgfTW9TYdkLXcx6c.21.1780472370895.1; sso_uid_tt_ads=e36fb6fc06bdaa2ea122b00a9f0df5aabc899679373996141c61c64f334a0525; sso_uid_tt_ss_ads=e36fb6fc06bdaa2ea122b00a9f0df5aabc899679373996141c61c64f334a0525; sso_user_ads=d8fff16e96012bde7c0c520b25aae605; sso_user_ss_ads=d8fff16e96012bde7c0c520b25aae605; sid_ucp_sso_v1_ads=1.0.1-KDQ1ZWEyYzliNjNmN2YxMzIwNjRhZTczNGFkNmVkNDg0MjRhMGJkMDgKIgiUiN7g9dSegGkQtbT_0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGQ4ZmZmMTZlOTYwMTJiZGU3YzBjNTIwYjI1YWFlNjA1Mk4KIH8TA5-96xYFeqAvIIl4KYUlZZrAHelBYn-NxtJYddzCEiBjGmn4Vsj4p4A9F5Vuc4vAVZFliR0cBAa2eeY4kC6FMhgCIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDQ1ZWEyYzliNjNmN2YxMzIwNjRhZTczNGFkNmVkNDg0MjRhMGJkMDgKIgiUiN7g9dSegGkQtbT_0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGQ4ZmZmMTZlOTYwMTJiZGU3YzBjNTIwYjI1YWFlNjA1Mk4KIH8TA5-96xYFeqAvIIl4KYUlZZrAHelBYn-NxtJYddzCEiBjGmn4Vsj4p4A9F5Vuc4vAVZFliR0cBAa2eeY4kC6FMhgCIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1780472352$o21$g1$t1780472373$j39$l0$h1079694751; sid_guard_tiktokseller=c5be8d781c53fea63b6daf9a57d6c815%7C1780472374%7C259199%7CSat%2C+06-Jun-2026+07%3A39%3A33+GMT; uid_tt_tiktokseller=2032f7860e25873e71b837dacd0d979ed1bad5a6c95a40b8f53b430bfea33db4; uid_tt_ss_tiktokseller=2032f7860e25873e71b837dacd0d979ed1bad5a6c95a40b8f53b430bfea33db4; sid_tt_tiktokseller=c5be8d781c53fea63b6daf9a57d6c815; sessionid_tiktokseller=c5be8d781c53fea63b6daf9a57d6c815; sessionid_ss_tiktokseller=c5be8d781c53fea63b6daf9a57d6c815; tt_session_tlb_tag_tiktokseller=sttt%7C5%7Cxb6NeBxT_qY7ba-aV9bIFf________-dNwuwyRtQkgPIAiOgmc_aK6zIsLQj8_ua0t6-y6qGMb4%3D; sid_ucp_v1_tiktokseller=1.0.1-KGZmZDU0OTgyN2VmYjljYzkyNTlhNWJlNDZmOWU4MmFiMGU4ZTVhMmUKHAiUiN7g9dSegGkQtrT_0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjNWJlOGQ3ODFjNTNmZWE2M2I2ZGFmOWE1N2Q2YzgxNTJOCiAnkpjtof5QAgG3jTEFvVuZp4cyW8i4wZ0hk3GHGTGFQRIgvqnq7GCGDAam4s6A_TzmCz_ygDio7QDt1rriPKkEeUcYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGZmZDU0OTgyN2VmYjljYzkyNTlhNWJlNDZmOWU4MmFiMGU4ZTVhMmUKHAiUiN7g9dSegGkQtrT_0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiBjNWJlOGQ3ODFjNTNmZWE2M2I2ZGFmOWE1N2Q2YzgxNTJOCiAnkpjtof5QAgG3jTEFvVuZp4cyW8i4wZ0hk3GHGTGFQRIgvqnq7GCGDAam4s6A_TzmCz_ygDio7QDt1rriPKkEeUcYAyIGdGlrdG9r; msToken=0R4F0Xevx7BB4kWNKcSU7q4fkOQ_ulSS2r0jn2aDNEkUTwMAduguiN5ZOqtQnEuLmGcD3PPXMkY1RCysxXrAh_vFd8WwEfxLnJOgV-moZMeGW7WM71u-kIrjeWTECM35iVwD7fUvsA==; msToken=0R4F0Xevx7BB4kWNKcSU7q4fkOQ_ulSS2r0jn2aDNEkUTwMAduguiN5ZOqtQnEuLmGcD3PPXMkY1RCysxXrAh_vFd8WwEfxLnJOgV-moZMeGW7WM71u-kIrjeWTECM35iVwD7fUvsA==; _ttp=3EcWqplRf0uRm4cOMsowD2v5R0k; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; s_v_web_id=verify_mq0ytlbk_Y4YIGPsn_R73s_4PR2_BKLX_G8f8Z7vtCn8f; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODA3NTI4NDAsIm5iZiI6MTc4MDY2NTQ0MH0.p5Jjvbf1hiMY0LjfNIaZY4ZFu_nt5YAdgpxC-7C11gQ; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgwNzUyODQwLCJuYmYiOjE3ODA2NjU0NDB9.QpsbT5Iar9ZcDA7il_8r4e2IdeKFtoxJYPmbZq-6ezc; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1780666547%7Cf31424d575dac819be33c820a2686064fce6cb3829577e3631aea62c895b2285; odin_tt=f19e581ad7e7363f9e92685235b28c964ee788214412cc5542a6aa8397369f0d8a5b6d2f6a1937d23d9fdd70bd07f490d623498b50774fcdfbac59fb2834b6a6; user_oec_info=0a53ade3d2185919315f88251625b75da62cee5083fc6fd1135724506a20e1ace999bd083e5f5e90004b3f27eabbd5d7060934f41a4c13864f94f8cc68fe127c54cd8607912e6545b61edd9798019834cc8613ac421a490a3c000000000000000000005081923e49d1bb5f5b5dd055cdbd8f377bac493b2820199bed9c1e88e8e026044a6b3124b58821d3162361b19860761e94d11082b1930e1886d2f6f20d220104c6564cbb; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
