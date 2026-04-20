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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; ATLAS_LANG=vi-VN; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzY3MzUzMjUsIm5iZiI6MTc3NjY0NzkyNX0.RWl-ufbIvl8O6H_zzouop3d1HHFR1NKuKwqTmgyO7vc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2NzM1MzI1LCJuYmYiOjE3NzY2NDc5MjV9.2NS4FAqgAnEY52PxHmDkvbyN8X1cf-zmqQz2L8Ym2A8; _tea_utm_cache_4068={%22campaign_id%22:1862554752900321}; _tea_utm_cache_1583={%22campaign_id%22:1862554752900321}; s_v_web_id=verify_mo7fk70g_4MOxoUaa_pLLI_434q_8G9e_f8JMruAQYy2z; FPGSID=1.1776703789.1776703789.G-BZBQ2QHQSP.YybFmiysct3UzJdHdQCyzQ; FPLC=m%2BbWtTwLgm2Gjk1B5ajmnlcKmVWPTC6YfpOmore7wkf5txgFasXSL1TCoHOltuX%2FBjCm0pUxCGcFbpSnrKC8xRuCn8DMgslfhTM%2FUiuCGXWXfUrIwKg9Fx9HXx7%2FkQ%3D%3D; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI.tt.1; msToken=IJ7_h5K8Ge0uuXzCUfAKOACR3TuO4GFKL0RDHy3UyuIPHPgG-iNudm291fLocx6RQdmqpi4WPBTava5TQaBtB0G-8kQVAN8jI2ooq1jYpWVfbzu1L7cbsdw7CMB9gw==; ttcsid=1776703789312::Zqs_hQMECBQnDIxcCCnB.5.1776703822619.0::1.-5027.0::33302.11.673.477::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1776703789312::RTb_vX0skd9ZysWHByR1.5.1776703822619.1; sso_uid_tt_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_uid_tt_ss_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_user_ads=a92756a08698fcc35544bb54e270ef04; sso_user_ss_ads=a92756a08698fcc35544bb54e270ef04; sid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776703788$o5$g1$t1776703825$j23$l0$h328177141; sid_guard_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27%7C1776703825%7C259200%7CThu%2C+23-Apr-2026+16%3A50%3A25+GMT; uid_tt_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; uid_tt_ss_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; sid_tt_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_ss_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; tt_session_tlb_tag_tiktokseller=sttt%7C5%7Cq03uHjMyIYq84OhzSustJ__________7hEyD0wF4ndOX2NXXZpZnPJwAQP9zGzJFzvwKgUMFezk%3D; sid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; msToken=lDBJbWpzw_j1ohK267hJAR_9vqo6mOAO89F3mifkSLhcVU9baY0rF5Ll3PsdhGcgJUwLzpjBNHM-y0ZkDvG15n6S4QyxXMVIWT06StwoD97jDN8sHzpye5NfVVIjPg==; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776703928%7Cf663a35b310c322ce2733d4dbc9dc8afe297e10297f4eadfcb995e027100410b; user_oec_info=0a5375fee0afe2937b6dbd413f37117a4f9a03ac9da217ccb68c921707827721ba61ccf134c03998533ddab4378969b1e8cc40bb0fea4d8f9a72fa608d8e95927bc271f7c7be12d41dc0f21e2b39b94e9b378241e11a490a3c00000000000000000000505324332643f4aee5252bef7edf28561c41fb8d7931f6ec91cc8afacc63332a3cfc34ae73427ce40923de8edba1d3d6ceff10c2ac8f0e1886d2f6f20d2201049dfc8be1; odin_tt=02e1bbad039f4c31b7f9bde0bd5c7308bd7a701c5a87e67cc52bc685e76be78c7cfc1a9e6c25014a9fe1ea1a60cb53b2fba2d42b49228c12cc291abe2faf3be5; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 4, 18).date()
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
