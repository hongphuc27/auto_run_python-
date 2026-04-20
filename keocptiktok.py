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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; ATLAS_LANG=vi-VN; _tea_utm_cache_4068={%22campaign_id%22:1861796447666290}; _tea_utm_cache_1583={%22campaign_id%22:1861796447666290}; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ttcsid_CMSS13RC77U1PJEFQUB0=1776396362719::btvc1oDwuuCD4SHmMMg3.4.1776396421402.1; ttcsid=1776396362720::eCO-JscRKJ1amZpKTiMW.4.1776396421402.0::1.44961.46765::58687.22.846.390::0.0.0; sso_uid_tt_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_uid_tt_ss_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_user_ads=454dba0da10f723807a2be83b8845e3c; sso_user_ss_ads=454dba0da10f723807a2be83b8845e3c; sid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; sid_guard_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913%7C1776396422%7C259200%7CMon%2C+20-Apr-2026+03%3A27%3A02+GMT; uid_tt_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; uid_tt_ss_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; sid_tt_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_ss_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C0K6SqcXt_t_VFpZqfP3JE__________cW5jmUlATYZWcde9YMLX3C86hQU8NH-ScWt71tyTLaXg%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776396362$o4$g1$t1776396423$j59$l0$h690599615; lang_type=vi; msToken=QLijwVK5-ZogyMVyZPEzfeet_OapgJCL42Qq1wsoVeX7Fo0-_NPsEobwqHTly918EhjFs7f9mD-NAmwcNu8-KvGlqUPAHRh1m9EQ7FLOrq6LB221nhpa_FHy3_eZnHbYrRaOVtw=; tt_csrf_token=QWhE3fK7-o1XpGaj4_VIXE5cih2yAu2HqdjE; msToken=tAhKl93DpAx6dp_LJ8RugcoyihEPRW2s5qbV8u7DHsfKyToVt-Wfsnmo9__xpv4-otB3BJoVOvO6H85TLcSbWPv_3bLPiHe73Vdp9FvCowHWfcnI6c7UhFqF2iKr; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzY1ODQ2MjUsIm5iZiI6MTc3NjQ5NzIyNX0.TixX7b5-__yo_m-OfJKzGcWFfgG2PwrZXkfTjxqdkmI; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2NTg0NjI1LCJuYmYiOjE3NzY0OTcyMjV9.M-HiQIgfX1Fv5mM2fsqh9rM3PTVjpAG-11D6RexrimE; s_v_web_id=verify_mo416cco_kckZtA4t_S5gF_4xLM_8YmC_dxQzXlDC36K4; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776498299%7Ce757351d97f1fea941ce6cc83c12a51a4f3804b672ad2b715cc9c131453f873d; odin_tt=0d75ad6a5753aaa1c970bf10879ed670818903005a2ad7031592b54fb23a16249d560f613755b08581c5d8e10b65b247f5c22e05cb37ef9e02ecc3255b6775cc; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a5357d68def074d9a9161b23ea2cc17781dbe8dcf2a4e70914bc24832517093fdcf91444dc435c4eca3f9eae63a610ce9e40115fa6b4d0f4c5efe49a0af75814534c6cd9c5499844a5069f835340deda2047a9b2d1a490a3c00000000000000000000505170c8e8aea3bb99bcdaf0fa0224854ac20c6918d184498099cfbbfc415176b882c9ce9f4906997f0132d6b0edd6da903d1083918f0e1886d2f6f20d220104df0dec1a"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

# today = datetime.today().date()
today = datetime(2026, 4, 19).date()
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
