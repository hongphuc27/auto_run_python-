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
    "aadvid": "7625589331687948309"
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://seller-vn.tiktok.com",
    "referer": "https://seller-vn.tiktok.com/ads-creation/dashboard?shop_region=VN&type=live",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "x-csrftoken": "pICMUok8m661IP2kRQPf2HpsAkfm4daB",
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; _m4b_theme_=new; gs_seller_type_for_report=pop; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776044459%7C259200%7CThu%2C+16-Apr-2026+01%3A40%3A59+GMT; sid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C1%7CiHhd0_VTVzXYzAY5HVf8hf_________X6QF71wEIh_9vkjIn8RqTBc05Xf_hP2Jh66NFE5GtjZw%3D; s_v_web_id=verify_mny26056_cEG4i3hF_RYZw_4LtV_8c3z_QvKJljelN5bQ; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; FPLC=Dedymv%2FPgKexpgm6MwQu70V2o0tCdLGm8%2FrUsRi4H5bfGgjFCyx%2F74WLsDtud1I4PGeiozilXmEdU7t0rP192Yb4pL67DBX1BaXkp2jbV5pwjgqyyf2HFDh6dqLmaQ%3D%3D; msToken=nXueNuOCBlXTR09gnE6SJlNdoOxgwIUVrtQDn9G8uBu-QI5eVI0HzYnYijIMmBrkvCSqsTAfOPZ38N9FHc3CyTGVR2MPsT6K2kP95NomBr2VDeOLXqQcAm72AU_zKSkHh9oufg==; ttcsid_CMSS13RC77U1PJEFQUB0=1776137101128::5q4gb__G7AmtOjDrYcpq.3.1776137126583.1; ttcsid=1776137101129::Dul7gHZIRT8MHOUSyB4R.3.1776137126583.0::1.-8026.0::31752.14.731.485::0.0.0; sso_uid_tt_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_uid_tt_ss_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_user_ads=3793b57f115dd5c737561e433a6cf308; sso_user_ss_ads=3793b57f115dd5c737561e433a6cf308; sid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776137100$o3$g1$t1776137135$j25$l0$h1248465928; sid_guard_tiktokseller=88550a7902dbadb4571676b145078738%7C1776137138%7C259199%7CFri%2C+17-Apr-2026+03%3A25%3A37+GMT; uid_tt_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; uid_tt_ss_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; sid_tt_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_ss_tiktokseller=88550a7902dbadb4571676b145078738; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CiFUKeQLbrbRXFnaxRQeHOP________-5u5Y_GrMYQ9o44TDdu9OnOdTJWSMfn3fYhgtNut3lcu8%3D; sid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; msToken=-kBdY9ClUtXa7FivYIVyE067baDRqtsssyYUU5S5ei_0g_4SFP1MGOjEy3Gp22Bq66xl7WziGHNS4ffzWZDd5voexqk2jl6hNMswk0AlcqYqCxHUAFTYSlmKsY9p; _ttp=3CKo60ZAkHMPGpMXKFtLeXc4Fuy; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ATLAS_LANG=vi-VN; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzYyMzUxNzUsIm5iZiI6MTc3NjE0Nzc3NX0.77TkhQpWLEHFvq7f5vSqP6BD9QLbT4vRd4Xj3zQ27n4; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2MjM1MTc1LCJuYmYiOjE3NzYxNDc3NzV9._obccaAxXy7oMUSWkymkiDvZlQC3aryORmIkWCNPQ7o; lang_type=vi; _tea_utm_cache_4068={%22campaign_id%22:1861796447666290}; _tea_utm_cache_1583={%22campaign_id%22:1861796447666290}; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776148830%7C062c4c36a14e813689980b63f47f542b9fcedee51f1232f2e1ac9d84f4faaace; user_oec_info=0a5377f34093451bc7a480d68185846b56042523a8b7f5fe96cd692a719364937aa20003cfc9d6e7df2fcd03e6db31c6e3ed48b7e8df4a6904d2d6cdaab74af000aa41d14a1a3640b8354da0e5a7951e451041abe71a490a3c00000000000000000000504d710d5d42c494520c3690bd2fd9da6ed9b4c963d8f6a6543d36034f1141bc82d8870be7428027dfa549f202a49a84e8b510bae48e0e1886d2f6f20d220104df98b341; odin_tt=3338abfc7be047be00664bf289033e7a3af5f72ec821c638bea0ec261d939fbc939440adcc4b609c3a7c1a957e45be70b87796dc8dc3c2f5707a2cdacf887523; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 4, 9).date()
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
