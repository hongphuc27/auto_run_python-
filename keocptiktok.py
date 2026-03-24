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
    "aadvid": "7615175616880279560"
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://seller-vn.tiktok.com",
    "referer": "https://seller-vn.tiktok.com/ads-creation/dashboard?shop_region=VN&type=live",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "x-csrftoken": "pICMUok8m661IP2kRQPf2HpsAkfm4daB",
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; sid_guard_ads=3847856ccedf5c11a59bc9d9ef5b272a%7C1773990826%7C185179%7CSun%2C+22-Mar-2026+10%3A40%3A05+GMT; _ga_HV1FL86553=GS2.1.s1773990828$o6$g1$t1773990828$j60$l0$h1985046469; _ga_Y2RSHPPW88=GS2.1.s1773990828$o5$g1$t1773990828$j60$l0$h1334679047; ttcsid_C97F14JC77U63IDI7U40=1773990829246::_nFQcqJTBcfmOF3E39x_.4.1773990829644.0; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; _tea_utm_cache_4068={%22campaign_id%22:1860078489790722}; _tea_utm_cache_1583={%22campaign_id%22:1860078489790722}; s_v_web_id=verify_mn3yplzl_NmrMbtnk_CGjJ_4qG4_9buj_kIgtxE1gYQex; lang_type=vi; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0MDgyMDQsIm5iZiI6MTc3NDMyMDgwNH0.Ni3ylC1N93XB_6cC1yvjZ6hav86bMjM1eb2CmQe-YBY; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDA4MjA0LCJuYmYiOjE3NzQzMjA4MDR9.nH0dlZnU1VzI820IllswWL71as-I7odm_2wGOkpRim0; FPGSID=1.1774328558.1774328558.G-BZBQ2QHQSP.EUh_DO54-XR5ZGCmzpqADQ; FPLC=xW%2BDoGjXAUETgRL4EvSRGsLe%2FivaqHtlUHUXjste25I4lwEC5PZ4OxOdRA2i9MEnLoXZ7CUik5WWKs81x%2F0Lh6UUoYNvSmPgJNTviYErWq%2BiNogtoPl9ya0spk8pag%3D%3D; msToken=qzPbrqm-8uJeodLC28_TzbbeLI_2Mv3mCs6gp0gkttWZhvnUUZuwccj2lEN3R7x7svuxXDk4fvo2R_wt0xZacwiJHk23a-Y8pP3a7O72pqGQrQnij60utMUu_nwmfmQWS9yItls=; ttcsid=1774328558618::vo01iITGmxLRpspmTycH.15.1774328581572.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774328558618::yaGX_UIJG4xKpNNcedlG.12.1774328581572.1; sso_uid_tt_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_uid_tt_ss_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_user_ads=28923db567af453c44258468669f6c64; sso_user_ss_ads=28923db567af453c44258468669f6c64; sid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774328558$o13$g1$t1774328584$j34$l0$h182365910; sid_guard_tiktokseller=89fc588555d8994266248c8b5c2794f7%7C1774328583%7C259200%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; uid_tt_ss_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; sid_tt_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_ss_tiktokseller=89fc588555d8994266248c8b5c2794f7; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CifxYhVXYmUJmJIyLXCeU9__________6r4d-Iyrp0mENXHEHHfoMCN3BQVGhnLrugOXdiVOSilI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; msToken=ChNONKAOs7uWOy0HnbY6FDHIh4-43xPDuioMzhzsvNWCG9y8vsfO18nKEVRdHVkhcP2xlHmP9eDcl5JMPd6P2aJkaF3UFp6hBY3cWBWvDQaHrA14JbwaIO9bD_TAU1qfs2OrkQE=; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774328634%7C1fbb02ec573f5b1256437a02213a3db6d19046dd1280d0d3f8dbb3afd489add5; odin_tt=96aa975a6976a70262686e2048270260fbc51849f533c2a2e14e24e4c66225f550f3d4f3f0e5e93f3cb9ed83bc82bfa1609e9225d3754d32cac5c2adaf7a7a65; user_oec_info=0a5342a2c8eb4d63666925a097bacb4ae66aa429cce9e0d5b259f85522609e9c45764e746a84718cb1dff5864f055060a29dc4f013af9023436fb986d7b22929c6850572fb91ff2051ff21b5284825d65942c3a6481a490a3c000000000000000000005037c01f85da4592a761b6dbaeddbeb2d12e9d02641f0d2dab9ce70a9fdc30e7fa6d5dd355090bb7e1d9e2ea918f8ce50e1e10edf58c0e1886d2f6f20d2201047cbf4831; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
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
