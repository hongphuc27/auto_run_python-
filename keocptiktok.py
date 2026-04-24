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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; ATLAS_LANG=vi-VN; _tea_utm_cache_4068={%22campaign_id%22:1862554752900321}; _tea_utm_cache_1583={%22campaign_id%22:1862554752900321}; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI.tt.1; uid_tt_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; uid_tt_ss_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; sid_tt_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_ss_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; uid_tt_ads=e3bc43581cb194c430ed109d02655cd34e916fbcd08f304a371cb735191c81d7; uid_tt_ss_ads=e3bc43581cb194c430ed109d02655cd34e916fbcd08f304a371cb735191c81d7; sid_tt_ads=28c9e866d0a9f385ee340f4869f64f3d; sessionid_ads=28c9e866d0a9f385ee340f4869f64f3d; sessionid_ss_ads=28c9e866d0a9f385ee340f4869f64f3d; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzcwMTY2MjQsIm5iZiI6MTc3NjkyOTIyNH0.f28hQ3yYl_6_3M7TWN-BolwSlReD_0ob0yxiSPehk3g; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3MDE2NjI0LCJuYmYiOjE3NzY5MjkyMjR9.w4dpd_AbQ-v9FfK18xouZ7ZV1JvxWmTPbcLdmFHOei0; s_v_web_id=verify_mob6dl5t_Cha0B6iv_Nq0g_4CfI_ABMu_8b5P093fWxGq; _m4b_theme_=new; sid_guard_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27%7C1776930278%7C259200%7CSun%2C+26-Apr-2026+07%3A44%3A38+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C4%7Cq03uHjMyIYq84OhzSustJ__________PlmloK-AJApbeS5SoqwEzey9NKrlqBEf6JUI3pOWf5VA%3D; sid_ucp_v1_tiktokseller=1.0.1-KDgyYTNmYTBlMzlmZDUxMDkzYzA1NmVlY2M1MDlmYmQ3OTlmODU1Y2IKHAiUiN7g9dSegGkQ5punzwYY5B8gDDgBQOsHSAQQAxoCbXkiIGFiNGRlZTFlMzMzMjIxOGFiY2UwZTg3MzRhZWIyZDI3Mk4KILNAam_gmfmoyOaYARCd0iIqmwWjhURtD4EYcaBcr5UUEiD1bV1H675cOzeqSG4OWsxZZXt9BO9zds44C-Mi8pFFmRgBIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDgyYTNmYTBlMzlmZDUxMDkzYzA1NmVlY2M1MDlmYmQ3OTlmODU1Y2IKHAiUiN7g9dSegGkQ5punzwYY5B8gDDgBQOsHSAQQAxoCbXkiIGFiNGRlZTFlMzMzMjIxOGFiY2UwZTg3MzRhZWIyZDI3Mk4KILNAam_gmfmoyOaYARCd0iIqmwWjhURtD4EYcaBcr5UUEiD1bV1H675cOzeqSG4OWsxZZXt9BO9zds44C-Mi8pFFmRgBIgZ0aWt0b2s; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; FPLC=X9wniw9cLWp94thlXyb95VurcDwrtROu6I8C8mBxHJt%2BLHFA0c8Dx5LgPcqUnPm6Sb62rxNfp%2FGiwJzwwB7dBmBNzVIS6%2FptdYMTu0nSJSA1cuwd4IMTraCsTRKItQ%3D%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1776930284079::Texc8anno72WVi_7JRwm.6.1776930284345.0; ttcsid=1776930284080::NMMVyHRVRIwZL1zSHIBG.6.1776930284345.0::1.-7212.0::1330.1.656.282::0.0.0; passport_fe_beating_status=true; _ga_BZBQ2QHQSP=GS2.1.s1776930282$o6$g0$t1776930285$j57$l0$h271757427; lang_type=vi; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; tt_session_tlb_tag_ads=sttt%7C2%7CKMnoZtCp84XuNA9IafZPPf_________TLuK4qpuGrGftjfxYhWVnDAvXk8dKPzCluDsAbYjte-Q%3D; sid_ucp_v1_ads=1.0.1-KDk3Mzc0MTViZTM2MGJkNjRhNjc3ZGNkMWEwNDAwMzQyNzg1ZGU5YjQKHAiUiN7g9dSegGkQ5qinzwYYrwwgDDgBQOsHSAQQAxoDc2cxIiAyOGM5ZTg2NmQwYTlmMzg1ZWUzNDBmNDg2OWY2NGYzZDJOCiBi7A2RCy2VKRxCi17BCvtiXHWHnaC9wx_uY0Na13Wc1xIg2KupdwG3Q03D__dg2DYGCeIpjmqZ7NyHsZrf5en-eHIYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDk3Mzc0MTViZTM2MGJkNjRhNjc3ZGNkMWEwNDAwMzQyNzg1ZGU5YjQKHAiUiN7g9dSegGkQ5qinzwYYrwwgDDgBQOsHSAQQAxoDc2cxIiAyOGM5ZTg2NmQwYTlmMzg1ZWUzNDBmNDg2OWY2NGYzZDJOCiBi7A2RCy2VKRxCi17BCvtiXHWHnaC9wx_uY0Na13Wc1xIg2KupdwG3Q03D__dg2DYGCeIpjmqZ7NyHsZrf5en-eHIYASIGdGlrdG9r; pre_country=VN; part=stable; msToken=LCbCR_2xF7TuEsQP0wH7aHy52xgsEXX0FEKw8vio8ZudaQdNi83dA7Tztx17-id_7iRonQwp8HUsuyf8mMpUSwmxk2whnczXZGZgnmq6jjakweSanE_ARnSBvlsdk4-JqpSAnJw=; msToken=LCbCR_2xF7TuEsQP0wH7aHy52xgsEXX0FEKw8vio8ZudaQdNi83dA7Tztx17-id_7iRonQwp8HUsuyf8mMpUSwmxk2whnczXZGZgnmq6jjakweSanE_ARnSBvlsdk4-JqpSAnJw=; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1777001928%7C81c9335901e619392e9c26936a7cef020a5d4a355d7a43400c0557b5cf8a0d74; user_oec_info=0a5382d4c1fe60a6a5e6cf8a9dd0cbe3fc804adfc44081a991f57709a8264d857a68075e8894791cc80f55e570cc51fbe3b8626c3cc3cab2298a3dd91b5d7c99d372c92670e27842d518d6dd1dd86c344623be3b741a490a3c000000000000000000005056b5cdf26f1cc9e323d5106c9b65d341ebcbf73eb9d12a9fabe6766e97085c4cc87a8b53663000308f96ee6f84cd61511c10a6d28f0e1886d2f6f20d220104895b9ff7; odin_tt=b26ee64eb062244b4b4b68dfa9527926a2c373c6b9290e0a328d28856502eab9ef14621aef803ce7b12ab8c710bf95c540d7ad4223303c797b96138887c7c0d5; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
