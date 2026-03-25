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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; part=stable; ac_csrftoken=8cd79cf1c74c45ccac78a07fc4a75c3e; pre_country=VN; app_id_unified_seller_env=4068; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0OTk5MjksIm5iZiI6MTc3NDQxMjUyOX0.eW9QIEO9DnGvlEaYJr_-8px2yVO-1LIgsgQ0Zxjs88k; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDk5OTI5LCJuYmYiOjE3NzQ0MTI1Mjl9.CDgMVWF2QU9q6eaOY_V3sWKcqo5TVcTML0WNkWZG-4M; _tea_utm_cache_4068={%22campaign_id%22:1860078570106065}; _tea_utm_cache_1583={%22campaign_id%22:1860078570106065}; FPLC=BKQ4ZRO1JwKeSGEvYCIImnFJsAUf8LTgQYMDKobiSim%2Fe51dBsyZ8n6CEJuFKylmMr%2FrJgvhks%2FPnGYnRXeJNgRqwR5O1KAT62R2ANLcsb5ggjEYBLlMu7P8p877Gg%3D%3D; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774435900719::0fHO-vrCqBgVRbGDlbk1.5.1774436089399.1; sid_guard_ads=9aa97d4ee3699fd68e07cc81035b8592%7C1774436090%7C259200%7CSat%2C+28-Mar-2026+10%3A54%3A50+GMT; uid_tt_ads=767a13768391ca11693594a32dc6df81cb848e1caf075f876ac63aff9de4e07b; uid_tt_ss_ads=767a13768391ca11693594a32dc6df81cb848e1caf075f876ac63aff9de4e07b; sid_tt_ads=9aa97d4ee3699fd68e07cc81035b8592; sessionid_ads=9aa97d4ee3699fd68e07cc81035b8592; sessionid_ss_ads=9aa97d4ee3699fd68e07cc81035b8592; sid_ucp_v1_ads=1.0.1-KGY4OWU3ODQ3ZTgxYjg2ZTJhNmE0MDY5OTgyODU4ZDQwOWM4NzI2YTAKHAiQiKz8zcvg22kQ-v2OzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDlhYTk3ZDRlZTM2OTlmZDY4ZTA3Y2M4MTAzNWI4NTkyMk4KIE_8QOvBy70BabZJr7gJAFodO1hi0jfazIGTU0KUGWTGEiD7j0gBXuxuAfilZc2gQNxpvg2Ed83SjmJOcUkut-wMMBgDIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KGY4OWU3ODQ3ZTgxYjg2ZTJhNmE0MDY5OTgyODU4ZDQwOWM4NzI2YTAKHAiQiKz8zcvg22kQ-v2OzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDlhYTk3ZDRlZTM2OTlmZDY4ZTA3Y2M4MTAzNWI4NTkyMk4KIE_8QOvBy70BabZJr7gJAFodO1hi0jfazIGTU0KUGWTGEiD7j0gBXuxuAfilZc2gQNxpvg2Ed83SjmJOcUkut-wMMBgDIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1774435922$o1$g1$t1774436092$j60$l0$h1896886948; _ga_Y2RSHPPW88=GS2.1.s1774435900$o6$g1$t1774436092$j60$l0$h1953927926; tt_session_tlb_tag_ads=sttt%7C5%7C40e0lu76dpIa2kj1hvdM3v_________FImYpDDl683DVLvTT-53pxoI39xR54X0htEbr27H_VFM%3D; s_v_web_id=verify_mn67o7pu_1jUM0uPm_yK5t_4gzi_BG6w_HCqYkDvqbGF9; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; FPGSID=1.1774453576.1774453576.G-BZBQ2QHQSP.Nsc-D4J5M6T1rZ3gyH0Q4Q; ttcsid=1774453289316::NF5U3uXHmdfcwUGZ_u1d.17.1774453592016.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; d_ticket_ads=cb73b14fab6aad9b8691e11bc72347f87856e; sso_uid_tt_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_uid_tt_ss_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_user_ads=9217a37eb4efc556ee17f0cf0366319c; sso_user_ss_ads=9217a37eb4efc556ee17f0cf0366319c; sid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774453594%7C259199%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CNl44rHhyf0iNxN90xYbMZ_________-_HNHIjdbW97XZvKbwDEBA_EjSBeseaOvDCI7tcnCGdfk%3D; sid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; msToken=sWnUP8HMdxTg2hewBrgPEtNsGe5RqB0HLIB-H68ZLmNgbup56ed9GThyIZRUpqTNMfivYDDcsbgz3MiyPxMLdEGGwd3Ax1e2BGci6vRhwCgBi3VcGC-oSmcyurzFNVB8pefP2WbK; msToken=OUPIchh3V3x_UVTuN9F5hXXGwCX9aXmvxrcNEVHwz21bN00-1D8JX9jUnuqyvwwzJr6ZIZKdEvUJyMHHAqnPUTmK8soJ0ncgpf8XbjM1lWsJ9pUQ-sx3g2BDyBWM; lang_type=vi; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774453932%7Ccad789fabc7bc99797399e35999c0e8b322b48c2feed1e9a385afa851ad5100e; odin_tt=a1bba16a87b80ee26ce58c0245f8862595f2412be142e59bb28557ba116afb6b7b7e133ad02c1db60e694797ecad4970fc7b0ba73afc34981508c566f99bf5de; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a53c02843473492ed6d659be9c4bd15e84adaf8c570b3969a66fcca37f3ee087c0166f57b23c944a08378e1577c2820bbfba8d8c2516301e2603c835f12a244d856aece58b7bc9e9bf76127f3498dfb0e02c2b9501a490a3c000000000000000000005039621330e6033bb570828975be2aa6364e4a37f34412e59262c31150074ea5b705ec630a3fb299f6ab31740fbf700a4fdb10d9878d0e1886d2f6f20d22010457ec31bb"
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
