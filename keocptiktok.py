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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; app_id_unified_seller_env=4068; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774435900719::0fHO-vrCqBgVRbGDlbk1.5.1774436089399.1; _ga_HV1FL86553=GS2.1.s1774435922$o1$g1$t1774436092$j60$l0$h1896886948; _ga_Y2RSHPPW88=GS2.1.s1774435900$o6$g1$t1774436092$j60$l0$h1953927926; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; ttcsid=1774453289316::NF5U3uXHmdfcwUGZ_u1d.17.1774453592016.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; d_ticket_ads=cb73b14fab6aad9b8691e11bc72347f87856e; sso_uid_tt_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_uid_tt_ss_ads=d3cda387cfd4d95934370c98b0f0c5ca01085a0039cd5e78e7b050805cf874ab; sso_user_ads=9217a37eb4efc556ee17f0cf0366319c; sso_user_ss_ads=9217a37eb4efc556ee17f0cf0366319c; sid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGU3NTUyYWM3Y2RlNTg4ZjYxZjUzNzI5NzE1ZDA5YWJhZTE4NmZhYmMKIgiUiN7g9dSegGkQ2YaQzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiA5MjE3YTM3ZWI0ZWZjNTU2ZWUxN2YwY2YwMzY2MzE5YzJOCiDfCqj-RG8tAU0Wxlbehj92sWEJr9faZRwTwuWF3Suf6BIgIVy8OUMWDjMRjqrM2E8VadL3yM5JvsgjN_xLd913ot4YBSIGdGlrdG9r; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774453594%7C259199%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CNl44rHhyf0iNxN90xYbMZ_________-_HNHIjdbW97XZvKbwDEBA_EjSBeseaOvDCI7tcnCGdfk%3D; sid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDQzZmEzNjRmMDhkYTZjNWVmNmZmMDIxYTI0NmQ2ZGYzNDBkNDMxMGYKHAiUiN7g9dSegGkQ2oaQzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNjVlMzhhYzc4NzI3ZjQ4OGRjNGRmNzRjNTg2Y2M2NzJOCiAhgBMuMF7V8m2ZULsLCBQT75fMm_G0osFSKBv8sprCJRIguxGu1ZPGk-csAHSHCv_NC1dN38NZh1APz02t-1wrEfEYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; sid_guard_ads=ee8cec8c67ab555af96a102ed407e9df%7C1774493563%7C219230%7CSat%2C+28-Mar-2026+15%3A46%3A33+GMT; uid_tt_ads=286561c0e6be80e0c24bc365ea55921968182b72a86f809f65173281faabb454; uid_tt_ss_ads=286561c0e6be80e0c24bc365ea55921968182b72a86f809f65173281faabb454; sid_tt_ads=ee8cec8c67ab555af96a102ed407e9df; sessionid_ads=ee8cec8c67ab555af96a102ed407e9df; sessionid_ss_ads=ee8cec8c67ab555af96a102ed407e9df; tt_session_tlb_tag_ads=sttt%7C4%7C7ozsjGerVVr5ahAu1Afp3__________QHs122uATT314lLQrv_LbSNOu6GrPg8FvR6FfPfvUIv4%3D; sid_ucp_v1_ads=1.0.1-KGVhYzhhZjhkZDNmOTIzZmU0Y2ZkNWJiZjBiZTQwNmQ4NmVlNjhkYWEKHAiUiN7g9dSegGkQ-76SzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiBlZThjZWM4YzY3YWI1NTVhZjk2YTEwMmVkNDA3ZTlkZjJOCiDuOA2C1xsNbwwEOT1RbcFcgtumLuaCmj2EnXSJ-0RtpBIgSCkOXq1tMpio6mE6whVtIcuBrmtE910igoQ9C3rATXwYAyIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGVhYzhhZjhkZDNmOTIzZmU0Y2ZkNWJiZjBiZTQwNmQ4NmVlNjhkYWEKHAiUiN7g9dSegGkQ-76SzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiBlZThjZWM4YzY3YWI1NTVhZjk2YTEwMmVkNDA3ZTlkZjJOCiDuOA2C1xsNbwwEOT1RbcFcgtumLuaCmj2EnXSJ-0RtpBIgSCkOXq1tMpio6mE6whVtIcuBrmtE910igoQ9C3rATXwYAyIGdGlrdG9r; _tea_utm_cache_4068={%22campaign_id%22:1860078489790722}; _tea_utm_cache_1583={%22campaign_id%22:1860078489790722}; s_v_web_id=verify_mn8r19cj_zVBF7dhC_kEh1_40Ls_9zab_tpHvNXEFYl3T; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ2OTMxNDEsIm5iZiI6MTc3NDYwNTc0MX0.NErhJnrvxPbOTjCACDH2RIr6syO5IWMkvQJ-cIxkwRc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NjkzMTQxLCJuYmYiOjE3NzQ2MDU3NDF9.N-tRMC6HiOxyej9_4jDje-B6hbuTneQsD1xbRRJZ3_A; msToken=U2XIFaNF4td0503O8Y9QX6_XaJi3xbNTbLbA6iBqu0sBLVSuEOlr_XzSoXgunUo625J34I39QBZKP8MJicnYuGilMh661DtKZQbBEpJCMUM5Ez1bLoAtx3ZhAnnROA==; msToken=DxzYYkIk_fqi7hrgH3R_XSskFza15XL-bhwi4XSyVlh_XlkMtJ1ATDz8Wz-rhn2F_CAjcT-gZ2tlwU26kckc4aJqHdKmdWAmjx6ipNPZsPuTDiMOt4FwdiSGVKnq; lang_type=vi; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774607213%7C2707c0455e48857f42b36e6a58ad9dfdae6a507d9d8deef5b8e8ea9b8d67418f; odin_tt=eaa34234b0d20f68f67ae9f4f6838add7c4e35141d2535423f5cb4b81a5a435e7f70ec553e25ab618a54887d9e5760db63b9af22b79c2bf2d26f0d0eb921c01c; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a5358a8fd8e8aab071bdb01784eada95921761054a45e6230918cd617511e57a05fb2d4e83b021ae9e8d18d6b897d718fbceef90138c9030ba2d0e1ac49e1738cbb25d0f94c8b11dc9ccc684592d2b5b616eedd3b1a490a3c00000000000000000000503b2d531acf93088290310bf67cdb3fad4184e8b38301cf846972659ac79f2b913795ff2ed8ef83abd5044eca6011829b3010c69b8d0e1886d2f6f20d220104539df3a2"
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
