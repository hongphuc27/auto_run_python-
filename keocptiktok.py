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
    "cookie": "tta_attr_id_mirror=0.1777215607.7633082911700287489; passport_csrf_token=63ea3d889eb0d0eeaaab0e5984a30479; passport_csrf_token_default=63ea3d889eb0d0eeaaab0e5984a30479; part=stable; _ga=GA1.1.1438146305.1777215645; FPID=FPID2.2.LXrw%2B1E7zDZp0p8S3fjDTPE8AO0Hrhv8RzQnjs91HHs%3D.1777215645; FPLC=c03a8BVA%2Bzq0hiQ1adNGW%2F%2BCJV1rG%2BuSPivCvWA9CwGcueYzsXvteq%2BMKMDlIIcQsFEM8EzptpnJHl90Ogy6Tbub2UcmMGfaK3SgxhL9GHkD5fQ7wJk8hR8ltAqB6w%3D%3D; _ttp=3CtrHuZIj7cqDujYFAbJjyjs6Q0.tt.1; _tt_enable_cookie=1; FPAU=1.2.811625642.1777215625; _fbp=fb.1.1777215624514.1056617089; _gcl_gs=2.1.k1$i1777215629$u242214610; FPGCLAW=2.1.kCj0KCQjw77bPBhC_ARIsAGAjjV_4-F5ntCxGTCXsacfMRuIXnctwnzcn1GozdajMGcjRjG9BwlznDWQaAqBxEALw_wcB$i1777215626; FPGCLGS=2.1.k1$i1777215609$u242214610; _hjSessionUser_6487441=eyJpZCI6ImI4ODE4MDk0LTFiMGUtNWNlYi05NjAzLTIyNTRlMTdmMDZlMSIsImNyZWF0ZWQiOjE3NzcyMTU2NDgwNDIsImV4aXN0aW5nIjp0cnVlfQ==; _hjSession_6487441=eyJpZCI6ImNmM2M0OGFiLTQyYzMtNGY0My04YWE3LWUzZDlmZTZiYTU1MyIsImMiOjE3NzcyMTU2NDgwNDksInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; d_ticket_ads=0587dc5d429c2fc26b66c405fac4ba2e5f6be; sso_uid_tt_ads=002a6c22337ab95b0736b5b5190ed602a407c7d2bb4f0594edd9acb7784fa1f3; sso_uid_tt_ss_ads=002a6c22337ab95b0736b5b5190ed602a407c7d2bb4f0594edd9acb7784fa1f3; sso_user_ads=c00738e37af3fae9e49aa95bb195c12b; sso_user_ss_ads=c00738e37af3fae9e49aa95bb195c12b; sid_ucp_sso_v1_ads=1.0.1-KDViNzdjYWFmZTAzZGVkNzVlNjk4NWM0YTZiNTc5MWQ5YWJmYjc5ZWEKIAiUiN7g9dSegGkQ19K4zwYYrwwgDDDN9YHIBjgBQOsHEAMaA215MiIgYzAwNzM4ZTM3YWYzZmFlOWU0OWFhOTViYjE5NWMxMmIyTgogutE88JNqM39e1CmigoQwJJot1VA7yHbg9heFsNM0joQSILnP80gbWf5m-1lFQMUNjTYrIvPAD2rTAb_3u9TeANqrGAIiBnRpa3Rvaw; ssid_ucp_sso_v1_ads=1.0.1-KDViNzdjYWFmZTAzZGVkNzVlNjk4NWM0YTZiNTc5MWQ5YWJmYjc5ZWEKIAiUiN7g9dSegGkQ19K4zwYYrwwgDDDN9YHIBjgBQOsHEAMaA215MiIgYzAwNzM4ZTM3YWYzZmFlOWU0OWFhOTViYjE5NWMxMmIyTgogutE88JNqM39e1CmigoQwJJot1VA7yHbg9heFsNM0joQSILnP80gbWf5m-1lFQMUNjTYrIvPAD2rTAb_3u9TeANqrGAIiBnRpa3Rvaw; sid_guard_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9%7C1777215831%7C259200%7CWed%2C+29-Apr-2026+15%3A03%3A51+GMT; uid_tt_ads=87c0fd1e98ea3eec64dc4d5fa01187825ab8fcb6f80e81d23f88c5e7d902161c; uid_tt_ss_ads=87c0fd1e98ea3eec64dc4d5fa01187825ab8fcb6f80e81d23f88c5e7d902161c; sid_tt_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sessionid_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sessionid_ss_ads=044a31bfe1b93cb9c1f92e0ab1e6a3b9; sid_ucp_v1_ads=1.0.1-KGJlYzIwMTQzY2VhOTNlMGEzMWUyNmEyMmYzNWMxZjM1ZWFjOTI2ODIKGgiUiN7g9dSegGkQ19K4zwYYrwwgDDgBQOsHEAMaA215MiIgMDQ0YTMxYmZlMWI5M2NiOWMxZjkyZTBhYjFlNmEzYjkyTgoglE_F24X5WvJRe04rizh8kduLIcIEH4gyjCOj22JNdtESIAFwaeuHM_76P-gkjrxy5XRzqub5EJYbxwVmLkEQoZlNGAIiBnRpa3Rvaw; ssid_ucp_v1_ads=1.0.1-KGJlYzIwMTQzY2VhOTNlMGEzMWUyNmEyMmYzNWMxZjM1ZWFjOTI2ODIKGgiUiN7g9dSegGkQ19K4zwYYrwwgDDgBQOsHEAMaA215MiIgMDQ0YTMxYmZlMWI5M2NiOWMxZjkyZTBhYjFlNmEzYjkyTgoglE_F24X5WvJRe04rizh8kduLIcIEH4gyjCOj22JNdtESIAFwaeuHM_76P-gkjrxy5XRzqub5EJYbxwVmLkEQoZlNGAIiBnRpa3Rvaw; ac_csrftoken=b4ee2928a7c54aef821542be30185d39; ttcsid_C97F14JC77U63IDI7U40=1777215645461::p6h_FDXSDxcKh-09e412.1.1777215853198.1; pre_country=VN; tt_ticket_guard_client_web_domain=2; ATLAS_LANG=vi-VN; _tea_utm_cache_4068={%22ad_id%22:692117978045%2C%22campaign_id%22:21059367265}; s_v_web_id=verify_mofwh3le_P05UsjRG_plFY_4O79_BWKQ_ImJpI5D3J6PC; _tea_utm_cache_345918={%22ad_id%22:692117978045%2C%22campaign_id%22:21059367265}; tt_session_tlb_tag_ads=sttt%7C3%7CwAc443rz-unkmqlbsZXBK__________zzEmxgmagr-hC0N86DlOqhBBPqiFbNFiJHT8p5Ktadlk%3D; _tea_utm_cache_1988={%22ad_id%22:692117978045%2C%22campaign_id%22:21059367265}; uid_tt_tiktokseller=d0c5939df306ac227d194b482ff97d9d6d165915cb75cd182a6b3061a48e4cdd; uid_tt_ss_tiktokseller=d0c5939df306ac227d194b482ff97d9d6d165915cb75cd182a6b3061a48e4cdd; sid_tt_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; sessionid_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; sessionid_ss_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; i18next=vi-VN; _tea_utm_cache_5969={%22ad_id%22:692117978045%2C%22campaign_id%22:21059367265}; pre_country=VN; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzczMDIzMTMsIm5iZiI6MTc3NzIxNDkxM30.zqheM39GUCEsrUJgvhjHmN2x5qMSHEmO5qXHvJ5XGlw; SHOP_ID=7075901688577638662; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3MzAyMzEzLCJuYmYiOjE3NzcyMTQ5MTN9.piskn8aJEeGVxyeiI-vja-W13eHd7CmEb54RAPBBfxA; _m4b_theme_=new; gs_seller_type_for_report=pop; sid_guard_tiktokseller=2b5da1ecd8a45b0999c88a5c34b698bc%7C1777215918%7C259113%7CWed%2C+29-Apr-2026+15%3A03%3A51+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C1%7CK12h7NikWwmZyIpcNLaYvP________-2GgD548kse-F80jfcPXRHfSPkaxdrgAGevv5UM1loe3s%3D; sid_ucp_v1_tiktokseller=1.0.1-KDFlNDExODU5MDg2MGYzZmFmM2MyYWFmNTQ0OTZlNzQ2OWE2ZGZiN2IKHAiUiN7g9dSegGkQrtO4zwYY5B8gDDgBQOsHSAQQAxoCbXkiIDJiNWRhMWVjZDhhNDViMDk5OWM4OGE1YzM0YjY5OGJjMk4KINVOzpV2UrxlqJSPc33uCJT0a1q0Rm9J-IBv48gydozNEiAbHpwDSqe2oEZmBsE9cKk_20f7OVT-HVcJbB5yoRlTDRgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDFlNDExODU5MDg2MGYzZmFmM2MyYWFmNTQ0OTZlNzQ2OWE2ZGZiN2IKHAiUiN7g9dSegGkQrtO4zwYY5B8gDDgBQOsHSAQQAxoCbXkiIDJiNWRhMWVjZDhhNDViMDk5OWM4OGE1YzM0YjY5OGJjMk4KINVOzpV2UrxlqJSPc33uCJT0a1q0Rm9J-IBv48gydozNEiAbHpwDSqe2oEZmBsE9cKk_20f7OVT-HVcJbB5yoRlTDRgCIgZ0aWt0b2s; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; _tt_ticket_crypt_doamin=2; _gcl_aw=GCL.1777215942.Cj0KCQjw77bPBhC_ARIsAGAjjV-zCP_uZ1q-IkAQ8wuQAaqcvgY-cyK5c-8wENwlmIkEkD_O9s7MvNYaAsyuEALw_wcB; FPGSID=1.1777215921.1777215921.G-BZBQ2QHQSP.hXyIXwM6GrUdM3KJie52ow; _gtmeec=e30%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1777215942904::HRsKDlMTJ-tBoRSV9e6D.1.1777215948035.0; passport_fe_beating_status=true; _ga_BZBQ2QHQSP=GS2.1.s1777215942$o1$g0$t1777215948$j54$l0$h2004259644; ttcsid=1777215645466::EcGEr7PYklXKjU6ZetYc.1.1777215853198.0::1.207688.0::226998.14.501.274::409190.17.23; _ga_ER02CH5NW5=GS1.1.1777215879.1.1.1777216131.0.0.186807175; _ga_Y2RSHPPW88=GS2.1.s1777215644$o1$g1$t1777216366$j60$l0$h918202082; _ga_HV1FL86553=GS2.1.s1777215879$o1$g1$t1777216366$j60$l0$h191191230; msToken=NZ1CKBPa5Nm13cFq0z5PkY1UXSRTI7WWhV6wFlPd558F8eOX0fw5KrqOHRuVHJbvXmA4IAbaCo585_WCPN6U2-L1Tdzo8LAajLmxEn2cyu1iRsvA812cguJzOAts6sguKtF1bT4=; msToken=-1nHlX-WA3MGD8qZADGuEjZWDtpxxzz411tMQBh-q-nhZXtOkRrpXplYOTjLR-EgfKEkOm1TWLoNi5s-faAjC9EX85hJw3OrmMSDmbPrJgi1bsj9KsJLfq70DXlf; lang_type=vi; csrftoken=b9l5AcaaAa9T4rbj6MHRJavjEdOo6tKh; ttwid=1%7CYV2NNRyt_v5nurwnWGe9UVmRB3RzTXAmbV6XiQ4T3JM%7C1777216671%7C1ab21cd5036070b04f69ae7f90bc72eddc8bce2aedb5a548c4b0d6e1141dc30b; odin_tt=e0ea2689b708b601adff72bc1e2ad0d274cdb9039016e0560d952c92170748da9303c442d56cf3ae85fbfe3fe14c0ff6015a7233e57d978558b1b6e7fed41955; user_oec_info=0a5366a7066c8fd3cf014904214affb8c283464ecc9194ded52f35a060b6c6045bb1cc7f7103a533250f450483366223716cf7f3d4ce7659fb8468ea8b8757dc58eebad655976be574bedcd533159c0507b783bf7d1a490a3c0000000000000000000050593c183d0f60868cd5a393957b1532128ecf4c69e4d5ef25e192e1fbdee393d9ca979a3c15cab207cfce116938160a5de310b3ef8f0e1886d2f6f20d2201048d1e27c4; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnSnUrME90MVdlZDhCOUUzQm1ucHNvd1pISEFzTVhZc1FhTUNKT2NoUXhHYWhSQU5DQUFSVXIwdmNoNnk1SnR5ZVluU29WOXM5YmprSHJqMGVnZ1ZjeXBaU2ZncWFEUWJYRWlWMEJNMTkwOVp2UkhPWHk0bzlwemc2dk1uY1gzRElsYm44bVk2Slxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVWSzlMM0llc3VTYmNubUowcUZmYlBXNDVCNjQ5SG9JRlhNcVdVbjRLbWcwRzF4SWxkQVROZmRQV2IwUnpsOHVLUGFjNE9yekozRjl3eUpXNS9KbU9pUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkZTdlM5eUhyTGttM0o1aWRLaFgyejF1T1FldVBSNkNCVnpLbGxKK0Nwb05CdGNTSlhRRXpYM1QxbTlFYzVmTGlqMm5PRHE4eWR4ZmNNaVZ1Znlaam9rPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
