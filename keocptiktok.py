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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; _tea_utm_cache_4068={%22campaign_id%22:1860078489790722}; _tea_utm_cache_1583={%22campaign_id%22:1860078489790722}; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; sid_guard_ads=fba832a9ef3bf1c561723599114d963c%7C1775009054%7C191443%7CFri%2C+03-Apr-2026+07%3A14%3A57+GMT; _ga_HV1FL86553=GS2.1.s1775009054$o3$g0$t1775009054$j60$l0$h647018127; _ga_Y2RSHPPW88=GS2.1.s1775009054$o8$g1$t1775009054$j60$l0$h566538698; s_v_web_id=verify_mniqiaj2_GsGBLtZy_AJ99_45tG_8aaw_bA136sORIYrd; store-country-sign=MEIEDKm9OTjpJX0F_q_2OQQgdc5LHZLuZPjyHbu97JVFpxq4xnpmVDowX4ncnPjUc5sEEMD9Ifw7rK9aIfR54bWjA6Q; FPGSID=1.1775210521.1775210521.G-BZBQ2QHQSP.8-anX_cWY-64v3GgJgXsGg; FPLC=fAERuVxGc%2B0FAd4VJWd99RGUE0Hf%2BcW0B7BbDhimy%2FVyihoDUO3TWwepVIZUT%2FoFZfHzwPHUaJHukpaEmtTKyr6Aeq9SdxhNwozvgB8eFlyoY9cQg5R0Pgi5L1IIDw%3D%3D; msToken=vp30Qq7JAxA4wiqfnjqDKSkt1EvCnbc77rxsg2vhcV8W_lI2Euuu2vQ8bZWTzhIb4xtE6U_eQvEukfVKwbywlOJBjB3xrIyXd6qSWptXQ2Oj9PfK1eh7WhbxOf1diA==; ttcsid=1775210520574::lpxhLY8mU4xaHlASeJaU.20.1775210535138.0::1.-2143.0::14563.15.1055.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1775210520573::joAYUSMCvqlYWRCVOp4B.15.1775210535138.1; sso_uid_tt_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_uid_tt_ss_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_user_ads=c103e6271279d24e6c4061d2c2613fbe; sso_user_ss_ads=c103e6271279d24e6c4061d2c2613fbe; sid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1775210520$o16$g1$t1775210537$j43$l0$h2111997584; sid_guard_tiktokseller=b22af622952384da29ce9cbafafbc01c%7C1775210537%7C259200%7CMon%2C+06-Apr-2026+10%3A02%3A17+GMT; uid_tt_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; uid_tt_ss_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; sid_tt_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_ss_tiktokseller=b22af622952384da29ce9cbafafbc01c; tt_session_tlb_tag_tiktokseller=sttt%7C1%7Csir2IpUjhNopzpy6-vvAHP_________WVUUl8r7EX6ao2IxjeENINn0gg-ndpOR0lwM-f4ToXN4%3D; sid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; msToken=yPW74SxtrQ1t3Wfynf2mFCF0h6gc7rXx9db7opXzDHQ0ZmllsuO6kyCV3dIhtzAsdCgF5jpfbHldy81rkErlTg7aAwm7fKyGxQbfVGS9cfox2njwfLH655pHlX9aZwkm86vGTEk9; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ATLAS_LANG=vi-VN; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzUyOTcxNjQsIm5iZiI6MTc3NTIwOTc2NH0.nEkwP_oXW9BagVMrf7K0ssbA14Z7bu0FAkxYKd0796k; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1Mjk3MTY0LCJuYmYiOjE3NzUyMDk3NjR9.8g3fTKsIZaEduMvYvfITbAjYQvcdKdUAZI_0-2du6Ro; lang_type=vi; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1775210798%7C826fa367dff1424751021fe8d1064044e46ad2cd36c61ae069bae5b698f7b766; odin_tt=3bfbef690e686df29959e8a0db425110416e0316554f741f16e246d30219b43c9b276c803e972f659f7a2765d1687f760c1691dd29f7a53245cf9f5a182ec674; user_oec_info=0a53e88b2c14cd99cee02b2557573b6f161ab4bcd9f8fc9d7de1318e0455d1cddffd33282a52212d8576840d4210eed59e7b39f5d8a9db07e7f23228f41392f8cc49d529b473caa84cc11895b70f06642845e7d0e81a490a3c000000000000000000005042a43afe0d068f19293dc1a84685861747e711d4bd328beb70d71ba8da1a7408503a245ba31203afe3954b1aebbcdf6cb0109bea8d0e1886d2f6f20d2201044476ef6b; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 3, 31).date()
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
