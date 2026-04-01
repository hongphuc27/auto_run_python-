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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; sid_guard_ads=f899825006ec49bd881b561b656a5f98%7C1774835615%7C259200%7CThu%2C+02-Apr-2026+01%3A53%3A35+GMT; uid_tt_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; uid_tt_ss_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; sid_tt_ads=f899825006ec49bd881b561b656a5f98; sessionid_ads=f899825006ec49bd881b561b656a5f98; sessionid_ss_ads=f899825006ec49bd881b561b656a5f98; sid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1774835513$o2$g1$t1774835616$j60$l0$h508160841; _ga_Y2RSHPPW88=GS2.1.s1774835513$o7$g1$t1774835616$j60$l0$h125334612; app_id_unified_seller_env=4068; _tea_utm_cache_4068={%22campaign_id%22:1860078489790722}; _tea_utm_cache_1583={%22campaign_id%22:1860078489790722}; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C5%7CeTB90XcQMeEWy9TpSLHjnv________-kOZQ-gfgJATLaewZc-YsmvK5DhYfQ5UVl0R61D5_45uM%3D; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; FPLC=l8qoyvg4VjVFU1tOPj0gmuiSND1kTlxTodp1MQOBrL0jlctxtuNFn9V7Qcajzwn9cwxatuvhRF9mykb8sZ2R5zGoREVii4pNco8dROlihoswcccpDqhsySnjwirkEw%3D%3D; store-country-sign=MEIEDF3Y0G9QIYyWmWmpkgQgwnfEpBduknwure1rT6lMk36oyvzKBDm9Zs4QeJJuMfEEEObOAhw6z3nxkvba-wp00Ik; ttcsid=1774941273302::U1-Fjv6__OEQU9nEFqmB.19.1774941295280.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774941273302::MgCW38qV0Mk0BEBfoLXk.14.1774941295280.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; sso_uid_tt_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_uid_tt_ss_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_user_ads=59dc308787ebd574297f1aa3d66152e3; sso_user_ss_ads=59dc308787ebd574297f1aa3d66152e3; sid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774941273$o15$g1$t1774941297$j36$l0$h1797522778; sid_guard_tiktokseller=3823c1a4e22721a66f3de0976fc4861e%7C1774941298%7C259199%7CFri%2C+03-Apr-2026+07%3A14%3A57+GMT; uid_tt_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; uid_tt_ss_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; sid_tt_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_ss_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; tt_session_tlb_tag_tiktokseller=sttt%7C1%7COCPBpOInIaZvPeCXb8SGHv_________LHeMVYk5Fl2CDKeV0khBmhOnBNjaI8VV2xOKbs0lgOlE%3D; sid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; msToken=HrEDPzQEv6l5S_fFD2w1NHsTbZE42n3i7L07XRsIlgULiJSxKunkMUgahrls8WFMr5cMU7xtG76j8Gs8RMnXoBrTjjmYEDnIay6vYHJhANlxzhij97bbpG3dMe4Zqw==; msToken=HrEDPzQEv6l5S_fFD2w1NHsTbZE42n3i7L07XRsIlgULiJSxKunkMUgahrls8WFMr5cMU7xtG76j8Gs8RMnXoBrTjjmYEDnIay6vYHJhANlxzhij97bbpG3dMe4Zqw==; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzUwOTM5ODUsIm5iZiI6MTc3NTAwNjU4NX0.TA1yB0wTOWvJkTG8GFc-VgeQdjkt6XxNolxWn8iRf-M; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1MDkzOTg2LCJuYmYiOjE3NzUwMDY1ODZ9.w0sqVC9CWaJdH6NEktnNSpVpEUS14u0Z7zgYFk3L-Z0; s_v_web_id=verify_mnfdordi_FNfC9biW_HTdH_48VD_9wrW_8KgsVjSqTlXX; lang_type=vi; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1775007626%7Ca6aa48ae0902d7af086f6b022cee6c46fd7dda54d6ec6732a603fc8350a3cd77; odin_tt=cc4065e4390bdb0bc771f1c7a8217294388f1402aef16ff27a7824e3c1346b6cb0427f8748103d1ddf293d9436759a70ee3fad47d8b0247dbdebe10b3ee10a7e; user_oec_info=0a53625a9fda2090ae69cb85e136962333b0d019db7ecf1440e6ee738504c27d0dac002ad66208fb25605c95cdee8491ba79e9e2abb0c90fc8e0efc15774755b3d691bfc9d59c7c96832550908776e8731188953d31a490a3c00000000000000000000503fb20d00617928e1ce9f0f4930fea0c5af352ecfd100e63c3b8b65d3929fbda23ebe6029218d5739ee2b00e850c0b894b410ffcd8d0e1886d2f6f20d2201048c3832f1; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
