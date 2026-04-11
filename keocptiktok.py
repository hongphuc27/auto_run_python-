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
    "cookie": "ATLAS_LANG=vi-VN; tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1775786598%7C259200%7CMon%2C+13-Apr-2026+02%3A03%3A18+GMT; tt_session_tlb_tag_ads=sttt%7C5%7CiHhd0_VTVzXYzAY5HVf8hf________-0ghDCbQrwzh7hNCTIT8HFefZid-1VMg7jav4VToF9qic%3D; sid_ucp_v1_ads=1.0.1-KDI1OGM2ZDVkOTk5YjUyOWJjZjlmNDk3YWZiYjNmOGU5ZjE5ZDFlNmMKHAiUiN7g9dSegGkQ5rThzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiBmtBWN1T6nH2a3rGJmeI61TSnNx-3heBnlPOW2eSjSHRIgZzkgqW0soruk2R_Us6wdGvXRDeEmHEu_0hxLpWheS7gYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDI1OGM2ZDVkOTk5YjUyOWJjZjlmNDk3YWZiYjNmOGU5ZjE5ZDFlNmMKHAiUiN7g9dSegGkQ5rThzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiBmtBWN1T6nH2a3rGJmeI61TSnNx-3heBnlPOW2eSjSHRIgZzkgqW0soruk2R_Us6wdGvXRDeEmHEu_0hxLpWheS7gYASIGdGlrdG9r; tta_attr_id=0.1775786611.7626945287505608722; FPLC=Wz4TDDvnKTkHMzaGm8IOmv60%2B4Zd1G1iPqp3Ma16KwRt9%2FeYUXLpLXbDIc6ISo2QJ6WvWl5niUwPcgvgR2AFfeCTYU4zuNXroZ9rHtdkdwAMpYRlQXyHVg0z465OLg%3D%3D; sso_uid_tt_ads=ff34ea72d144a8ab560b931be2247508692db542b3339b5bab8e6125fc4e0bda; sso_uid_tt_ss_ads=ff34ea72d144a8ab560b931be2247508692db542b3339b5bab8e6125fc4e0bda; sso_user_ads=3d6e2213c7af48d27758e54b562df619; sso_user_ss_ads=3d6e2213c7af48d27758e54b562df619; sid_ucp_sso_v1_ads=1.0.1-KGRkODczZDIwZTFjMzA5NjE2NGQzYzQwYzM3Mzg0MWU2MDhhYjY5ZmMKIgiUiN7g9dSegGkQpMzmzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDNkNmUyMjEzYzdhZjQ4ZDI3NzU4ZTU0YjU2MmRmNjE5Mk4KIAbeAOJHDN_EGkd4zTzKgH61tJigFzFdY82DCX1ISMkaEiDQXJ3UhpMgGzTFOYBgner5_WPBk0Ae60aGQWjA9DFH9xgDIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGRkODczZDIwZTFjMzA5NjE2NGQzYzQwYzM3Mzg0MWU2MDhhYjY5ZmMKIgiUiN7g9dSegGkQpMzmzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDNkNmUyMjEzYzdhZjQ4ZDI3NzU4ZTU0YjU2MmRmNjE5Mk4KIAbeAOJHDN_EGkd4zTzKgH61tJigFzFdY82DCX1ISMkaEiDQXJ3UhpMgGzTFOYBgner5_WPBk0Ae60aGQWjA9DFH9xgDIgZ0aWt0b2s; uid_tt_tiktokseller=a6cd6e85d6db6a6d9851ce2d7c09a3ed019b36657d01dcf47f0b135a281e246d; uid_tt_ss_tiktokseller=a6cd6e85d6db6a6d9851ce2d7c09a3ed019b36657d01dcf47f0b135a281e246d; sid_tt_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; sessionid_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; sessionid_ss_tiktokseller=9f3c144d072747e9ff4b1d209c97b044; _m4b_theme_=new; gs_seller_type_for_report=pop; sid_guard_tiktokseller=9f3c144d072747e9ff4b1d209c97b044%7C1775871565%7C259159%7CTue%2C+14-Apr-2026+01%3A38%3A44+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CnzwUTQcnR-n_Sx0gnJewRP_________UA5xemqo1VyX9R0Fw-ov6eOL-gpNMGhv6ZBE52iJ0990%3D; sid_ucp_v1_tiktokseller=1.0.1-KDA3YTBiZjg4MWQ2N2QzMGI2ZjZlNjk5ODlmOTdiNzNhNWEwZGE4NjcKHAiUiN7g9dSegGkQzczmzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDlmM2MxNDRkMDcyNzQ3ZTlmZjRiMWQyMDljOTdiMDQ0Mk4KIF2DvHw_WX0-05Js_DughCVPPmUJZpDLN7BdfDOyXeLdEiABzK1-Fi1jMXFzZRzbUZ3KjojDqL-7LPBeHlgjukZdzRgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDA3YTBiZjg4MWQ2N2QzMGI2ZjZlNjk5ODlmOTdiNzNhNWEwZGE4NjcKHAiUiN7g9dSegGkQzczmzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDlmM2MxNDRkMDcyNzQ3ZTlmZjRiMWQyMDljOTdiMDQ0Mk4KIF2DvHw_WX0-05Js_DughCVPPmUJZpDLN7BdfDOyXeLdEiABzK1-Fi1jMXFzZRzbUZ3KjojDqL-7LPBeHlgjukZdzRgCIgZ0aWt0b2s; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; ttcsid_CMSS13RC77U1PJEFQUB0=1775871502961::3lotr2RjRIb_eiNajBJf.2.1775871568420.1; msToken=x91LjIV1HGyOPHLKcytrXLqETsCA7MIWh4T-Eauks6md32XHAo2meDfPp2TcZNF07rtzRwUE9LFeDeBeIePo0uHS3oS-_yw24VfVNpsX39soOtrn0PH36M37TkBNwBjHHKkuDmY=; ttcsid=1775871502961::U9BaWkt_NXTozqvztRrG.2.1775871568420.0::1.60090.65120::276108.16.153.458::0.0.0; _ga_BZBQ2QHQSP=GS2.1.s1775871502$o2$g1$t1775871791$j60$l0$h921084537; _tea_utm_cache_4068={%22campaign_id%22:1861796635819025}; _tea_utm_cache_1583={%22campaign_id%22:1861796635819025}; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU5NTkwOTAsIm5iZiI6MTc3NTg3MTY5MH0.5cfGgF4f6JfwvBAc-jCMtOG2OblROEBpj1wHk-b3fhM; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1OTU5MDkwLCJuYmYiOjE3NzU4NzE2OTB9.BzwpMrlH-NF9dabHA4j-LMtQ3rDB1a-_0T_goLllSNI; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; msToken=Rlv1NfK_FKg7jEdwiCKkykXUuN2FOI3cBOYewu-Fz2sYs8-7PWUMxQ7Jgr8V_gGmFI74-dpuFqRGkDaIm5NKukXpn2r5uBycfiEM8Prg_Jd3QIqqNlCLOyJVANL8; s_v_web_id=verify_mntq23kv_z266LV2h_XfHx_4agM_9zkm_8KCyIkeNCaHd; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1775875051%7C3d85715de50619cc5dcc6d072c46323101aec963b8b59f6309c607dc9913b7c3; odin_tt=efdc61b45cc1888177db7bf4896631bd7dada30a523c7c34cfdbac3cd66d0c3eebb762b13f6dd73e6907b32cec5a727e15119503d8bbba7133ccf760b690e6db; user_oec_info=0a5379b83bf524fe2cd2122e99f4f107ae1c9fdf9d391fd35bbe024a8cac2da7da3ddbd06e8263774cba2eb0a0aa6e14e6690add2d94d621e59b15b5b48b15788ca410797f0e231167157bb03d60a0c23be2d367201a490a3c0000000000000000000050495766e10ebdc606b4ab0e3b1aa81f34025a624831f593d2906cde18a029893b8195aa6314b6d5fc8704e611db49fdeb9110ccbf8e0e1886d2f6f20d220104db4941c0; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
