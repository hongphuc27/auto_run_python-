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
    "cookie": "ATLAS_LANG=vi-VN; tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; msToken=mxlz1ZPE0QWXW960jO2qs40862-eVzmid3yPUZjNpOpEstztUqtcIKskyq-8ytJ43lHHOl6TP7619zkMcJKfVLe87qks5sqW64kEHwOrBdV7zX_AWKbByUoY8dT1G14Ab1jwdg==; ttcsid_CMSS13RC77U1PJEFQUB0=1775537660817::aRA4G-_EmTu_ToyOpsFu.1.1775537723207.1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; ttcsid=1775537660818::bhcc8U1LpZTuFD306i5E.1.1775537723206.0::1.-18488.0::67734.24.732.495::0.0.0; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; sso_uid_tt_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_uid_tt_ss_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_user_ads=9689c113a5f4913dbb255ae04a102874; sso_user_ss_ads=9689c113a5f4913dbb255ae04a102874; sid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1775537644$o1$g1$t1775537731$j60$l0$h890508184; sid_guard_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2%7C1775537732%7C259200%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; uid_tt_ss_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; sid_tt_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_ss_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C7QbhYA1urHAjAfvQ4Qmfwv__________QPF9zItUF2qAhZaIzMbvfm4jp09mDdlugUUrP303PR0%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1775537770%7C259162%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; tt_session_tlb_tag_ads=sttt%7C2%7CiHhd0_VTVzXYzAY5HVf8hf_________176bHukUfGOuuG88-YidTfBynpHWGzETuNiYJeLzceqc%3D; sid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; msToken=_UGYSwS3AuCLwlIvgzUISPjWan8OESQmWEqf1Yux9yqRh7wRU54k2nXYMPveUDHmE4H31a-JGN-doElmjaWAn05TWfpLmt11AdqLq2pAolrH8FIVJUr-5pzXetYCUyZdXptP3vnlZX5bxoO7y4FuCRg=; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; _tea_utm_cache_4068={%22campaign_id%22:1861795932191937}; _tea_utm_cache_1583={%22campaign_id%22:1861795932191937}; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU4NzI2MzEsIm5iZiI6MTc3NTc4NTIzMX0.guS1n_ccLfd06Gs2UTXS7J0lLWBCf4sG5gVjLbS_4so; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1ODcyNjMxLCJuYmYiOjE3NzU3ODUyMzF9.8GEGa-3BM5-KJ0SUaD4GvJGmyck3ips81s2x6yiwAlA; s_v_web_id=verify_mns99uel_3TgXNgEt_pvAW_44Vw_9Glo_AkI6v7766uXu; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1775786411%7C9a3a77a37b77c8d78631f9e96e08cbb3efad60285f09fd4ca91f5171b9a9060c; odin_tt=571f26e7271a3e3361de3256fb2f74d7c28d4d1890b4bc7e8b66d60973d0d60a6ff33977b7ac739712ac82ac0173b9dcd5390d7071d7d7978f63b5003a6b5569; user_oec_info=0a53f633f1caab819381044bb262e9968379243b4c9e6c97453e881b5c4002e9353b672dc9cbd6cd498e9d0e5afdea42c57c4cdb6119f3a31af6ecc082e2d08834bce9234b8933374b442c3f6c3ab996efb69d1e111a490a3c000000000000000000005048b26008978e7856077172939d524976ebc0b15697c7e0e876fe78eacc53d05cd2a9f6844f86f5922fa6f94353d4994aa6109fb58e0e1886d2f6f20d22010442b59f69; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

# today = datetime.today().date()
today = datetime(2026, 4, 9).date()
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
