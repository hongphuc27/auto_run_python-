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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; _tea_utm_cache_4068={%22campaign_id%22:1862554723618386}; _tea_utm_cache_1583={%22campaign_id%22:1862554723618386}; store-country-sign=MEIEDN6tHm8E3erEnsr0CwQgcOzcrTS46Zr5AjQQLW0yLCPFkZyW5_6xcdmW8YoDG2UEEDMpTD1D3MsaHHDr8e039fA; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzc5NDc3NzksIm5iZiI6MTc3Nzg2MDM3OX0.nEeqh1klrzof1WWWZqltGjYhWBbee87jhuT2SXtWplw; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc3OTQ3Nzc5LCJuYmYiOjE3Nzc4NjAzNzl9.GWWVCSv6cKGXGcTNt3o3EzKFU17_qxWotGjBND3-_So; s_v_web_id=verify_moqkrewj_maFHFXKx_VgmB_4Gen_8dqm_W0V2wTMJzSQq; FPGSID=1.1777874157.1777874157.G-BZBQ2QHQSP.i2MHXbeMBYi7Y7txbplbZA; _ttp=3D6vj7Za20V8AAwValP8dhN6J6A.tt.1; FPLC=LS1b8hoYDuJgmCuzQtKxbFYU%2FqYvGigs71N0bqskrXeh1%2F3X5W62cJbD%2BcYMuLed3rDHaBEnUeEVn6uiWbIUsPXoURCDtVKLqSOncOWg48Qe2QDvPi4wyfsfIomCWA%3D%3D; msToken=JDH5sgQH6UVkk5HrfC-8v5MJBjaNDJz-OSnMzrPn9krdT4yMcSW_qs_liC_QKngLZuRDZ_8zNr9oRr7W4nHLlAS9vq1xFU7L8DMJdzhOBJSK3jaFT5GDNe2pv9h_q8V2IV3Civs=; ttcsid_CMSS13RC77U1PJEFQUB0=1777874155386::sJBQNu6p920sDZAaweY8.9.1777874165395.1; ttcsid=1777874155386::eHDozZKc-ZoC_LBnxbcw.9.1777874165393.0::1.-4547.0::22437.14.1025.572::0.0.0; sso_uid_tt_ads=bc3a77113e2770c2879634aef6e1f0a8fb43ea3110ee7a8cd154d61c9043cd3a; sso_uid_tt_ss_ads=bc3a77113e2770c2879634aef6e1f0a8fb43ea3110ee7a8cd154d61c9043cd3a; sso_user_ads=1cf57a13b29727526696e5ffa4f0ab10; sso_user_ss_ads=1cf57a13b29727526696e5ffa4f0ab10; sid_ucp_sso_v1_ads=1.0.1-KDNiMDQ4ZmFhYTgyYWM0NGFmZTgyMTVhYjRmZTRiMmQxZjA1MTM1YjAKIgiUiN7g9dSegGkQhergzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAxY2Y1N2ExM2IyOTcyNzUyNjY5NmU1ZmZhNGYwYWIxMDJOCiAVcSSmFYPn17nx0OygdDvMh9vz546PHLpWWRUJE6vasBIgFGAlwIhIuRoqltdLSm8VbBCrBMvzoQYzU-NOfLLwPDAYBCIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDNiMDQ4ZmFhYTgyYWM0NGFmZTgyMTVhYjRmZTRiMmQxZjA1MTM1YjAKIgiUiN7g9dSegGkQhergzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAxY2Y1N2ExM2IyOTcyNzUyNjY5NmU1ZmZhNGYwYWIxMDJOCiAVcSSmFYPn17nx0OygdDvMh9vz546PHLpWWRUJE6vasBIgFGAlwIhIuRoqltdLSm8VbBCrBMvzoQYzU-NOfLLwPDAYBCIGdGlrdG9r; sid_guard_tiktokseller=36ba69de8b3de6f2b9681f5c748fbc5e%7C1777874182%7C259199%7CThu%2C+07-May-2026+05%3A56%3A21+GMT; uid_tt_tiktokseller=95893838a3d87fe81520a41fa40c961d41cfcecde1dc024ea543afecf469b263; uid_tt_ss_tiktokseller=95893838a3d87fe81520a41fa40c961d41cfcecde1dc024ea543afecf469b263; sid_tt_tiktokseller=36ba69de8b3de6f2b9681f5c748fbc5e; sessionid_tiktokseller=36ba69de8b3de6f2b9681f5c748fbc5e; sessionid_ss_tiktokseller=36ba69de8b3de6f2b9681f5c748fbc5e; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CNrpp3os95vK5aB9cdI-8Xv________-z6M246ZSSZ6UwZpfcbHhQ17xVccMvn4FLydYZt0bzxqk%3D; sid_ucp_v1_tiktokseller=1.0.1-KDY3ZmMwYjllZGIzMWQ4YTEzNWJmN2ViYmZkYTgxODg4OTI0OGNlZmMKHAiUiN7g9dSegGkQhurgzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNmJhNjlkZThiM2RlNmYyYjk2ODFmNWM3NDhmYmM1ZTJOCiCA7Fq9KxEJt9va2FDURTF6wcMXZL8uglmH4mYaJRyFJBIgQH1hnx964JXY_QR5gC-_htYmHfK1KP5MPyzGgZ1SPo4YAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDY3ZmMwYjllZGIzMWQ4YTEzNWJmN2ViYmZkYTgxODg4OTI0OGNlZmMKHAiUiN7g9dSegGkQhurgzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzNmJhNjlkZThiM2RlNmYyYjk2ODFmNWM3NDhmYmM1ZTJOCiCA7Fq9KxEJt9va2FDURTF6wcMXZL8uglmH4mYaJRyFJBIgQH1hnx964JXY_QR5gC-_htYmHfK1KP5MPyzGgZ1SPo4YAiIGdGlrdG9r; msToken=Duob6-fmtHDj4r4WUBJ6SZCy5xNso28F7XocnAyYqRZam5pYCnMinAlRZ8PH0mykVigOX-crqLhQTdghE2GonNwdzpIgrnDiOQ_CpIBv3CTpMwL6K1yTxmcRIA6eGun85gVYQqc=; _ga_BZBQ2QHQSP=GS2.1.s1777874155$o9$g1$t1777874182$j33$l0$h138980728; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1777874233%7C424f966311f6ca21a19ec2fa9fb98dd01c998a2968b2fc1b3628e9ded6b4fa89; odin_tt=0dfe6cf31e62fd48ab3fce975da8fe95837266fa2a0302e05f3908b236c4781ba9b3b2f798e05875e64b9fef001ae0eef45e8172fc5fe6201a5a3ce18e860d64; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a530b1eea722cf535e3cb9efa683cf1a26cbd91a7934a71b0d2c21e475aeab4cae79c9615bb76d91d802c1a2cac5a08755e5937768faf812a0bc91fea1b1d2438064b6a72fc41991d730831f950c7cc9f908d1b181a490a3c00000000000000000000506146c56fcc20ea80d102768d407ea7a3b7a545ac7b1abb554c9d923a1785ecb85343b57ca36716bc3ff447234e48203c8010a7c5900e1886d2f6f20d2201044794790f"
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
