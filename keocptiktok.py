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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; store-country-sign=MEIEDJrrjhF0UYMmze-hJgQgtBXrbFgn9zT0pHHPFgkCcqws_UQrNtf6_XZkOdHY6yoEEFyR-HbK12kNipK0qziPw98; gd_random=eyJwZXJjZW50IjowLjk2ODQ3NDc1MTEwMzQzMjQsIm1hdGNoIjpmYWxzZX0=.WWV75FWV/E2F/cKeE1tdE8mlX0Hj8PB93g/4Rtqd8Vs=; oec_lucifer=AQEBADBIu1sV/SjNnMtQg3Adlmr0QZMQTFtoURhK32L+ZoBNGxaxIh1+ITUW0jiW0GalAEjEYtFusX4yFxNgRPm3bxukJTk2tQ==; _ttp=3DbaseAXzK8bb2t9zERxXQChg3F.tt.1; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzkxOTIxNzMsIm5iZiI6MTc3OTEwNDc3M30.PBqzXND0vzW5uWY6RYHheI5tmpTJb1SPbi3FvyXkIrc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc5MTkyMTczLCJuYmYiOjE3NzkxMDQ3NzN9.HfshO1imzRD4PTdjtl4xArrLfeOeAGMu0tdrVPH-TMI; s_v_web_id=verify_mpbz4m2e_0mehIFKd_G2KL_4FjV_83Q2_ibmNT5Gy2MjU; FPGSID=1.1779155299.1779155299.G-BZBQ2QHQSP.mnOcvx4xSKjLvFPg3W9c-w; FPLC=STN0MUcxyL6%2Bhsl%2FizDFEhIdJ3wWtOc03Qq5FafQ4LsfJnBj5aQoigCg3hRWG4vuAGatpJTp1Ufnt4XEVr99OUFCgIt8eRiB1g9aWnUI7ZDSruusnsCJTPQ%2F55jyMw%3D%3D; msToken=NgFJaMXUAU7s3LmIevep_AQi9Ydho3T4QXq84mfz8iKvwPtwvv3LBr2dxISzXRoi__DIiADZBcs8NY0OUsg-a-8-RfmBj2JgdkTtC2nIrm9DY2hDsX1r9tGJslN4-w==; ttcsid_CMSS13RC77U1PJEFQUB0=1779155299523::nut_ekXU-ktVyg61cB6w.14.1779155323800.1; ttcsid=1779155299524::vx8yqQV-tTp6RqHWHQl_.14.1779155323800.0::1.-2098.0::27950.15.921.575::0.0.0; sso_uid_tt_ads=f03ffd8ecccaf2627a6fc57842fea0f482058715d08905ffb82ebe5ffcf63b6c; sso_uid_tt_ss_ads=f03ffd8ecccaf2627a6fc57842fea0f482058715d08905ffb82ebe5ffcf63b6c; sso_user_ads=79a9b813a02ec674523295229febe2a9; sso_user_ss_ads=79a9b813a02ec674523295229febe2a9; sid_ucp_sso_v1_ads=1.0.1-KDg0ZTYxNTk0OWYxNzQ2OTBhNDkzZGQyYzg5MzFlYzZkMzNmYzM5NTMKIgiUiN7g9dSegGkQgoOv0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc5YTliODEzYTAyZWM2NzQ1MjMyOTUyMjlmZWJlMmE5Mk4KIKjNqxrIuusUpM4t3yOOkmBOzYKKSwAd_e831PGECJQpEiBZt3Q6o6JUQ33gY1HIa52S6hQPFHD8j3jHDTLhTSdT1RgFIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDg0ZTYxNTk0OWYxNzQ2OTBhNDkzZGQyYzg5MzFlYzZkMzNmYzM5NTMKIgiUiN7g9dSegGkQgoOv0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc5YTliODEzYTAyZWM2NzQ1MjMyOTUyMjlmZWJlMmE5Mk4KIKjNqxrIuusUpM4t3yOOkmBOzYKKSwAd_e831PGECJQpEiBZt3Q6o6JUQ33gY1HIa52S6hQPFHD8j3jHDTLhTSdT1RgFIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1779155299$o14$g1$t1779155329$j30$l0$h1346921811; sid_guard_tiktokseller=b22b01da469027583f8f34cdb77f5607%7C1779155330%7C259200%7CFri%2C+22-May-2026+01%3A48%3A50+GMT; uid_tt_tiktokseller=0349811c3ed530860b8f5413f8c5a8f5461e25c01565a5293a9c1ecba9351bbc; uid_tt_ss_tiktokseller=0349811c3ed530860b8f5413f8c5a8f5461e25c01565a5293a9c1ecba9351bbc; sid_tt_tiktokseller=b22b01da469027583f8f34cdb77f5607; sessionid_tiktokseller=b22b01da469027583f8f34cdb77f5607; sessionid_ss_tiktokseller=b22b01da469027583f8f34cdb77f5607; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CsisB2kaQJ1g_jzTNt39WB__________ZZGviVxJTyJlX3q9clCotwH2AT-C5163zh73Q2-2vaxc%3D; sid_ucp_v1_tiktokseller=1.0.1-KGY3NjdiMTlkNDE5N2MxMjU4NzU2ZWYyNTJmMTNjZWI5ZjA4YmNjZmQKHAiUiN7g9dSegGkQgoOv0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJiMDFkYTQ2OTAyNzU4M2Y4ZjM0Y2RiNzdmNTYwNzJOCiCwbcMLhRl6O88QWpad-nvvs4azNJDGaeTVSgDDid_vIRIgtf_7omffq05SihZLLv7OFH4ha4Y7JSvPpxxumKWMg10YBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGY3NjdiMTlkNDE5N2MxMjU4NzU2ZWYyNTJmMTNjZWI5ZjA4YmNjZmQKHAiUiN7g9dSegGkQgoOv0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJiMDFkYTQ2OTAyNzU4M2Y4ZjM0Y2RiNzdmNTYwNzJOCiCwbcMLhRl6O88QWpad-nvvs4azNJDGaeTVSgDDid_vIRIgtf_7omffq05SihZLLv7OFH4ha4Y7JSvPpxxumKWMg10YBCIGdGlrdG9r; msToken=d9I4OuomOTsKnG5dHI1SWSQRiubozZrmUuaxQogCXzTRa_WPuJaQrORZyncCVUNXuY1KlX8CvwrKlARQIAKasb_o9jZoX44DB6NTroLGry4bOTm7Ce5Vr8oChkZPGg==; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1779155348%7Cdd01cf0f57e8830ced6fe3e001a55bacc91354d3de62d0e5e3cb3d20a284164f; odin_tt=628978e29efa4be31b7f03d883112d6f1d14f7e9c1719ddf2c771ea8b7b3451b1d7835be5d2789cf8d0c688d7b97480ae8e47fb66e9b902a8399c3c056939b18; user_oec_info=0a539a48cafa3d97246f35bbfa11df60fe8260faa34334b6c522f6da09ebc851adafbcfc7fa6a3c9c777874c9912e5c758308903df5d6406dc908754c341f7f3bd5a5e0cf244995d580dade12e69c10221f282fbed1a490a3c00000000000000000000506f42ed9e3182dcff2c3489ae5a5dfe315d4c891e3dc8c52f8cd7f15ffcb088de86145ac5f6ff2e3bf703b9ebf6b409f6de10a8ea910e1886d2f6f20d220104c9f29a8c; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
