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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; _tea_utm_cache_4068={%22campaign_id%22:1862554723618386}; _tea_utm_cache_1583={%22campaign_id%22:1862554723618386}; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; store-country-sign=MEIEDO5KLJRmeh6W9Z8S1gQgSzdsPPkSTvvfXBJqCB3zQioV1kfbj2f5DuEHuwMVocAEEIHA67KjdYGFweVp83lKwXk; s_v_web_id=verify_mouv2vn7_Voqw6mk5_BtUj_4jVa_B9qv_o22aXjlnG330; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzgyMTI0MTUsIm5iZiI6MTc3ODEyNTAxNX0.73Rjlx5ivmXvWZM1G-l1gg_WlGa1AnFWPMMwqUTUhAQ; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc4MjEyNDE1LCJuYmYiOjE3NzgxMjUwMTV9.WbEPGuvYPylk8FemJripKrvy9lK_bb_AddjToxLojJg; _ttp=3DLBadUXKCMRtambp2WLFdprgNu.tt.1; FPLC=lqdK2ANr5fuh%2Bq5mTRFLuIaIvZXVRqZxQzmEBWwBQVYzqMYvDlf7fj51D5o48h5PO9MxCCKM1qDuPnMHoQdMaj%2B7b4x%2FGx2rSSrF0LuNMsWndFL0ZQtM9upEed6w5g%3D%3D; FPGSID=1.1778133389.1778133830.G-BZBQ2QHQSP.IkIUXJc9qyFriw_E5NA9lw; ttcsid=1778133390337::uSAarNC4UFNEPQ3HMU3y.10.1778133866112.0::1.447157.448910::475766.21.704.477::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1778133390336::w_P0W9-M6PXiQYB4JLe2.10.1778133866112.1; sso_uid_tt_ads=ae013706c5160f77301dd894bbb558daca6b656f3e62abcee34447e8881003a1; sso_uid_tt_ss_ads=ae013706c5160f77301dd894bbb558daca6b656f3e62abcee34447e8881003a1; sso_user_ads=bb7216e24a7fdc03b459927a4289e1c4; sso_user_ss_ads=bb7216e24a7fdc03b459927a4289e1c4; sid_ucp_sso_v1_ads=1.0.1-KGM5NWQ5NTE2NGE0NGQ5Y2ZjNzY5YmI5MTM4OTUwNzRhZmNlOTMyMjAKIgiUiN7g9dSegGkQ7NbwzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiBiYjcyMTZlMjRhN2ZkYzAzYjQ1OTkyN2E0Mjg5ZTFjNDJOCiBmaFSZ11Q7GFZ46rakDpzmZpxrWAWwtJxeMb7iYp-lVRIgLHECh2lqU2CSbR-RE8jUAATQx2WQsa5pIk4S7GZ8PkAYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGM5NWQ5NTE2NGE0NGQ5Y2ZjNzY5YmI5MTM4OTUwNzRhZmNlOTMyMjAKIgiUiN7g9dSegGkQ7NbwzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiBiYjcyMTZlMjRhN2ZkYzAzYjQ1OTkyN2E0Mjg5ZTFjNDJOCiBmaFSZ11Q7GFZ46rakDpzmZpxrWAWwtJxeMb7iYp-lVRIgLHECh2lqU2CSbR-RE8jUAATQx2WQsa5pIk4S7GZ8PkAYBSIGdGlrdG9r; sid_guard_tiktokseller=8ebd32d1c079a70001ed26c51c8ef395%7C1778133868%7C259200%7CSun%2C+10-May-2026+06%3A04%3A28+GMT; uid_tt_tiktokseller=7e0d95eb69d0f49d31aeae43082b2a03383b0f4e6d8e88f85506692dbf396df3; uid_tt_ss_tiktokseller=7e0d95eb69d0f49d31aeae43082b2a03383b0f4e6d8e88f85506692dbf396df3; sid_tt_tiktokseller=8ebd32d1c079a70001ed26c51c8ef395; sessionid_tiktokseller=8ebd32d1c079a70001ed26c51c8ef395; sessionid_ss_tiktokseller=8ebd32d1c079a70001ed26c51c8ef395; tt_session_tlb_tag_tiktokseller=sttt%7C4%7Cjr0y0cB5pwAB7SbFHI7zlf________-rRKCBx_5SiIviU7uhRAmgH_5qWCpXAB2sywo0oPdrUOs%3D; sid_ucp_v1_tiktokseller=1.0.1-KGI1ODVhYmJlNzdkYjMyYzMwMWJlNDhlMzQ3YjRjNjg3YmRlOWE3YjkKHAiUiN7g9dSegGkQ7NbwzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ZWJkMzJkMWMwNzlhNzAwMDFlZDI2YzUxYzhlZjM5NTJOCiBUgHJPQ0lpAJ1I5NDapVMR0viO6jOl_NqghuXCQ6a9PxIgdnB6ykjEsuWyf9gC3qbpVhMv-cHXXFJrIDztZ3iOcEkYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGI1ODVhYmJlNzdkYjMyYzMwMWJlNDhlMzQ3YjRjNjg3YmRlOWE3YjkKHAiUiN7g9dSegGkQ7NbwzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ZWJkMzJkMWMwNzlhNzAwMDFlZDI2YzUxYzhlZjM5NTJOCiBUgHJPQ0lpAJ1I5NDapVMR0viO6jOl_NqghuXCQ6a9PxIgdnB6ykjEsuWyf9gC3qbpVhMv-cHXXFJrIDztZ3iOcEkYAiIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1778133390$o10$g1$t1778133869$j20$l0$h1158633095; lang_type=vi; msToken=FbTHMHCEynDVgGrN5fvsJx9Q9O4clCcaBn61HgNVyDxCOx4OPfarTJDdSTAf0FCIOSC7VVhe2hAnQlpNNQND06izuJcSQQkfyRazpSrIJELKEhde0VYBGcekg--Yu90jLVu6f3tQ; msToken=FbTHMHCEynDVgGrN5fvsJx9Q9O4clCcaBn61HgNVyDxCOx4OPfarTJDdSTAf0FCIOSC7VVhe2hAnQlpNNQND06izuJcSQQkfyRazpSrIJELKEhde0VYBGcekg--Yu90jLVu6f3tQ; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1778133949%7Cfad2c2ed68bf3658332cdf0e85b93a137498090887842b0b0bbaa180a0e307dd; user_oec_info=0a536935fc18a9f8989e75c5e3827ce731007958024392627132fb451aa7cfeec309cb4b423c552086775e931adaa4fec0f5b55546007c0181be43a8c2cbda5bfa0e44082ab70a184d04fcd208b2b39f60013af0851a490a3c0000000000000000000050646ba8faadbcb5620d1dbfe242542af8787cbe6fb1e6b0c0518571e263297b7d44bcfd751480ca76198ee1144f42ea2d5f10c7e5900e1886d2f6f20d2201048ea9bfe1; odin_tt=a76e8ab37e58fc585867ed45c95dfa696b25a5fcc9653228e035264ee4399a4156752cec76d878dc50fa12392a3f166cbb64aa12bc671416dc1902e5717dcaf5; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
