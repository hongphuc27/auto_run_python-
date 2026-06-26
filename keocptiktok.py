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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; sid_guard_ads=e4b6187eabde1fe83f9a57de8e97b848%7C1779865206%7C259199%7CSat%2C+30-May-2026+07%3A00%3A05+GMT; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-sign=MEIEDLcaVFMNPJn5xf6ZMAQg-A-jYgq_pCZx4CR3_zfQNgWDydl8nod6lsQ_GJyoEIQEELXym9Z8EbwX5_FKsT5XGYk; store-country-code=vn; store-country-code-src=uid; _m4b_theme_=new; gs_seller_type_for_report=pop; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODIzMDU1NDYsIm5iZiI6MTc4MjIxODE0Nn0.gvo_kedzSRxjs8o0Pncd85g2J3L_B9ofHAz4VbL8jW0; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgyMzA1NTQ3LCJuYmYiOjE3ODIyMTgxNDd9.vmgIFAzRS_d0_S63pOO9qt_R32wjelHYMFX4DB9OeaU; s_v_web_id=verify_mqrefr2s_W0sqnvou_5gso_4Z1L_98ZW_5X1Wh31deXu4; _ttp=3FI4kgAM8T5pBpXv3gTZw41YKig.tt.1; FPGSID=1.1782264794.1782264794.G-BZBQ2QHQSP.6SQTQTbDwe1nu06Pfrc8dQ; FPLC=IzyAGyZx%2BJPOunH2LZkyM2g%2Fl7RQ7qvymHLaFkwM2x9KdS1TNRcaUHQMYTWRlJWWnldANxhAQPsWaGBGLlUF8L9PGAlpgs8svRLZfMmKr1obWVdvNuO9VKK8VEL1xg%3D%3D; msToken=8Z4cP8R45FclsQhd-zppNEghUeH5kf87GxtzxqHMDJ9XN54WWkkHhzyKhwdrXdAjdMqz2gxDHWyTwpQeCS_3YybOUvZHdLcackQOWPdQFKUCtNJ3hGtdQpB-aqVy_P1uSzm_CFg=; ttcsid=1782264790752::Dvi3INtL6uPTaa3vx-n7.29.1782264813643.0::1.-5338.0::22889.10.943.571::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1782264790752::ekviyJibt1tWvf-o0MFH.29.1782264813643.1; sso_uid_tt_ads=c149119b0cd04a094e57b0c6cefd03de1117e51b3f8b79c93e2d36436a783299; sso_uid_tt_ss_ads=c149119b0cd04a094e57b0c6cefd03de1117e51b3f8b79c93e2d36436a783299; sso_user_ads=16ee3ad9431b78a549bc0fdec4ae150b; sso_user_ss_ads=16ee3ad9431b78a549bc0fdec4ae150b; sid_ucp_sso_v1_ads=1.0.1-KDUzZjg5Zjg2MTNlMjU1NmMzNDc1MzE0NGY5N2JlZWJiZDg2NzgxN2YKIgiViLWa9-_oimkQ9efs0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAxNmVlM2FkOTQzMWI3OGE1NDliYzBmZGVjNGFlMTUwYjJOCiBYKP8-NXq0AdfwT_7RlZPxlE996mTrpYNEbI_zyT8gRxIgfv2grXZ2BKrR36Jpm94jcqrCoKlAKPveIqHZCwqpTbwYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDUzZjg5Zjg2MTNlMjU1NmMzNDc1MzE0NGY5N2JlZWJiZDg2NzgxN2YKIgiViLWa9-_oimkQ9efs0QYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiAxNmVlM2FkOTQzMWI3OGE1NDliYzBmZGVjNGFlMTUwYjJOCiBYKP8-NXq0AdfwT_7RlZPxlE996mTrpYNEbI_zyT8gRxIgfv2grXZ2BKrR36Jpm94jcqrCoKlAKPveIqHZCwqpTbwYASIGdGlrdG9r; sid_guard_tiktokseller=800f155a447c44f3880daba446359b17%7C1782264821%7C259200%7CSat%2C+27-Jun-2026+01%3A33%3A41+GMT; uid_tt_tiktokseller=8010052edaaa2f6bedaa9d06b1252615b080d8ca7afa917b3becdc3e6944d14a; uid_tt_ss_tiktokseller=8010052edaaa2f6bedaa9d06b1252615b080d8ca7afa917b3becdc3e6944d14a; sid_tt_tiktokseller=800f155a447c44f3880daba446359b17; sessionid_tiktokseller=800f155a447c44f3880daba446359b17; sessionid_ss_tiktokseller=800f155a447c44f3880daba446359b17; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CgA8VWkR8RPOIDaukRjWbF__________YpKqedE__daNeW0q0OkFj6R8tQAR4gG4iCW8SV3Wx_WI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDk2ZDE4OWE1ZmZiZjU1YmVjMjlmYWZiMTNlODE0NjMwMjFiNGEzZDEKHAiViLWa9-_oimkQ9efs0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MDBmMTU1YTQ0N2M0NGYzODgwZGFiYTQ0NjM1OWIxNzJOCiA-mptb3kRPa35HYR0-qhGr5H6TiYwd8xCDgxEFS6BOyhIgqzhwiUrr5yu-g1FVwuBp-XjuSGcH_sYfYBe-o3fCHesYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDk2ZDE4OWE1ZmZiZjU1YmVjMjlmYWZiMTNlODE0NjMwMjFiNGEzZDEKHAiViLWa9-_oimkQ9efs0QYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MDBmMTU1YTQ0N2M0NGYzODgwZGFiYTQ0NjM1OWIxNzJOCiA-mptb3kRPa35HYR0-qhGr5H6TiYwd8xCDgxEFS6BOyhIgqzhwiUrr5yu-g1FVwuBp-XjuSGcH_sYfYBe-o3fCHesYAiIGdGlrdG9r; msToken=0qGXuhBJSkuhCIJtEMFx6BcbFFF9p2l_PwYdpZ9LGFDA8PxZtIB1rSK2SkUNfsUGTP84Rs5yXaQ--wz8siQZA_LdlLAyaxn0R8Kti9B1V_H55QWVpKHQ_oTCeP2YMHkPUkp8xJI=; _ga_BZBQ2QHQSP=GS2.1.s1782264791$o29$g1$t1782264819$j32$l0$h454299774; oec_lucifer=AQEBAMg2AAfv3iaR1r0nosp74LN89t5IvRujf2yMsHpExIONSLlfeZGAzf0ea8XV1sW7brnaQ1L4JFs/UaLkAhisCwKG03MkeQ==; lang_type=vi; odin_tt=8aba10458a2f8fa5288bcad2bd735e69621baa34e39ec22d2c7a5753fc4620f463b79dc2ef5df98d101ff7ac676064069b364ee98fae4a0967e8351094c9a324; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1782264848%7Cc623096e6cef9c3ed309c10ed7c62db44e935339f691657efc94b4033216a432; user_oec_info=0a530696085d51858626ca1e80234f2fad64b56719a31f9019fec6ec3b9fe789b59b8693038c2ac18db876cfa252129f4462292eeb4d981eb51eeb4165cd8e523cca2e9af20fc39c25714b8d508582d8714f383c301a490a3c0000000000000000000050948c98be887783e84e8ef4233b39ab07c6a85c40412d08708dbd869d38384cf330834f991cf636d8068bba87ee6d5fbbf410a081950e1886d2f6f20d220104c3768350; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

today = datetime.today().date()
# today = datetime(2026, 6, 23).date()
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
