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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; store-country-sign=MEIEDESPxrvB06qNfYdfUgQg4B_HDrbpLeP9CdIE-YJY6BdaarXL7bKKGGjiLJPICDYEEGliolUtTngR1bGAFS67g74; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; sid_guard_ads=7dce85a5ece0119837a9e49193d82721%7C1782788240%7C259199%7CFri%2C+03-Jul-2026+02%3A57%3A19+GMT; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; s_v_web_id=verify_mr20y3tc_H2U7jwlJ_Kusd_4M8B_9ILr_fllXt6cz14UH; FPGSID=1.1783073627.1783073627.G-BZBQ2QHQSP.6uxyFdKQec6NTlGcnjHNwQ; _ttp=3FyeB5r9slSOXhiiWRfY0B6dR34.tt.1; FPLC=AUXMx5UjBx3mHhkYSeArq05PH5Wv7dxmE%2FtKZdh%2BqLVRDeHYgu3i9hiYEzF9n3yQN1CSt%2BIjdy7oSuTL3SoCHNu6NYK1GRtN%2B2WXc539Pcv8HzeoKimTMSxB3udVhA%3D%3D; msToken=brJObm49PATRdEqkGkCoEYcoKo8kh6k0FP3Z2ZX2YWeHA2DuD0ScVCls4oFr4ihdvHV41eTd9jo5AeXcF6nPorAMkxJ_zLlt0p_wKCXPB3qm2qSZ9qmzvCNfWfsO4aCirNHdXE0rylvvx4I=; ttcsid=1783073627631::jrbao3mJGW0ECOf8U0u1.33.1783073643613.0::1.-2436.0::15980.11.953.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1783073627630::-Cq0pu6W1VEjDcVpFzuf.33.1783073643613.1; sso_uid_tt_ads=a66e0ea72c94c80ea7ab7d791e840213b8069b9e334e571c834bae98e89e92dc; sso_uid_tt_ss_ads=a66e0ea72c94c80ea7ab7d791e840213b8069b9e334e571c834bae98e89e92dc; sso_user_ads=2a9addf46435434c4117d71fbbf1aa5a; sso_user_ss_ads=2a9addf46435434c4117d71fbbf1aa5a; sid_ucp_sso_v1_ads=1.0.1-KDA2ZjlmNjlkMDFkNTM2NmM5MjI5YzllOGNkMjE3ZDEwMDhhMTgyNjgKIgiViLWa9-_oimkQ7pae0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhOWFkZGY0NjQzNTQzNGM0MTE3ZDcxZmJiZjFhYTVhMk4KIE1y2UpFZJVudzvCmLe2-zuW3cMH1oKPBXYTsa8FxdQlEiC6F7OubJbWp0d9FOkYn-pYXrQ0XX30vF-dKCyzji8PXBgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDA2ZjlmNjlkMDFkNTM2NmM5MjI5YzllOGNkMjE3ZDEwMDhhMTgyNjgKIgiViLWa9-_oimkQ7pae0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhOWFkZGY0NjQzNTQzNGM0MTE3ZDcxZmJiZjFhYTVhMk4KIE1y2UpFZJVudzvCmLe2-zuW3cMH1oKPBXYTsa8FxdQlEiC6F7OubJbWp0d9FOkYn-pYXrQ0XX30vF-dKCyzji8PXBgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1783073627$o32$g1$t1783073646$j41$l0$h1463057170; odin_tt=18f5ed999d7473ffb8ae969eef939c78080d28e5fb8422276c3004e2b6cce85f5023bfcbdc0b1496035b85bb15df179ac97abdc16f7fdf1fe492d4e4043ee089; sid_guard_tiktokseller=b549083481cf182036b40a675b5a8171%7C1783073646%7C259200%7CMon%2C+06-Jul-2026+10%3A14%3A06+GMT; uid_tt_tiktokseller=6c4459d0945324456cbd8fd7beabed0e6e30813929065c47d3a79a19830f52bd; uid_tt_ss_tiktokseller=6c4459d0945324456cbd8fd7beabed0e6e30813929065c47d3a79a19830f52bd; sid_tt_tiktokseller=b549083481cf182036b40a675b5a8171; sessionid_tiktokseller=b549083481cf182036b40a675b5a8171; sessionid_ss_tiktokseller=b549083481cf182036b40a675b5a8171; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CtUkINIHPGCA2tApnW1qBcf________-8E5H1Avu1CrFS01_AFWVGVk1858Q0j17gJaAFruNfHTw%3D; sid_ucp_v1_tiktokseller=1.0.1-KDBiZmY1MzA0NjJhODk2Mjk5ODIzMzM5OGZkOWQ4NTM5OGNjZTNhYzIKHAiViLWa9-_oimkQ7pae0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiNTQ5MDgzNDgxY2YxODIwMzZiNDBhNjc1YjVhODE3MTJOCiCyZsbnrH9NvyZngt6niWGYKBMTnVYRNAu6_PJHjXTQGxIgQnTg5ftjl4mRePo4sQdXEXirfdiQySqBFk_myHbi7tcYBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDBiZmY1MzA0NjJhODk2Mjk5ODIzMzM5OGZkOWQ4NTM5OGNjZTNhYzIKHAiViLWa9-_oimkQ7pae0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiNTQ5MDgzNDgxY2YxODIwMzZiNDBhNjc1YjVhODE3MTJOCiCyZsbnrH9NvyZngt6niWGYKBMTnVYRNAu6_PJHjXTQGxIgQnTg5ftjl4mRePo4sQdXEXirfdiQySqBFk_myHbi7tcYBCIGdGlrdG9r; msToken=0hHOzgkXmbz6g55HAxiZ-GZSA5zRXvPAYqYJmOlJC4JElCLGahDAMkRDI55ACoc4N0L587HzophO9xPfJxC-oSKk3jGbzKsoBXI7Lq8DDHkGFNjDyzxpRYlsGDnEIpBupA7VomiWgfXK-zQ=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODMxNjAwNDgsIm5iZiI6MTc4MzA3MjY0OH0.cpvn3XfxUvqBKlz1ql542Hjv0Ln8H7zNkl__d2i2-e0; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgzMTYwMDQ4LCJuYmYiOjE3ODMwNzI2NDh9.ToAjuka7L_OxS1RVUekHES_UUE8d1VUXDk6dAnSOh_I; oec_lucifer=AQEBAHeBbdUL92nSHhwFOjNMCbnuoSj4J+b3B9lhkpX5VWQZSTQBSYiNZQ9RyBLnfjUYAuRYLidttz90NmKc6aU33KsqDdFqgkcQ; user_oec_info=0a530111e02f4df8999041ca3ec77be852d1607abd7eafeda2d42c7c4892ab5739d4b32b0ebad94abf6caaa206c35bfaad8e1016cac72cd038c754b66b4a1d0d2bfb6265372032247505a7faa79df033f4c1b564a81a490a3c00000000000000000000509d25e5fff232cd6ba72089cb834cda5746dc123b68e6e8c02833e0b2a7806a6eaa918cb167b2645102f3143b46105563751091e9950e1886d2f6f20d22010443e3a604; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1783073655%7Ce2e717fa9bc768447d972a542b6a8b88b303b9dd8abd64ef366b52dd108d4414; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9"
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
