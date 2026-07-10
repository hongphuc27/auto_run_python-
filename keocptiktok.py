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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDGn2a3p3E0H2QlTdcAQgQ59vpwlpCePUZm51gAhTgs7hh0kpudj6n18IMjWCnBsEEHCwnzZ3VY8pcUqam-Pg2Fk; FPAU=1.2.1047425659.1783389422; sid_guard_ads=2a17390f694186c6010ee8f8f7a166fe%7C1783389462%7C259192%7CFri%2C+10-Jul-2026+01%3A57%3A34+GMT; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODM2NzUzMzEsIm5iZiI6MTc4MzU4NzkzMX0.87V1vudvzRmlU18DBnu9d0KDINQHqx09gjoXUyaub9Y; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgzNjc1MzMxLCJuYmYiOjE3ODM1ODc5MzF9.IYRvgWDgfUUEJenmKzySP3KzcMrJrtp7-iD9z4UNND0; s_v_web_id=verify_mreas780_49P1ruQ9_ndId_44qE_AGjS_OSl2PKDiEz9N; FPGSID=1.1783649372.1783649372.G-BZBQ2QHQSP.G2PXdkmPXmotVZOVRXYldg; _ttp=3G9tthP2YVCxSKFaXnbKdJkS2Fn.tt.1; FPLC=sMhmJpnz6qurfCyZ2hnckvGq7HHpPF7P%2Fwm0IDqjw0caKpjk14D%2F03P3alaCuinqePohgs9UN%2Fcj9siFyJX8DPL56verwie2F8Cf2TQBZA16C5R%2F2oIR9iveGQgoYA%3D%3D; msToken=uSUq6y7zVG6zfzj9rRWHh8kPHTg9jK19-xIbDk3DMUQILvBEMjt-A-pmI_sqjY6XIztgtuZsgDa2_tk6hvkmuR0w_kG4UyRLn01eikVdmyHCOAPldgMhRzJGRs7Udw77tA8TmGX56gu7X_A=; ttcsid_CMSS13RC77U1PJEFQUB0=1783649373071::Ee6RSfFeHTzFE8dZ6MGZ.35.1783649386305.1; ttcsid=1783649373072::49mOSGXG7ULxxEOdYGo9.35.1783649386305.0::1.-2630.0::20385.11.923.585::0.0.0; sso_uid_tt_ads=94f91684a70ff76a79915916f591201ee57170dca39802261fc1efa099364d7f; sso_uid_tt_ss_ads=94f91684a70ff76a79915916f591201ee57170dca39802261fc1efa099364d7f; sso_user_ads=89fd5ea74adb2a37c72cf560cdff69be; sso_user_ss_ads=89fd5ea74adb2a37c72cf560cdff69be; sid_ucp_sso_v1_ads=1.0.1-KGI4YmIzY2NhMThiMTdlZDIxNWNhZjE2ZWE2OThkYmRmYWMyNmFmNjEKIgiViLWa9-_oimkQ9KjB0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiA4OWZkNWVhNzRhZGIyYTM3YzcyY2Y1NjBjZGZmNjliZTJOCiDonw0fTEM7QIEgt5tq2RFCAXny4Y8vInFkRzFwq6DF_RIguxUJ4LdFeutjD2IZ9R7wSLLGDgdViHtshP9jOhLSjdIYAiIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGI4YmIzY2NhMThiMTdlZDIxNWNhZjE2ZWE2OThkYmRmYWMyNmFmNjEKIgiViLWa9-_oimkQ9KjB0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDc2cxIiA4OWZkNWVhNzRhZGIyYTM3YzcyY2Y1NjBjZGZmNjliZTJOCiDonw0fTEM7QIEgt5tq2RFCAXny4Y8vInFkRzFwq6DF_RIguxUJ4LdFeutjD2IZ9R7wSLLGDgdViHtshP9jOhLSjdIYAiIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1783649372$o34$g1$t1783649396$j36$l0$h964712691; odin_tt=86591eb09e706b7e9bcf902a0e3fedce5adeb6061a1ff0c614fbe3a2023dba9b85636213b6e198d330cf1c3995075ddc38d610d988ad0cd6efff258ae74f449d; sid_guard_tiktokseller=fc0e378b3265a877e301e41d4dabbaf6%7C1783649396%7C259200%7CMon%2C+13-Jul-2026+02%3A09%3A56+GMT; uid_tt_tiktokseller=075f99ff7d758a840b5f5e896b42a0bbe27c6bf6bb0bd824bcb4ac82e31a2b90; uid_tt_ss_tiktokseller=075f99ff7d758a840b5f5e896b42a0bbe27c6bf6bb0bd824bcb4ac82e31a2b90; sid_tt_tiktokseller=fc0e378b3265a877e301e41d4dabbaf6; sessionid_tiktokseller=fc0e378b3265a877e301e41d4dabbaf6; sessionid_ss_tiktokseller=fc0e378b3265a877e301e41d4dabbaf6; tt_session_tlb_tag_tiktokseller=sttt%7C3%7C_A43izJlqHfjAeQdTau69v_________KjQTukdn1WSFZoUcgqR-d2lcxFEvX_Eb-tlZ3-Sc0p0M%3D; sid_ucp_v1_tiktokseller=1.0.1-KGZiNWI0MmY3YjUyY2MwM2E4ODUyZTk5NTdkOWMwYjVkZWYyODQ4MTEKHAiViLWa9-_oimkQ9KjB0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBmYzBlMzc4YjMyNjVhODc3ZTMwMWU0MWQ0ZGFiYmFmNjJOCiBphxKJNu8Hig2cIa7fBomUNkxn_NqNlBml1F_2aePFvhIgz7rm-v5XPYECwjV2q5T3L0uvZ-i3xFONSQ2vm-8iYNIYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGZiNWI0MmY3YjUyY2MwM2E4ODUyZTk5NTdkOWMwYjVkZWYyODQ4MTEKHAiViLWa9-_oimkQ9KjB0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBmYzBlMzc4YjMyNjVhODc3ZTMwMWU0MWQ0ZGFiYmFmNjJOCiBphxKJNu8Hig2cIa7fBomUNkxn_NqNlBml1F_2aePFvhIgz7rm-v5XPYECwjV2q5T3L0uvZ-i3xFONSQ2vm-8iYNIYBSIGdGlrdG9r; msToken=MDiTeBTo1uLMJR5JmMOyVj9M1UR7KmuxpFn3CgM1zJg_DZger4AJCGfta8gg-_qi_70jiRLItyPzRSR5hW_WSBK6vfDtRZHsFkUkA9753UAr7XbdeQFzyN_0Phpf7jBBpbqvR8OBLe9Ftuc=; oec_lucifer=AQEBAAPLKrm/xY2MHycr3vqa+mfmLVNGjj5ju5WIvrdgZYaVcpq/RLLmybhnzC56Wo7gEE9GMcnBCQwEJ5kPWk/kV+sP+goYr8p3; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1783649411%7C917e8583208ac7ac56f5bd1d3a2303a879c4b566c101b6682433dffb87a26cb3; user_oec_info=0a5361e84f963ea16591775427e6e1bfa70267224b7b16d46930683e437839711bd9256a4a2da708158e3c923b5891cf0fbd1423e099e1bf73cc45372eb9474b3d571091b485ae49742261a9041fac0126baa5b64f1a490a3c0000000000000000000050a424c2ac5495883167192d3fcfb9c21b05cc8d3dad6992b1ac604f6e83f0de6d520a05bf33af09e155a63cc287b07f95ea10d7b5960e1886d2f6f20d2201042c292d11; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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
