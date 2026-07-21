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
    "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; FPAU=1.2.1047425659.1783389422; sid_guard_ads=2a17390f694186c6010ee8f8f7a166fe%7C1783389462%7C259192%7CFri%2C+10-Jul-2026+01%3A57%3A34+GMT; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDII-KHBmquvpOqtf4wQg-94KOUzYFOJxKdZ8VOI2LTXmP27Cf89jSzEySM82X0sEEM8l9uY94VagX01oEOwy6ZQ; _ttp=3GRa5zmj8ZSuYyGohr4orlRmGA6.tt.1; s_v_web_id=verify_mrsmr98u_38SMbyID_nlyF_4syF_AH2v_iJE2BOAY7Vyy; FPGSID=1.1784516010.1784516010.G-BZBQ2QHQSP.uQLNo4Jj5vEXn4dOvRkbIw; FPLC=66trP1Na1UY4A9Hwy2IDXw2b9YLRlKtRsS6tLgkxxdRbj1PK4qJiHTAp1ALvgFS65PhdkRkgAJ0%2FRNMffUPdqeU3rNZZZ4YFd4WqngwpjYBk5qlK0LZxNsVeSNMv0w%3D%3D; msToken=j_3TvOTYRpzcDuasypDlKCDZuRf2HG0U-ZzasBmdlz_7XGyWQRgLxPT2Ec7vTs1BLojdKO_sztBXdvTCfjVdAh4HUd3TLeps2YOq8Nq31wXfCdCPM7gg9oQpwOd1LvTXxosrALtaWfl4dMpAWOBXzCNg; ttcsid=1784516010919::4_WxqZ6gt5bWhKU6Lrkk.38.1784516033295.0::1.-2502.0::22373.12.1023.574::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1784516010917::_gxqqOjmXl8bOKdkHzjQ.38.1784516033295.1; sso_uid_tt_ads=86c6d31df02cb6e34654469e3f64da37cb6427c5098efeed4f0cfd31ccf33bab; sso_uid_tt_ss_ads=86c6d31df02cb6e34654469e3f64da37cb6427c5098efeed4f0cfd31ccf33bab; sso_user_ads=d8d64d3f6a122197d7aa6f3794e93355; sso_user_ss_ads=d8d64d3f6a122197d7aa6f3794e93355; sid_ucp_sso_v1_ads=1.0.1-KDI5ZDcyMzAwZWU4YTIwYmI1NzYwYjAxMWRmYTkyZjlkNDNjYzI1ZWUKIgiViLWa9-_oimkQw5v20gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDbXkyIiBkOGQ2NGQzZjZhMTIyMTk3ZDdhYTZmMzc5NGU5MzM1NTJOCiDZYoyDkwyS53O60UUAVoAdG-IKHZXLLswzWsbNG-nb5xIg2rB8b5_j850JHcB9DRwwe9ZPFjA9PtubUizfkEictNgYAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDI5ZDcyMzAwZWU4YTIwYmI1NzYwYjAxMWRmYTkyZjlkNDNjYzI1ZWUKIgiViLWa9-_oimkQw5v20gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoDbXkyIiBkOGQ2NGQzZjZhMTIyMTk3ZDdhYTZmMzc5NGU5MzM1NTJOCiDZYoyDkwyS53O60UUAVoAdG-IKHZXLLswzWsbNG-nb5xIg2rB8b5_j850JHcB9DRwwe9ZPFjA9PtubUizfkEictNgYAyIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1784516010$o37$g1$t1784516035$j35$l0$h926160094; odin_tt=a1ec7d6ad62b206d536d58c4deeab8e35e152fbde5d5d42cc0592fc75b037cbc64170c30f4d04ebb1a3910b235a206df74c8e2e63aca7b037390e615f01ba250; sid_guard_tiktokseller=3069cd2922ce8111b6cf1afd5241a558%7C1784516036%7C259199%7CThu%2C+23-Jul-2026+02%3A53%3A55+GMT; uid_tt_tiktokseller=eadf08382dc965e3c39bae12ec87ce908451c119847ea1b7ba95b1e2f1907e11; uid_tt_ss_tiktokseller=eadf08382dc965e3c39bae12ec87ce908451c119847ea1b7ba95b1e2f1907e11; sid_tt_tiktokseller=3069cd2922ce8111b6cf1afd5241a558; sessionid_tiktokseller=3069cd2922ce8111b6cf1afd5241a558; sessionid_ss_tiktokseller=3069cd2922ce8111b6cf1afd5241a558; tt_session_tlb_tag_tiktokseller=sttt%7C4%7CMGnNKSLOgRG2zxr9UkGlWP________-2kmvxymtOM7bmD3AZsQFTPKpwxxe5aTCCwiOIucXPQh8%3D; sid_ucp_v1_tiktokseller=1.0.1-KGEzNjFhOTFiMmU4MDlmMGZmOWNkNTBiMjM5NjVhZTYzOWZlYjVmMjUKHAiViLWa9-_oimkQxJv20gYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzMDY5Y2QyOTIyY2U4MTExYjZjZjFhZmQ1MjQxYTU1ODJOCiB8KGI_xV_nJsS_xIE9YaZeRvx7pNkMVnNtfIA59o5vVBIgltzti_DP4YvndTyF95uSoIuMR1HWhAO3dZApfUOAjKsYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGEzNjFhOTFiMmU4MDlmMGZmOWNkNTBiMjM5NjVhZTYzOWZlYjVmMjUKHAiViLWa9-_oimkQxJv20gYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzMDY5Y2QyOTIyY2U4MTExYjZjZjFhZmQ1MjQxYTU1ODJOCiB8KGI_xV_nJsS_xIE9YaZeRvx7pNkMVnNtfIA59o5vVBIgltzti_DP4YvndTyF95uSoIuMR1HWhAO3dZApfUOAjKsYASIGdGlrdG9r; msToken=hXZwPALD7kwPv3xY8ZBfT-NzJEpmspMewqVK1--yelQVdaMnL8d6MV4pKeJ_7GLlnLmOaZWFU3hIWMymAQMuFubDsuat_JOWGv6vyfISPhVJg1DN8TGO2OkN__dZY3TyyswADUF7aSZkwMW7Oq5y9Ij5; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODQ2MDI0MzcsIm5iZiI6MTc4NDUxNTAzN30.jyIL83ndsHrTQqTVfmKDBRCL2D8IBdQmesereM687Kg; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzg0NjAyNDM3LCJuYmYiOjE3ODQ1MTUwMzd9.u8QVXLleWjk5PDWnMJl80V29kOqEHI2jV0nJq2UZE00; lang_type=vi; oec_lucifer=AQEBAJZcj1ALb7YNfcD03RGhSf4mv7Z3uaBapBSx4dL1ytIUvhahy2jQ3R9qJte1oXfYUao6L/VdTNIGp/UfNv44/gNSELfhQoPr; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1784516054%7Cbf43efb41555a9753587b036e4e6942f2236ee961e57d114bd07c8242676e784; user_oec_info=0a53e01e003c10db8cb9720eea0caae1e724cc7e39e58f444b1d8872351d0dfc2060ccefc5d7adab8b5982abb916f52711b330038dba24d2883c394e7347c3f001cb31b4943452feee06cf43fd9f42769773cace131a490a3c0000000000000000000050ae93da6d47b7049df9f2935eb9554e3e7e1a8c2f8b1f07c67ccdb17efe4c5fbae18a8cd5c0239b3655e8d6d65b1f86fb7d10cda6970e1886d2f6f20d2201047044fa8e; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

# today = datetime.today().date()
today = datetime(2026, 7, 6).date()
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
