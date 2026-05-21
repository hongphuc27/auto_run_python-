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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; gd_random=eyJwZXJjZW50IjowLjk2ODQ3NDc1MTEwMzQzMjQsIm1hdGNoIjpmYWxzZX0=.WWV75FWV/E2F/cKeE1tdE8mlX0Hj8PB93g/4Rtqd8Vs=; _tea_utm_cache_4068={%22campaign_id%22:1862554723618386}; _tea_utm_cache_1583={%22campaign_id%22:1862554723618386}; _m4b_theme_=new; FPLC=KuCKq73XpZZnXgBZC4n5Iy6Ijhi6c9MKHq4K1KRXGjgFji7pG14hD506zahKe8g4CSAFcufZUy4R1KZ%2Bsre3Bywa%2B69jf%2BXSqjaGkfowz9qqgwqmMbRucpXA5hbftw%3D%3D; _ttp=3Dw7r0jnzZIjQUgEfz8DGNHrlzG.tt.1; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; app_id_unified_seller_env=4068; sid_guard_ads=8befa66f0da6ecc549dc6781bc35e632%7C1779273067%7C259178%7CSat%2C+23-May-2026+10%3A30%3A45+GMT; uid_tt_ads=96cea1172cdcd9ed27a3c8e71f23ec487005414fda3e57cfe32f559eb0fc0b8a; uid_tt_ss_ads=96cea1172cdcd9ed27a3c8e71f23ec487005414fda3e57cfe32f559eb0fc0b8a; sid_tt_ads=8befa66f0da6ecc549dc6781bc35e632; sessionid_ads=8befa66f0da6ecc549dc6781bc35e632; sessionid_ss_ads=8befa66f0da6ecc549dc6781bc35e632; oec_lucifer=AQEBAIW4e0LzJkLGlwXM6lVdaNVAdWgSww2E30v6v2GyC7J6hoItGN42fAPQNeqsqyjlBwyF34c/J0OrhSBpaOFmBuLpod1VLQ==; passport_auth_status_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; passport_auth_status_ss_ads=ec9eed9eb3c0b180fa8af1c0d49a4510%2Ce99bfb2707696a7e72e592c440950c9f; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde; tt_session_tlb_tag_ads=sttt%7C3%7C9EPhJiNkMKV3NUTEoRXkC_________-iYP1puM_Tdd15AuOzxZvFpWDbLcuMIhCg-h8svFPkAFM%3D; store-country-sign=MEIEDIlwvNNq2oMujkVi9wQg_NyxFk_zwgytPKAc-XGblMwLdPzL93E4GZ-nVvKuIvwEECG0lqWWniJMWWV49cFEbOw; s_v_web_id=verify_mpeu9nla_rLOWRqRG_LO1C_4zJu_86ph_t776rFAVs14R; FPGSID=1.1779328537.1779328537.G-BZBQ2QHQSP.GaQnBaL7tFbXTAaI7XtcqQ; ttcsid=1779328535873::uDhGikw8kgKLGYs7xub3.17.1779328556260.0::1.-5092.0::20383.10.942.592::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1779328535872::p-8LrCyXwXGGCx-KI6HB.17.1779328556260.1; d_ticket_ads=40b4f7f55431d4013801acad214d89a237e1d; sso_uid_tt_ads=993d77ea59a2dd4267dd8c9e235dfd29540c3f531b4c5a5754f491b7e8df9548; sso_uid_tt_ss_ads=993d77ea59a2dd4267dd8c9e235dfd29540c3f531b4c5a5754f491b7e8df9548; sso_user_ads=9d6e49221356f3cd3fbaf285520986f6; sso_user_ss_ads=9d6e49221356f3cd3fbaf285520986f6; sid_ucp_sso_v1_ads=1.0.1-KGI3ZTE5MTNhOTBiYjAxZjhiYjgwZWYyYjczYWQzMjE3OTJhMTI4YjkKIgiUiN7g9dSegGkQsMy50AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDlkNmU0OTIyMTM1NmYzY2QzZmJhZjI4NTUyMDk4NmY2Mk4KIIfPscFvnBxQK92DJWn-bq--1z-6qo-Uc-30t-zZ4KAtEiCHDuSNLYosq1lYIY4cF-23TCJWMXwY1mCBy94aqsLZxBgBIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGI3ZTE5MTNhOTBiYjAxZjhiYjgwZWYyYjczYWQzMjE3OTJhMTI4YjkKIgiUiN7g9dSegGkQsMy50AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDlkNmU0OTIyMTM1NmYzY2QzZmJhZjI4NTUyMDk4NmY2Mk4KIIfPscFvnBxQK92DJWn-bq--1z-6qo-Uc-30t-zZ4KAtEiCHDuSNLYosq1lYIY4cF-23TCJWMXwY1mCBy94aqsLZxBgBIgZ0aWt0b2s; sid_guard_tiktokseller=83710b2a9a10a968e66a0c1cd6317609%7C1779328561%7C259199%7CSun%2C+24-May-2026+01%3A56%3A00+GMT; uid_tt_tiktokseller=60b2ba39e94940298e2b198d70fd9dd329c54bde32053380ff720fca1decacc3; uid_tt_ss_tiktokseller=60b2ba39e94940298e2b198d70fd9dd329c54bde32053380ff720fca1decacc3; sid_tt_tiktokseller=83710b2a9a10a968e66a0c1cd6317609; sessionid_tiktokseller=83710b2a9a10a968e66a0c1cd6317609; sessionid_ss_tiktokseller=83710b2a9a10a968e66a0c1cd6317609; tt_session_tlb_tag_tiktokseller=sttt%7C5%7Cg3ELKpoQqWjmagwc1jF2Cf_________6SQihZUiv7qYLJWXD7IJB3hX5syJXZ2y5C5O0l858y40%3D; sid_ucp_v1_tiktokseller=1.0.1-KGI1OTE4NTVjMjEyYWRjYzhiMjFmNDZmNTM5NDlhYmQ3MmRkOTY0NGIKHAiUiN7g9dSegGkQscy50AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MzcxMGIyYTlhMTBhOTY4ZTY2YTBjMWNkNjMxNzYwOTJOCiCIgEKTZHfciXwBK19LrQARErO1shR4VB5nT3Vm40pa7xIgq0m6VYkHDdG1lzF8zefYyF5pRPw_doYAi4unBasd_-IYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGI1OTE4NTVjMjEyYWRjYzhiMjFmNDZmNTM5NDlhYmQ3MmRkOTY0NGIKHAiUiN7g9dSegGkQscy50AYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4MzcxMGIyYTlhMTBhOTY4ZTY2YTBjMWNkNjMxNzYwOTJOCiCIgEKTZHfciXwBK19LrQARErO1shR4VB5nT3Vm40pa7xIgq0m6VYkHDdG1lzF8zefYyF5pRPw_doYAi4unBasd_-IYAiIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; _ga_BZBQ2QHQSP=GS2.1.s1779328535$o17$g1$t1779328561$j34$l0$h893136804; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzk0MTQ5NjUsIm5iZiI6MTc3OTMyNzU2NX0.qW21aPADi1DH8kE91qTzcahpSvlqHrU4M0smFUk_KbQ; SHOP_ID=7075901688577638662; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc5NDE0OTY1LCJuYmYiOjE3NzkzMjc1NjV9.SmucmZMYm6SBE8JxJYnUcPHb3ef2p5p6SQeZ8vocJek; msToken=hYAEblkH576goHlF-ZaqAvNf2mzfZAgkkCX5GumKbLFGInisyGXHkI2eiN2dPXiIdnqYFWZ4U1EUlSfBu-vjKAF9cv4VLO8tyUzuPcSbiKnO9k7Xr1XyR1i7VOodEc_tsPs3Qd0=; msToken=hYAEblkH576goHlF-ZaqAvNf2mzfZAgkkCX5GumKbLFGInisyGXHkI2eiN2dPXiIdnqYFWZ4U1EUlSfBu-vjKAF9cv4VLO8tyUzuPcSbiKnO9k7Xr1XyR1i7VOodEc_tsPs3Qd0=; lang_type=vi; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1779329125%7C3fa0dd91fd6c5a1b8184a40e42f91fc87d867170fbef472ba5427c60358e7f95; user_oec_info=0a536f081e9bdaeb6c8ee66fe7401af6b6b04fb2403d1bbb8dcdfd94a4a92dfee7f83f1ac8b7309d02a82fdbc5bc90d0b6d7f03c2b19f89ea2ae969a746bde51d8fc66b6ae5547a25b3e728990e9b2de0b835a61401a490a3c0000000000000000000050719b4d0d289d560a774f5950cae39b2366ff332905f2412abf50c02dee53cad593cfe4b4705e1b8153819343ef959a80b810fa81920e1886d2f6f20d2201040a85909d; odin_tt=b470cf0ff565b0ad65ea02e910dffcb9f58a21eef9f48b9b3ad58c738052bf7ad4ccae3e0eecfd923a19aca76abe22c7ab1f973256b00e2aa8684dc9e20ab868; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
}

# =====================================================
# 2. DATE RANGE
# =====================================================

# today = datetime.today().date()
today = datetime(2026, 5, 21).date()
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
