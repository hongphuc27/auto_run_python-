import requests
import pandas as pd
import time
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import os


# =====================================================
# BIGQUERY CONFIG
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_phantich_video_tiktok_ahuy"

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# =====================================================
# TIKTOK API
# =====================================================
URL = (
    "https://affiliate.tiktok.com/api/v2/insights/affiliate/seller/creator/video/list"
    "?user_language=vi-VN"
    "&aid=4331"
    "&app_name=i18n_ecom_alliance"
    "&device_id=0"
    "&device_platform=web"
    "&cookie_enabled=true"
    "&screen_width=1920"
    "&screen_height=1080"
    "&browser_language=vi"
    "&browser_platform=Win32"
    "&browser_name=Mozilla"
    "&browser_version=5.0+(Windows+NT+10.0%3B+Win64%3B+x64)+AppleWebKit%2F537.36+(KHTML,+like+Gecko)+Chrome%2F145.0.0.0+Safari%2F537.36"
    "&browser_online=true"
    "&timezone_name=Asia%2FSaigon"
    "&oec_seller_id=7494545630022240481"
    "&shop_region=VN"
    "&platform_data_source=shop"
    "&use_content_type_definition=1"
    "&msToken=LqawutZy4mOCmN-wVYI7IwT6Sy2wdkNoRxDFCWRI6-Cp0cV4ZeNkf6o_QVDT1o3dCHejHv2IRo24yQLNsHFK-aI4i7WK8e21fpjh1NQ4Zg-YG5dLZDuRo11FoX30O1RN6mX9O7I="
    "&X-Bogus=DFSzswVOXhaJiIKzCEHr4OVRr37j"
    "&X-Gnarly=MP8Lu0e8m5DCg/Vp2XudrMxyrx7iOSmAOyDXS/EQodhGUCZVaogL919amHDmzYWJr5t8yh0Sa9J2pSJdKepySUig5YN0SqTJEuzsXDjT73zIsDtPK69Gs2iQc2gkqBo0QseLrVddlTQkDJMDH6GPE26gwbzipQTtzRS-vNstbLB8joxwOFQ/KvysqipGxRFiyoA/TLI4MZ16SBvKcwfqq7pn6X8qOY8u0ntxPPTnNXcDM6Vh7aKxk8cg8RMEf-ClwyCdrwf1zGB2"
)

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": "https://affiliate.tiktok.com",
    "referer": "https://affiliate.tiktok.com/data/video?shop_region=VN&shop_id=7494545630022240481",
    "request-start-time": "1773731094815",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _ttp=3AvIaixPN8VfHyK5OTkTyJrsKKD.tt.1; _m4b_theme_=new; ttcsid_C97F14JC77U63IDI7U40=1773635979896::IHQwRRA_YETJxxAS0PmA.3.1773636022342.1; _ga_Y2RSHPPW88=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h640968867; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; i18next=vi-VN; part=stable; uid_tt_ads=ed3399099171552ba3a2970110b15bd620fd463caa0ea8aee84b3b62bd821430; uid_tt_ss_ads=ed3399099171552ba3a2970110b15bd620fd463caa0ea8aee84b3b62bd821430; sid_tt_ads=9a93d6faa80e177431e5cabd65508e65; sessionid_ads=9a93d6faa80e177431e5cabd65508e65; sessionid_ss_ads=9a93d6faa80e177431e5cabd65508e65; _ga_HV1FL86553=GS2.1.s1773829245$o5$g0$t1773829245$j60$l0$h359415048; pre_country=VN; FPLC=l%2Fz79w5do%2F0HqSmU428VsTEkC66IGxkJ7nqKsIlo0%2BYlLV9%2BAvPwZ71l0GWq%2F2DHtBYeTKiaLfkjOE8UHebQEwJZnXjtLw86DvdpJSVioNbTWW%2BTofbzLeY9SX%2B0Tg%3D%3D; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; sso_uid_tt_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_uid_tt_ss_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_user_ads=14d85f2b0adb9b74de123cec9b36ea8d; sso_user_ss_ads=14d85f2b0adb9b74de123cec9b36ea8d; sid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; uid_tt_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; uid_tt_ss_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; sid_tt_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_ss_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQwMDMyMDgsIm5iZiI6MTc3MzkxNTgwOH0.Z0e1l9NWRGB5FU9FB-IfrbDkBMYZe0XjPyB9gW4PqcQ; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0MDAzMjA4LCJuYmYiOjE3NzM5MTU4MDh9.tRueupzlz2Psmfi-hGZ74sMbQgpTzat8xSiizVsShhw; sid_guard_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf%7C1773917200%7C258806%7CSun%2C+22-Mar-2026+10%3A40%3A06+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CtE6h4jZD57IHTJyD6AGvv__________ofSyY124wjArfuNJz-lYs0-mN9doAGDJk-7L10m36hZo%3D; sid_ucp_v1_tiktokseller=1.0.1-KGEzN2JiMWUxNmM4NGY1NzNmNWQ3M2FmODQ4MzQ4MjkwODFlNzcyNzQKHAiUiN7g9dSegGkQkKjvzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIGma_ijqiCSBW2aLJUk4r9_q0DI3z_YE0RCgGUeAkg1mEiB8GioGo0fq54sW_R0MUakAvFNBK1LgRflEyZKBs6xlkxgFIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KGEzN2JiMWUxNmM4NGY1NzNmNWQ3M2FmODQ4MzQ4MjkwODFlNzcyNzQKHAiUiN7g9dSegGkQkKjvzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIGma_ijqiCSBW2aLJUk4r9_q0DI3z_YE0RCgGUeAkg1mEiB8GioGo0fq54sW_R0MUakAvFNBK1LgRflEyZKBs6xlkxgFIgZ0aWt0b2s; sid_guard_ads=9a93d6faa80e177431e5cabd65508e65%7C1773971489%7C259200%7CMon%2C+23-Mar-2026+01%3A51%3A29+GMT; tt_session_tlb_tag_ads=sttt%7C4%7CmpPW-qgOF3Qx5cq9ZVCOZf_________DaE7IXPWkJDVV_pZ6t4OelGj1nMjRtTBX7UfnWo0j3rg%3D; sid_ucp_v1_ads=1.0.1-KGY1OTE0YTY0MGFhOWY1ZDc1NGE1ODUwMTU2OTk1M2E5ZDExNjVhMmEKHAiUiN7g9dSegGkQodDyzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiA5YTkzZDZmYWE4MGUxNzc0MzFlNWNhYmQ2NTUwOGU2NTJOCiD3cjoNj8BAD_XxKmcgV93PLzmpLQ7Wh4VtfOjsGrp1fxIg9fZBZVm84rBKvokX_84ZHNl9yU1NH0DLSqnedZeOj60YAyIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGY1OTE0YTY0MGFhOWY1ZDc1NGE1ODUwMTU2OTk1M2E5ZDExNjVhMmEKHAiUiN7g9dSegGkQodDyzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiA5YTkzZDZmYWE4MGUxNzc0MzFlNWNhYmQ2NTUwOGU2NTJOCiD3cjoNj8BAD_XxKmcgV93PLzmpLQ7Wh4VtfOjsGrp1fxIg9fZBZVm84rBKvokX_84ZHNl9yU1NH0DLSqnedZeOj60YAyIGdGlrdG9r; ttcsid=1773972914915::1uzqfv2rYEiPymKjf9zq.12.1773973086478.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773972914914::L8HykAtDISpj945YDcYg.9.1773973086478.1; _ga_BZBQ2QHQSP=GS2.1.s1773972909$o9$g1$t1773973090$j55$l0$h1257713795; msToken=hirGtE7RwmiPSRzOjawHSlL9d0h7Z8t6xCoue_G4c_Hhyd90im6ius_--Aj4FtXWmmrCW7iGwpfRj7pT6x8WVpAz0CSqfoHaqqGOy06QPaAjp_5iAKJyQNicuzuo; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773973380%7C1dd2948dca3f317dffc4c3dfd448cdecbce205a074b1f16f7e83b48e306bfa9a; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=04abb79fb0d1e27d2789f691f619a5758d5f536378ec7cff4b6d779e22c8158e5e5110c7839836474101148b42a5a72ac285212a57865196b1c30c45a44177cd; user_oec_info=0a53be21f99f4150af3abc2ecf208628c8b83cc6d1d647da1f8aec4d25543a163a514ca86b57868e0f94c383869f1850253ae8f9f411ab8fca034412de3e58bfabf952f2b8345601b5700e7fde79566f4d13e246971a490a3c00000000000000000000503420d446edef3d1693441348773c5121d4330d40086a432de6a2bbc2710c00bdb451f7adb047d2c6229d8f35fa0471e96210f8c98c0e1886d2f6f20d220104e9188ac7; msToken=KU0rxQCIDNpy_HnR6ONrO9EAXGTeS3fEd-2E6aoGo3oZDHho-P86es1gpSUzh2dZ_FpNQJfAXNmE_fUldjRnKu5pQ2TNgZ-0CsvqJaXVWlaiPohzPbmpbK1lJMLFDcom0ELHFvY="""
}

# =====================================================
# HÀM GỌI API
# =====================================================
def fetch_page(page: int, start_date: str, end_date: str):
    payload = {
        "request": {
            "requests": [
                {
                    "stats_types": [11, 5, 1, 12, 3, 2, 4, 5, 30, 31, 40, 46, 44, 41, 32, 22, 23, 42, 43, 34, 33],
                    "filter": {
                        "filter_type": 1
                    },
                    "list_control": {
                        "rules": [
                            {
                                "field": "SELLER_CREATOR_VIDEO_LIST_REVENUE",
                                "direction": 2
                            }
                        ],
                        "pagination": {
                            "size": 100,
                            "page": page
                        }
                    },
                    "time_descriptor": {
                        "start": start_date,
                        "end": end_date,
                        "timezone_offset": 25200,
                        "scenario": 1,
                        "granularity": "",
                        "with_previous_period": False
                    }
                }
            ]
        },
        "version": 2
    }

    r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# =====================================================
# CONVERT DECIMAL
# =====================================================
def to_decimal(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None



def find_rows(obj):
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and "item_id" in obj[0]:
            return obj
        for item in obj:
            result = find_rows(item)
            if result:
                return result

    elif isinstance(obj, dict):
        for value in obj.values():
            result = find_rows(value)
            if result:
                return result

    return []
# =====================================================
# MAIN
# =====================================================
def generate_date_list(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def extract_data_by_date(run_date_str):
    all_rows = []

    report_date = run_date_str
    next_date = (
        datetime.strptime(run_date_str, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    print(f"\n===== Processing date: {report_date} =====")
    print(f"Fetching API range: from {report_date} to {next_date}")

    page = 0
    empty_retry = 0
    page_error_retry = 0

    while True:
        try:
            data = fetch_page(page, report_date, next_date)
        except Exception as e:
            print("API error retry:", e)
            time.sleep(5)
            continue

        if isinstance(data, dict) and data.get("code") not in (None, 0):
            error_code = data.get("code")

            print(f"API returned error on date {report_date}, page {page}: {data}")

            if error_code == 98001021:
                page_error_retry += 1

                if page_error_retry >= 5:
                    print(f"Skip page {page} after too many downstream errors.")
                    break

                wait_seconds = min(10 * page_error_retry, 60)
                print(f"Retry page {page} after {wait_seconds}s")

                time.sleep(wait_seconds)
                continue

            break

        rows = find_rows(data)

        print(f"Date {report_date} | Range {report_date} -> {next_date} | Page {page} | Rows: {len(rows)}")

        if not rows:
            empty_retry += 1
            if empty_retry >= 3:
                print("No more data for this date.")
                break

            print("Empty page -> retry")
            time.sleep(2)
            continue

        empty_retry = 0
        page_error_retry = 0

        for o in rows:
            item_id = o.get("item_id")
            creator_handle = o.get("creator_handle")
            revenue_amount = o.get("revenue", {}).get("amount")
            direct_gmv_amount = o.get("direct_gmv", {}).get("amount")
            est_commission_amount = o.get("est_commission", {}).get("amount")

            all_rows.append((
                report_date,
                item_id,
                creator_handle,
                revenue_amount,
                direct_gmv_amount,
                est_commission_amount
            ))

        page += 1
        time.sleep(0.5)

    print(f"TOTAL ROWS OF {report_date}: {len(all_rows)}")

    if not all_rows:
        return pd.DataFrame(columns=[
            "report_date",
            "item_id",
            "creator_handle",
            "revenue_amount",
            "direct_gmv_amount",
            "est_commission_amount",
        ])

    df = pd.DataFrame(all_rows, columns=[
        "report_date",
        "item_id",
        "creator_handle",
        "revenue_amount",
        "direct_gmv_amount",
        "est_commission_amount",
    ])

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    df["item_id"] = df["item_id"].astype("object")
    df["creator_handle"] = df["creator_handle"].astype("object")

    df["revenue_amount"] = df["revenue_amount"].apply(to_decimal).astype("object")
    df["direct_gmv_amount"] = df["direct_gmv_amount"].apply(to_decimal).astype("object")
    df["est_commission_amount"] = df["est_commission_amount"].apply(to_decimal).astype("object")

    df = df.dropna(subset=["report_date", "item_id"])
    df = df.drop_duplicates(subset=["report_date", "item_id"])

    return df


def delete_and_insert_by_date(run_date_str, df_final):
    print(f"\n🧹 Delete data of {run_date_str} in BigQuery")

    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE report_date = @date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", run_date_str)
        ]
    )

    client.query(delete_query, job_config=job_config).result()

    if df_final.empty:
        print(f"Không có dữ liệu để insert cho {run_date_str}")
        return

    print(f"⬆️ Insert {len(df_final)} rows for {run_date_str} into BigQuery")

    load_job = client.load_table_from_dataframe(
        df_final,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("report_date", "DATE"),
                bigquery.SchemaField("item_id", "STRING"),
                bigquery.SchemaField("creator_handle", "STRING"),
                bigquery.SchemaField("revenue_amount", "NUMERIC"),
                bigquery.SchemaField("direct_gmv_amount", "NUMERIC"),
                bigquery.SchemaField("est_commission_amount", "NUMERIC"),
            ],
        )
    )

    load_job.result()
    print(f"✅ Loaded {len(df_final)} rows for {run_date_str}")


def main():
    tz = ZoneInfo("Asia/Bangkok")
    today = datetime.now(tz).date()

    # 3 ngày gần nhất: hôm kia, hôm qua, hôm nay
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    print("🚀 START JOB")
    print(f"📅 DATE RANGE: FROM {start_date} TO {end_date}")

    target_dates = generate_date_list(start_date, end_date)

    print(f"📅 TOTAL DAYS: {len(target_dates)}")
    print(f"📋 DATE LIST: {target_dates}")

    for run_date_str in target_dates:
        df_final = extract_data_by_date(run_date_str)
        delete_and_insert_by_date(run_date_str, df_final)

    print("\n✅ DONE: refreshed last 3 days")


if __name__ == "__main__":
    main()

