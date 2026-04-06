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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; _ga_HV1FL86553=GS2.1.s1775009054$o3$g0$t1775009054$j60$l0$h647018127; _ga_Y2RSHPPW88=GS2.1.s1775009054$o8$g1$t1775009054$j60$l0$h566538698; store-country-sign=MEIEDKm9OTjpJX0F_q_2OQQgdc5LHZLuZPjyHbu97JVFpxq4xnpmVDowX4ncnPjUc5sEEMD9Ifw7rK9aIfR54bWjA6Q; ttcsid=1775210520574::lpxhLY8mU4xaHlASeJaU.20.1775210535138.0::1.-2143.0::14563.15.1055.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1775210520573::joAYUSMCvqlYWRCVOp4B.15.1775210535138.1; sso_uid_tt_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_uid_tt_ss_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_user_ads=c103e6271279d24e6c4061d2c2613fbe; sso_user_ss_ads=c103e6271279d24e6c4061d2c2613fbe; sid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1775210520$o16$g1$t1775210537$j43$l0$h2111997584; sid_guard_tiktokseller=b22af622952384da29ce9cbafafbc01c%7C1775210537%7C259200%7CMon%2C+06-Apr-2026+10%3A02%3A17+GMT; uid_tt_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; uid_tt_ss_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; sid_tt_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_ss_tiktokseller=b22af622952384da29ce9cbafafbc01c; tt_session_tlb_tag_tiktokseller=sttt%7C1%7Csir2IpUjhNopzpy6-vvAHP_________WVUUl8r7EX6ao2IxjeENINn0gg-ndpOR0lwM-f4ToXN4%3D; sid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; sid_guard_ads=6fcaa88d51eae4c00d19b88db4693f37%7C1775293982%7C175755%7CMon%2C+06-Apr-2026+10%3A02%3A17+GMT; uid_tt_ads=1641c44d879f94593a1d45866ae3bb4d1a8fce9e72b18145c6e4df23628d6e3f; uid_tt_ss_ads=1641c44d879f94593a1d45866ae3bb4d1a8fce9e72b18145c6e4df23628d6e3f; sid_tt_ads=6fcaa88d51eae4c00d19b88db4693f37; sessionid_ads=6fcaa88d51eae4c00d19b88db4693f37; sessionid_ss_ads=6fcaa88d51eae4c00d19b88db4693f37; tt_session_tlb_tag_ads=sttt%7C4%7Cb8qojVHq5MANGbiNtGk_N__________-sDJR1Bby5Uf_jeHwrxqPGnmxDCN0A6K0IDMq85m_M4s%3D; sid_ucp_v1_ads=1.0.1-KGE3NWQ2MzRmNzViYjdlZTM2YzM2Y2EwMjIwYjA4ZDJjZjNhOTM3ZTgKHAiUiN7g9dSegGkQnqzDzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDZmY2FhODhkNTFlYWU0YzAwZDE5Yjg4ZGI0NjkzZjM3Mk4KIApaRRfYUr6wbQ-R9lABwMk2hYW5m9SsAz1g9NcNQAErEiDexhIADFa7Z2VpeWNZUw2XZHymlRsyTOTQj0kZRozCCxgBIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KGE3NWQ2MzRmNzViYjdlZTM2YzM2Y2EwMjIwYjA4ZDJjZjNhOTM3ZTgKHAiUiN7g9dSegGkQnqzDzgYYrwwgDDgBQOsHSAQQAxoCbXkiIDZmY2FhODhkNTFlYWU0YzAwZDE5Yjg4ZGI0NjkzZjM3Mk4KIApaRRfYUr6wbQ-R9lABwMk2hYW5m9SsAz1g9NcNQAErEiDexhIADFa7Z2VpeWNZUw2XZHymlRsyTOTQj0kZRozCCxgBIgZ0aWt0b2s; msToken=LYts5nwRUopujay97cIf-q-r3uSjJqBEN1KOFJMmc8eUm2sH7pjq_M42gaWhmcw__V2INznyefFWTqVvWaGBdyYhmFyO-zMRkSkvCHWmrDbIux59ZV-wBoooNnmyOwoQOsucPLHUKehiRUBsOKU9uREbYII=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU1Mjg2ODgsIm5iZiI6MTc3NTQ0MTI4OH0.8EK2na3huY__aHUergryk_95dHIAJROI9xWTnaUJ_U0; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1NTI4Njg4LCJuYmYiOjE3NzU0NDEyODh9.xZQIRIV60hxHpXjmdgjH57_T4_OkhtKOjpXPvaIOsTI; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1775442404%7C5b2040be9e2f9f80833512bd1f89c131a5a78f2186146e45a76db242ecce5fc4; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=06974b08880846dd2f74a8151510b055225120498f61ae02fa2a9d3edcf0a9788e705d2e9056c1c3b8008cd6eadc4de98a38fce91aced4c880650732a19eb2e3; user_oec_info=0a53d48012e73848b4d267c1cd61daff8411b2d389b1ae8ba83dac85d075940cbfa5985993f52a3f5e6e6b533d97ddef2e478594602c5d4f53ba816467d8d293e24d39f9dabe71c39de26c94fcde54782ce2633fd11a490a3c000000000000000000005044bcf0d7f562b7d8d9272c30c4d0870a7e5b34e1b3f8317a06d591ea3ca69949d4f050b3ee5f198bb91c82c6f930d77a2c10bd878e0e1886d2f6f20d220104b326c342; msToken=XT5CsqPLXPx3kPT1WUpTZcBJoBwqzMmxz4zJ2u3R-S3gJI7VJUsrEFxSFSwEbFhx52O_rtkG-kjNkClTjUvqPXzCxbYZx6hB4qukou5bbsRBzNS2bZ1GeJD7bAcj2WDpl8Br3ZQ="
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
    start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")

    print("🚀 START JOB")
    print(f"📅 DATE RANGE: FROM {start_date} TO {end_date}")

    target_dates = generate_date_list(start_date, end_date)

    print(f"📅 TOTAL DAYS: {len(target_dates)}")
    print(f"📋 DATE LIST: {target_dates}")

    for run_date_str in target_dates:
        df_final = extract_data_by_date(run_date_str)
        delete_and_insert_by_date(run_date_str, df_final)

    print("\n✅ DONE: refreshed last  days")


if __name__ == "__main__":
    main()

