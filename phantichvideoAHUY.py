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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; sid_guard_ads=f899825006ec49bd881b561b656a5f98%7C1774835615%7C259200%7CThu%2C+02-Apr-2026+01%3A53%3A35+GMT; uid_tt_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; uid_tt_ss_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; sid_tt_ads=f899825006ec49bd881b561b656a5f98; sessionid_ads=f899825006ec49bd881b561b656a5f98; sessionid_ss_ads=f899825006ec49bd881b561b656a5f98; sid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1774835513$o2$g1$t1774835616$j60$l0$h508160841; _ga_Y2RSHPPW88=GS2.1.s1774835513$o7$g1$t1774835616$j60$l0$h125334612; app_id_unified_seller_env=4068; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ5NDg4MDYsIm5iZiI6MTc3NDg2MTQwNn0.51Ghcv4gm5dD-EIfeKmaiyLiVhxi0su6g_BZ9io9Rc8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0OTQ4ODA2LCJuYmYiOjE3NzQ4NjE0MDZ9.OL-fidQQ5bZZ6hvVaYYYR1tPJI9Z4vg3sA1zP38m-IQ; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C5%7CeTB90XcQMeEWy9TpSLHjnv________-kOZQ-gfgJATLaewZc-YsmvK5DhYfQ5UVl0R61D5_45uM%3D; FPGSID=1.1774941273.1774941273.G-BZBQ2QHQSP.-k9g9eTvwPWbFWYIEitq8g; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; FPLC=l8qoyvg4VjVFU1tOPj0gmuiSND1kTlxTodp1MQOBrL0jlctxtuNFn9V7Qcajzwn9cwxatuvhRF9mykb8sZ2R5zGoREVii4pNco8dROlihoswcccpDqhsySnjwirkEw%3D%3D; store-country-sign=MEIEDF3Y0G9QIYyWmWmpkgQgwnfEpBduknwure1rT6lMk36oyvzKBDm9Zs4QeJJuMfEEEObOAhw6z3nxkvba-wp00Ik; ttcsid=1774941273302::U1-Fjv6__OEQU9nEFqmB.19.1774941295280.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774941273302::MgCW38qV0Mk0BEBfoLXk.14.1774941295280.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; sso_uid_tt_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_uid_tt_ss_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_user_ads=59dc308787ebd574297f1aa3d66152e3; sso_user_ss_ads=59dc308787ebd574297f1aa3d66152e3; sid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774941273$o15$g1$t1774941297$j36$l0$h1797522778; sid_guard_tiktokseller=3823c1a4e22721a66f3de0976fc4861e%7C1774941298%7C259199%7CFri%2C+03-Apr-2026+07%3A14%3A57+GMT; uid_tt_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; uid_tt_ss_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; sid_tt_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_ss_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; tt_session_tlb_tag_tiktokseller=sttt%7C1%7COCPBpOInIaZvPeCXb8SGHv_________LHeMVYk5Fl2CDKeV0khBmhOnBNjaI8VV2xOKbs0lgOlE%3D; sid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; msToken=HrEDPzQEv6l5S_fFD2w1NHsTbZE42n3i7L07XRsIlgULiJSxKunkMUgahrls8WFMr5cMU7xtG76j8Gs8RMnXoBrTjjmYEDnIay6vYHJhANlxzhij97bbpG3dMe4Zqw==; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774941351%7C912edb2f19e5359305c32aa98f61549c7d83479664c7e70c92e5f00fbff3a845; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=e80e7f62818b9622b2f6b4230b83e08f2b32f18fc52694dd7c19644039d2d48f8764df451ad6fa789389c009a9615046682d4a4775bccfd7318dda6accb0b8de; msToken=ydg-MZ27sDEKKmbBliDwVwaoqlWCj_Fy6mJ8GyQLRsmGnyQmrsed5lEbJAbXKQPfzwJ8CAybY0uZOuPjsO4CC0iy4RukM9mu8qVamJBhHYx7lRib9FRHIoEusoajAzVl7CD_TCez; user_oec_info=0a536edbca46b43ee4ca585036e2e25535fc162590956ef9e5f24d56d3339d6dfb65dd962ed984d15927ecee6d714b798fe94fbab43de3b4a7a04e0868880983c8490c1b7c85806025e663380736f3415928e9ca5a1a490a3c00000000000000000000503fb20d00617928e1ce9f0f4930fea0c5af352ecfd100e63c3b8b65d3929fbda23ebe6029218d5739ee2b00e850c0b894b410eac78d0e1886d2f6f20d220104d09e647e"""
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

