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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; _m4b_theme_=new; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776044459%7C259200%7CThu%2C+16-Apr-2026+01%3A40%3A59+GMT; sid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGU1YmVhY2Q0OTQ0M2ZjZGMzOTkzODVmMGZmYmQyYWY3OWFiYjU5NmIKHAiUiN7g9dSegGkQq5PxzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiC29jN6N86trc3by2MiNWoZdTEntQJYUMrWGTRllo3ngBIg5QGy_cX6iNasxPHm_I13gCSmjWWG5TiaYoZPlqe052UYAiIGdGlrdG9r; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C1%7CiHhd0_VTVzXYzAY5HVf8hf_________X6QF71wEIh_9vkjIn8RqTBc05Xf_hP2Jh66NFE5GtjZw%3D; s_v_web_id=verify_mny25c44_9XFZ4joY_2GKq_4VsG_BqRv_vj26CWCg9NfH; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; FPLC=Dedymv%2FPgKexpgm6MwQu70V2o0tCdLGm8%2FrUsRi4H5bfGgjFCyx%2F74WLsDtud1I4PGeiozilXmEdU7t0rP192Yb4pL67DBX1BaXkp2jbV5pwjgqyyf2HFDh6dqLmaQ%3D%3D; ttcsid_CMSS13RC77U1PJEFQUB0=1776137101128::5q4gb__G7AmtOjDrYcpq.3.1776137126583.1; ttcsid=1776137101129::Dul7gHZIRT8MHOUSyB4R.3.1776137126583.0::1.-8026.0::31752.14.731.485::0.0.0; sso_uid_tt_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_uid_tt_ss_ads=860d671b6e90d0fda4361a1002cd530e702853ac2055ce141aab7e5e755972ca; sso_user_ads=3793b57f115dd5c737561e433a6cf308; sso_user_ss_ads=3793b57f115dd5c737561e433a6cf308; sid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGI5MGY1MzVmMGQzYTVhNDQzNDhlYmQ1NTc1ZDAyMWJhMDQ4MGQ1NzIKIgiUiN7g9dSegGkQsef2zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAzNzkzYjU3ZjExNWRkNWM3Mzc1NjFlNDMzYTZjZjMwODJOCiAeKH2J6tl5o2CQ4Ni7Dsodicqj4wT_sicLfIchCghd1RIgR7n6Cm3YvodDe5wS6BxMj0IeCOKPsIR4E9iKWGk0GocYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776137100$o3$g1$t1776137135$j25$l0$h1248465928; tt_ticket_guard_web_domain=2; sid_guard_tiktokseller=88550a7902dbadb4571676b145078738%7C1776137138%7C259199%7CFri%2C+17-Apr-2026+03%3A25%3A37+GMT; uid_tt_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; uid_tt_ss_tiktokseller=1a413be4ebd0a01b2ff800a305ba2f290cddfdc34cdde22eb8578d6786fa07e1; sid_tt_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_tiktokseller=88550a7902dbadb4571676b145078738; sessionid_ss_tiktokseller=88550a7902dbadb4571676b145078738; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CiFUKeQLbrbRXFnaxRQeHOP________-5u5Y_GrMYQ9o44TDdu9OnOdTJWSMfn3fYhgtNut3lcu8%3D; sid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGI1MjFjOTAyMWZmMWZkNTNjMTNkOTFiNjFmYmVjY2UyNDJmMGIxOTUKHAiUiN7g9dSegGkQsuf2zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4ODU1MGE3OTAyZGJhZGI0NTcxNjc2YjE0NTA3ODczODJOCiAcBe4b5wvM6XVdWbDE41c07ABhhna0M8xLDD-nWjNPIBIgcLxi5-uFKyn3_DvI0YiXk7fnbZTm2t4F8Z7mcOMS3bwYAiIGdGlrdG9r; msToken=-kBdY9ClUtXa7FivYIVyE067baDRqtsssyYUU5S5ei_0g_4SFP1MGOjEy3Gp22Bq66xl7WziGHNS4ffzWZDd5voexqk2jl6hNMswk0AlcqYqCxHUAFTYSlmKsY9p; _ttp=3CKo60ZAkHMPGpMXKFtLeXc4Fuy; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzYyMzUxNzUsIm5iZiI6MTc3NjE0Nzc3NX0.77TkhQpWLEHFvq7f5vSqP6BD9QLbT4vRd4Xj3zQ27n4; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2MjM1MTc1LCJuYmYiOjE3NzYxNDc3NzV9._obccaAxXy7oMUSWkymkiDvZlQC3aryORmIkWCNPQ7o; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776148830%7C062c4c36a14e813689980b63f47f542b9fcedee51f1232f2e1ac9d84f4faaace; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a53cf257361dd5409128c39e400a1de5dd7f0ecb5df466cdc12959217101dce7c7753003c81e8b1d80a6186cef5c65b9796ecb68e17719bb47464b246c88e6cc1b01d4aeae854e24b00243b3d532cd647bc2739e91a490a3c00000000000000000000504d710d5d42c494520c3690bd2fd9da6ed9b4c963d8f6a6543d36034f1141bc82d8870be7428027dfa549f202a49a84e8b510cee38e0e1886d2f6f20d2201040b7dd435; odin_tt=cf35f162b82f2fd4dd36496f70948e16c7fbb307b7f6bb4cdf89d4f30a3343ca3b8c546f1497e8fc4df7ab5dde7285411b644b39bed072da9bf526390df529ba; msToken=VQrhtUjd_9DrGWfEqJ15pHMG42tTsoPOHeB3HfRJmmpu24serONey1LpIiZlAYtNjF5yIFLMt0L8SWrU6Xgt9-RcGHK5-ebwW4tkxaHZowuhkQ8NQGQianQRA4mD0Hix6spvpg=="
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

