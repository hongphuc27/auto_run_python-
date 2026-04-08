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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _ttp=3C10D8OQrVqlFYQo34srY0TOVaI.tt.1; _tt_enable_cookie=1; ttcsid_CMSS13RC77U1PJEFQUB0=1775537660817::aRA4G-_EmTu_ToyOpsFu.1.1775537723207.1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; ttcsid=1775537660818::bhcc8U1LpZTuFD306i5E.1.1775537723206.0::1.-18488.0::67734.24.732.495::0.0.0; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; sso_uid_tt_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_uid_tt_ss_ads=e8f2f5bfb71111187e6bdc83dfd00d4efbb0b67df5abb1e9e0e2baeb96e418af; sso_user_ads=9689c113a5f4913dbb255ae04a102874; sso_user_ss_ads=9689c113a5f4913dbb255ae04a102874; sid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDhhOWFlYWNkMTU2NmU1MzIxYjIxYmQ1YzdkODVkNGVmNDk0YmUzMDMKIgiUiN7g9dSegGkQxJzSzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA5Njg5YzExM2E1ZjQ5MTNkYmIyNTVhZTA0YTEwMjg3NDJOCiBoe50cdjX7uRS7QI1zDkCvWdac0a0CV4Ur8MYRYCVqoRIgaM72t1pX85LWbB7Wb38QCAkQoecZORmMKtMhnHe9GHEYAyIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1775537644$o1$g1$t1775537731$j60$l0$h890508184; sid_guard_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2%7C1775537732%7C259200%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; uid_tt_ss_tiktokseller=5094602d703533b55034d3f39cf00cf7765ce676cfed8736a5433c19b339650f; sid_tt_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; sessionid_ss_tiktokseller=ed06e1600d6eac702301fbd0e1099fc2; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C7QbhYA1urHAjAfvQ4Qmfwv__________QPF9zItUF2qAhZaIzMbvfm4jp09mDdlugUUrP303PR0%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM3YTc3Y2VhMWNjMGFmODhhZWQ4N2UxMjFhNTAzOGJlYzdiNmU0ZDAKHAiUiN7g9dSegGkQxJzSzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBlZDA2ZTE2MDBkNmVhYzcwMjMwMWZiZDBlMTA5OWZjMjJOCiAPBTgpzdO3B425_dRMvrVDKGToSWN6XlF_8_HbvUm99RIg8WiV6xFwrlf0Sp45E8Ezjb5V1bKTnT50hRI9SvNcs3YYAyIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; _tt_ticket_crypt_doamin=2; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzU2MjQxMzUsIm5iZiI6MTc3NTUzNjczNX0.UXXPVwe67zdp_oi66apV9KQuMu_8tfTOtspqFqltwe8; SHOP_ID=7075901688577638662; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc1NjI0MTM1LCJuYmYiOjE3NzU1MzY3MzV9.sLE87zt-dPEuRf5yrqO7-0xwfpl-EVDOzpoAzh79WUg; i18next=vi-VN; part=stable; tta_attr_id_mirror=0.1775537767.7625876643680124948; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1775537770%7C259162%7CFri%2C+10-Apr-2026+04%3A55%3A32+GMT; uid_tt_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; uid_tt_ss_ads=2777d71cb83f3daa35a13f436b432760520901561d41b04b2d09745ef35a31e2; sid_tt_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ads=88785dd3f5535735d8cc06391d57fc85; sessionid_ss_ads=88785dd3f5535735d8cc06391d57fc85; tt_session_tlb_tag_ads=sttt%7C2%7CiHhd0_VTVzXYzAY5HVf8hf_________176bHukUfGOuuG88-YidTfBynpHWGzETuNiYJeLzceqc%3D; sid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KGY1YzY2YzQ0NmYwZjk3NTE1MGE0ZTJhYzI4ZTgzNjdkMGI4NGZiZDcKHAiUiN7g9dSegGkQ6pzSzgYYrwwgDDgBQOsHSAQQAxoDc2cxIiA4ODc4NWRkM2Y1NTM1NzM1ZDhjYzA2MzkxZDU3ZmM4NTJOCiDmwq9kKJIQFStRrUgQaKKUGJ7YzpFlW1KydIbPSdcF5xIgU08Z0Qa8Ve2ZjCmICOyPGgQMkBbeBUf7lwtdXddFWBQYASIGdGlrdG9r; ac_csrftoken=3a6953b4381b4b159f9c60b73dadf820; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; pre_country=VN; msToken=_UGYSwS3AuCLwlIvgzUISPjWan8OESQmWEqf1Yux9yqRh7wRU54k2nXYMPveUDHmE4H31a-JGN-doElmjaWAn05TWfpLmt11AdqLq2pAolrH8FIVJUr-5pzXetYCUyZdXptP3vnlZX5bxoO7y4FuCRg=; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1775612363%7C7438eda01d0fa63a8a62a3d6b36d09213b00f1440594d22540909bd707b33658; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; user_oec_info=0a53e895f33373840ed5d755fc123664b3f390d07194edc10e7a8738ddf6cdad98f27acaeb2b7237d9677cbaedd7451ceeef8ac648987758186d39fd29b8f1a94e5ed2583d956b0b6091bd33c7ea34ff4575458c721a490a3c00000000000000000000504620155e3e86acde7a6bcf3f73a51b040a29279db6556dc54e9adf13e208d0ed62d0254d5cb416e0a77ce407d2ccdde7d510a59d8e0e1886d2f6f20d2201044cec152f; odin_tt=1e623b93777e4928a700a2938604225de40927a2603bd7446088a26691c42fb9abaec40ebb5676822628883a9965d4839119266b8319a0565c92faa8e5218e6b; msToken=MNrGvGT_zv8P7rK5jyWqf4edsniglbRQeOOjP_2SpsD7CV2qhQNWRvoeViRZMC_6KapKflLG479bubr2KjhDmgxC5BFSa5b_q-h9yevCOWBVZFYW_HEZFc9GszYOd6-27IdMEsk="
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

