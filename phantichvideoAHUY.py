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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; _ga_HV1FL86553=GS2.1.s1773990828$o6$g1$t1773990828$j60$l0$h1985046469; _ga_Y2RSHPPW88=GS2.1.s1773990828$o5$g1$t1773990828$j60$l0$h1334679047; ttcsid_C97F14JC77U63IDI7U40=1773990829246::_nFQcqJTBcfmOF3E39x_.4.1773990829644.0; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; ttcsid=1774328558618::vo01iITGmxLRpspmTycH.15.1774328581572.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774328558618::yaGX_UIJG4xKpNNcedlG.12.1774328581572.1; sso_uid_tt_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_uid_tt_ss_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_user_ads=28923db567af453c44258468669f6c64; sso_user_ss_ads=28923db567af453c44258468669f6c64; sid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774328558$o13$g1$t1774328584$j34$l0$h182365910; sid_guard_tiktokseller=89fc588555d8994266248c8b5c2794f7%7C1774328583%7C259200%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; uid_tt_ss_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; sid_tt_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_ss_tiktokseller=89fc588555d8994266248c8b5c2794f7; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CifxYhVXYmUJmJIyLXCeU9__________6r4d-Iyrp0mENXHEHHfoMCN3BQVGhnLrugOXdiVOSilI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; part=stable; sid_guard_ads=c506bb03011584fbc778c79fd05d2514%7C1774405638%7C182145%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_ads=bf54c5a17681444f503392eb812e2acee8b9112aabd07f74205a1b8c7e30c151; uid_tt_ss_ads=bf54c5a17681444f503392eb812e2acee8b9112aabd07f74205a1b8c7e30c151; sid_tt_ads=c506bb03011584fbc778c79fd05d2514; sessionid_ads=c506bb03011584fbc778c79fd05d2514; sessionid_ss_ads=c506bb03011584fbc778c79fd05d2514; tt_session_tlb_tag_ads=sttt%7C1%7CxQa7AwEVhPvHeMef0F0lFP________-hpWG4_nAPrs9G6ZPvifVViERvQclRY-HUGpUjezy4E7k%3D; sid_ucp_v1_ads=1.0.1-KDU1MzdhZTliZGVhMTY1YjZiM2U5MDViMTY4MTgxN2U2ZjcyNWM3MmYKHAiUiN7g9dSegGkQhpCNzgYYrwwgDDgBQOsHSAQQAxoDbXkyIiBjNTA2YmIwMzAxMTU4NGZiYzc3OGM3OWZkMDVkMjUxNDJOCiB4rDPRkoSgpLwvqPU_BMiOxMyodOTvw3XX3fzBoRBhnhIgPIIQHl7mB3LarXI9O3DAHafsKRE96aO8OehVVjeHKOoYBSIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDU1MzdhZTliZGVhMTY1YjZiM2U5MDViMTY4MTgxN2U2ZjcyNWM3MmYKHAiUiN7g9dSegGkQhpCNzgYYrwwgDDgBQOsHSAQQAxoDbXkyIiBjNTA2YmIwMzAxMTU4NGZiYzc3OGM3OWZkMDVkMjUxNDJOCiB4rDPRkoSgpLwvqPU_BMiOxMyodOTvw3XX3fzBoRBhnhIgPIIQHl7mB3LarXI9O3DAHafsKRE96aO8OehVVjeHKOoYBSIGdGlrdG9r; ac_csrftoken=8cd79cf1c74c45ccac78a07fc4a75c3e; pre_country=VN; msToken=5wldyEIEbBrXCCKq0fIQT2NM2qBBnexIv3-xCl5vbev2uLDzHcnC3xgZUggcLr4YNOtYLBZE2wkTga4NVcI1osFCsgseRT6Rv4G2ZqP23tdYe2jvUkl_cMy0fJk8u-K0RSEwrVdyEfDG4ks=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774413528%7Cf331e77b726f42363895814ac24a7e00241a8b064407e644c46e1c7c0149f8c2; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJHZTZ2UFJtbTJNa2ZvbWxhYnUzZ1lKM2hUU05aQU43U3NNNEVyU1kraUxUSFd0SDdxc203dldnVnVtRGROVnRYbWkvb0ZERmhxeXlRQzZwa2hlbU1TST0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0OTk5MjksIm5iZiI6MTc3NDQxMjUyOX0.eW9QIEO9DnGvlEaYJr_-8px2yVO-1LIgsgQ0Zxjs88k; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDk5OTI5LCJuYmYiOjE3NzQ0MTI1Mjl9.CDgMVWF2QU9q6eaOY_V3sWKcqo5TVcTML0WNkWZG-4M; user_oec_info=0a53f75d0d41f072c660cfc69f79f0a008281777ea647f59cd366b308330759273fbdf469e11f4bab1ed7e1888db3221a408edb1c6568be5c36e3d9e9130b6ad2e2bf988e94c50678fdc0ef2606a80a028dd80cbf71a490a3c000000000000000000005039621330e6033bb570828975be2aa6364e4a37f34412e59262c31150074ea5b705ec630a3fb299f6ab31740fbf700a4fdb10dd828d0e1886d2f6f20d220104d7c64521; msToken=70o-ILq-1dmzFsd6CzienufYzMuij7gqzAZHhjcJSEO7Bfzsgi4mYjxJq9uSybpwmyly_ABqbxegB2Qqf7Rt2HQsYgfcAaqP3AxQ7FKkZ3Py6kbfvlKJ4qh1aFF1tln3843bHa-k; odin_tt=f14f9324ee08dbaa95b7a7054c38e900ab1cc87d9b8f8bb03548bb070d0e7e412f1c59e45e7d00c943ac3ee18b6dc8116b4f0be1d880a7afaec165e902e0133e"""
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

