import requests
import pandas as pd
import time
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta


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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _ttp=3AvIaixPN8VfHyK5OTkTyJrsKKD.tt.1; _m4b_theme_=new; ttcsid_C97F14JC77U63IDI7U40=1773635979896::IHQwRRA_YETJxxAS0PmA.3.1773636022342.1; sid_guard_ads=e04a3f7caf24b5e89be320a0f121c4c1%7C1773636074%7C259200%7CThu%2C+19-Mar-2026+04%3A41%3A14+GMT; uid_tt_ads=af72cc7ff1263f8118268d9c989bd65328cea559970c4189c78145c45398273b; uid_tt_ss_ads=af72cc7ff1263f8118268d9c989bd65328cea559970c4189c78145c45398273b; sid_tt_ads=e04a3f7caf24b5e89be320a0f121c4c1; sessionid_ads=e04a3f7caf24b5e89be320a0f121c4c1; sessionid_ss_ads=e04a3f7caf24b5e89be320a0f121c4c1; sid_ucp_v1_ads=1.0.1-KGQxMTg4MzFmZmY2MmIyZTYzZDg5ZmZhMTI4MzZiNjc4ZmFiZWJiOWMKHAiQiKz8zcvg22kQ6pPezQYYrwwgDDgBQOsHSAQQAxoCbXkiIGUwNGEzZjdjYWYyNGI1ZTg5YmUzMjBhMGYxMjFjNGMxMk4KIDSCgQFxeor2EFw9EIiH4TivRFC0zDEn6mumsnaTBB7DEiDj4BEEJCXRb4BiPEk2q5d3cqpz5-EWqzz9CasXjrz8chgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KGQxMTg4MzFmZmY2MmIyZTYzZDg5ZmZhMTI4MzZiNjc4ZmFiZWJiOWMKHAiQiKz8zcvg22kQ6pPezQYYrwwgDDgBQOsHSAQQAxoCbXkiIGUwNGEzZjdjYWYyNGI1ZTg5YmUzMjBhMGYxMjFjNGMxMk4KIDSCgQFxeor2EFw9EIiH4TivRFC0zDEn6mumsnaTBB7DEiDj4BEEJCXRb4BiPEk2q5d3cqpz5-EWqzz9CasXjrz8chgEIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h1171674335; _ga_Y2RSHPPW88=GS2.1.s1773635979$o4$g1$t1773636075$j60$l0$h640968867; tta_attr_id=0.1773636090.7617708817361666066; tt_session_tlb_tag_ads=sttt%7C5%7Cq04bIGogHNq-BZBhKsspNv________-gO3xCsRydn5k8XnQLVCKqRbSmLFoPvXHPrPqfJNQjWD0%3D; app_id_unified_seller_env=4068; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; d_ticket_ads=13251634588e056625fdb16f92053bdf7856e; sso_uid_tt_ads=dac0fab2ffd0377f9ee99824c9fea7e7e34d052431421f08c3c81d2b54b4e500; sso_uid_tt_ss_ads=dac0fab2ffd0377f9ee99824c9fea7e7e34d052431421f08c3c81d2b54b4e500; sso_user_ads=78e27ddaff17e02961fbff0fa941e235; sso_user_ss_ads=78e27ddaff17e02961fbff0fa941e235; sid_ucp_sso_v1_ads=1.0.1-KGJkZDRiYTE5MmI0MDFmZmUwYzUzNjY3YzRmMmI2NDM4M2VjNTA4NjgKIgiUiN7g9dSegGkQ2IDfzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc4ZTI3ZGRhZmYxN2UwMjk2MWZiZmYwZmE5NDFlMjM1Mk4KIJaaEU4-XXfe4BZMG3K9889WdYhUvyhKTH10HvrcaGLXEiDG0iz3AxMzisWISYunUa-TiwPqHOLti6StrW3KB0TXWBgCIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGJkZDRiYTE5MmI0MDFmZmUwYzUzNjY3YzRmMmI2NDM4M2VjNTA4NjgKIgiUiN7g9dSegGkQ2IDfzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDc4ZTI3ZGRhZmYxN2UwMjk2MWZiZmYwZmE5NDFlMjM1Mk4KIJaaEU4-XXfe4BZMG3K9889WdYhUvyhKTH10HvrcaGLXEiDG0iz3AxMzisWISYunUa-TiwPqHOLti6StrW3KB0TXWBgCIgZ0aWt0b2s; uid_tt_tiktokseller=2a257248ffeddbffb18d0571f662584c95c229d829eaa9bd417d02dacdaf5508; uid_tt_ss_tiktokseller=2a257248ffeddbffb18d0571f662584c95c229d829eaa9bd417d02dacdaf5508; sid_tt_tiktokseller=b1a808d4548d99acd66052430df2a272; sessionid_tiktokseller=b1a808d4548d99acd66052430df2a272; sessionid_ss_tiktokseller=b1a808d4548d99acd66052430df2a272; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; sid_guard_tiktokseller=b1a808d4548d99acd66052430df2a272%7C1773650066%7C259142%7CThu%2C+19-Mar-2026+08%3A33%3A28+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CsagI1FSNmazWYFJDDfKicv_________jsO5tnyFhLGGwlDw1EZmAskdLjfS24kcpz1j8XN20xrc%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhkYmZlNzE0NWZkMGE1NTBjZjQ0YWEzZjdiZWRiODYxZWNjNzhlM2IKHAiUiN7g9dSegGkQkoHfzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGIxYTgwOGQ0NTQ4ZDk5YWNkNjYwNTI0MzBkZjJhMjcyMk4KIO-IJ6VQ_K0r_6dDh6RWAXIvPEbCrgp85AbxtDg7A7P1EiAYypS2pXL4loFZ9vIezyja2X-1o2gF9SzA0ALeF6yogxgDIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDhkYmZlNzE0NWZkMGE1NTBjZjQ0YWEzZjdiZWRiODYxZWNjNzhlM2IKHAiUiN7g9dSegGkQkoHfzQYY5B8gDDgBQOsHSAQQAxoCbXkiIGIxYTgwOGQ0NTQ4ZDk5YWNkNjYwNTI0MzBkZjJhMjcyMk4KIO-IJ6VQ_K0r_6dDh6RWAXIvPEbCrgp85AbxtDg7A7P1EiAYypS2pXL4loFZ9vIezyja2X-1o2gF9SzA0ALeF6yogxgDIgZ0aWt0b2s; gd_random=eyJtYXRjaCI6ZmFsc2UsInBlcmNlbnQiOjAuNzQ0OTExMjI0Mjk3OTIzMn0=.kuZJEeAxTlYteeNv9bdkAC0YDdDhmNjlmLhtXubALX8=; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzM4MDkzMjAsIm5iZiI6MTc3MzcyMTkyMH0.xJqLQoKcq2uzslNqrU3ugRMfaVluhq2rLDWjQ-oc9pM; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzczODA5MzIwLCJuYmYiOjE3NzM3MjE5MjB9.YfmswAtGZjO4HzWtTxPDq4MQz61smFSR4Sl3tx-k16I; FPLC=vtKJDnH64fboAzMQhvYcDa8C46zdANZgB4LSMY1V3uSdZaiCk8cHVo3zbuTYYjH34zhWcOUpb95INWYnYZpvsEq5PaNI5Iq4nOCtXsk67fSa68ZqaQYrjfJQBnp0Aw%3D%3D; ttcsid=1773729704810::4DqE3QXxAvYV_Bgu0lil.10.1773729729692.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773729704809::BqtCmwAUHob-1QApKpkd.7.1773729729692.1; _ga_BZBQ2QHQSP=GS2.1.s1773729703$o7$g1$t1773729730$j33$l0$h290354246; i18next=vi-VN; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773736674%7C8f6b5074e3e9cbda9f16d14974a85e6eafeb502dfb3dd7d54fa07b97d446487a; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJHZTZ2UFJtbTJNa2ZvbWxhYnUzZ1lKM2hUU05aQU43U3NNNEVyU1kraUxUSFd0SDdxc203dldnVnVtRGROVnRYbWkvb0ZERmhxeXlRQzZwa2hlbU1TST0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; msToken=Y4lR4cbChCIMbwznacUD3-2UaeFIdZFnHtC7IC6o5uDQVG-KvMtZHF-enH9BQqCeKSogqDYWbZX8Z13ecxFgtATBxlGQEjW_A44p9-bXP9BNHdN27OR9ZLa9XsQZ; odin_tt=9440db4c47be8410cc1f436933858c977c4cf97e28b6b3156f699afdd17d276032ec4e28b9c16ec022446c3c6d70ab7a3549fcacefdf21495a236886274e6bde; user_oec_info=0a53db57151edd695057a9e5bfc6c2542f059e372cfd16100046dcd73217b4b98df88f317645695df96e7070bc0df966cc60e70367a836a1ffeba9d7047078b81857c7cb74d71d29e848c0801d531c84143b4a48c91a490a3c000000000000000000005031c19cf2435c934c4ec5f5c5f081f560fa77a7e334488de6bf8ae62556c11fe6ad14e7d829f26a949f385819c5cf4c447810d3b18c0e1886d2f6f20d22010480d28b58; msToken=np89-l2jwbDiM8fVBW7Hm1sdj0eOvgRqRJ4aVyw49z7LURAxs9Dly3TgZ-cve6V9q_JqUdozHurSdPGjYoa26eKR_Jp9MBSIwKrZPlKrmz3XJll5Kz6ZLmG-cTejL5INH0eMd3k="""
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
def extract_data_by_date(run_date):
    all_rows = []

    report_date = run_date.strftime("%Y-%m-%d")
    next_date = (run_date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n===== Processing date: {report_date} =====")

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

        print(f"Date {report_date} | Page {page} | Rows: {len(rows)}")

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


def delete_and_insert_by_date(run_date, df_final):
    print(f"\n🧹 Delete data of {run_date} in BigQuery")

    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE report_date = @date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", run_date)
        ]
    )

    client.query(delete_query, job_config=job_config).result()

    if df_final.empty:
        print(f"Không có dữ liệu để insert cho {run_date}")
        return

    print(f"⬆️ Insert {len(df_final)} rows for {run_date} into BigQuery")

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
    print(f"✅ Loaded {len(df_final)} rows for {run_date}")


def main():
    tz = ZoneInfo("Asia/Bangkok")
    today = datetime.now(tz).date()
    yesterday = today - timedelta(days=1)

    target_dates = [yesterday, today]

    for run_date in target_dates:
        df_final = extract_data_by_date(run_date)
        delete_and_insert_by_date(run_date, df_final)

    print("\n✅ DONE: refreshed yesterday + today")

if __name__ == "__main__":
    main()


