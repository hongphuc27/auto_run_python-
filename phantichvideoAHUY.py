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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; FPLC=m%2BbWtTwLgm2Gjk1B5ajmnlcKmVWPTC6YfpOmore7wkf5txgFasXSL1TCoHOltuX%2FBjCm0pUxCGcFbpSnrKC8xRuCn8DMgslfhTM%2FUiuCGXWXfUrIwKg9Fx9HXx7%2FkQ%3D%3D; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI.tt.1; ttcsid=1776703789312::Zqs_hQMECBQnDIxcCCnB.5.1776703822619.0::1.-5027.0::33302.11.673.477::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1776703789312::RTb_vX0skd9ZysWHByR1.5.1776703822619.1; sso_uid_tt_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_uid_tt_ss_ads=73d2f51b42b1f42e431a0b0e83bec8d044e99d7d28d6234ea5640dde31c55621; sso_user_ads=a92756a08698fcc35544bb54e270ef04; sso_user_ss_ads=a92756a08698fcc35544bb54e270ef04; sid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDNlYzliYzFlMjc0Y2IyY2IwYmQwNzNlY2Q0Nzg2NWRhMWJkZGE3OWEKIgiUiN7g9dSegGkQ0bKZzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiBhOTI3NTZhMDg2OThmY2MzNTU0NGJiNTRlMjcwZWYwNDJOCiASRcInQFAOrtKpClGze6LQxC3HtUsD69-f0N6cP9oJYRIg9WJkUqtlhYl9wQVn-XAHqZ6yy17jcZ431wBxhr81yaAYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776703788$o5$g1$t1776703825$j23$l0$h328177141; sid_guard_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27%7C1776703825%7C259200%7CThu%2C+23-Apr-2026+16%3A50%3A25+GMT; uid_tt_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; uid_tt_ss_tiktokseller=89700e5b74b9da8363ee35cb971f41cb2ce1d784c09a7c49c320561f048e9baf; sid_tt_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; sessionid_ss_tiktokseller=ab4dee1e3332218abce0e8734aeb2d27; tt_session_tlb_tag_tiktokseller=sttt%7C5%7Cq03uHjMyIYq84OhzSustJ__________7hEyD0wF4ndOX2NXXZpZnPJwAQP9zGzJFzvwKgUMFezk%3D; sid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGFhNzc5Zjk5MWY2ODg0NWIzOWRlZjI2MWIxYzNmN2IwMDc3ZGU2MzkKHAiUiN7g9dSegGkQ0bKZzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBhYjRkZWUxZTMzMzIyMThhYmNlMGU4NzM0YWViMmQyNzJOCiBetX15Mpsy1qdgMhSnSBLpAPj5Wp2wJRVSwXI8ZT_MCBIgm6aeDwWPuoHhxRoMaVT7Mbj8k6btrsZzGiI90c8r3nsYAiIGdGlrdG9r; msToken=lDBJbWpzw_j1ohK267hJAR_9vqo6mOAO89F3mifkSLhcVU9baY0rF5Ll3PsdhGcgJUwLzpjBNHM-y0ZkDvG15n6S4QyxXMVIWT06StwoD97jDN8sHzpye5NfVVIjPg==; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776703928%7Cf663a35b310c322ce2733d4dbc9dc8afe297e10297f4eadfcb995e027100410b; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=92aa5d4aa753744c4082431f0d62d44425c405b1651f5399bd47ecb4bef3c197e24a587dfd51ac54d282d6f27c7b2f41c144136efe523038455198636215aee1; msToken=WKjvB2OT2EMiSVJ5kbxghhbczITQ5rwrkeVr9PLUgCuSOICGZJuH1qXbHQ0R8P5Ooh7aOXMvaoYw6jvVyIJ7LRJztZaGtYs318zOYn9AodHtEd6j0S7VBoBCsT272Y0jjTEDXdo=; user_oec_info=0a532c6d2d92b2eac01c6f0df3f3bd846d545bf214ea563eab25f8f31b4c851434a2c6dfad9d35cb2291a99dfcb004aff4cc60764c17c56ff3b98ec5753f902927cf22440b9931e6c6d042ebc517fd4286fc4975c41a490a3c00000000000000000000505450fdee28d18af6119c80f0501251fe89dea61766ded3562adcd6d0612f7d2a9461426bf8693f4f7bed3fc78a4aad07c81090b28f0e1886d2f6f20d220104e412e71f"
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

