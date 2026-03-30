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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774671423%7C259200%7CTue%2C+31-Mar-2026+04%3A17%3A03+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CNl44rHhyf0iNxN90xYbMZ__________huEsqr_IGWehVOAuNZ5yefvR6fTMLbJBFIH_W7VHTOew%3D; sid_ucp_v1_tiktokseller=1.0.1-KGE0ZDUwZjU3YTBkNjEwMjMxNjQ4NGMzNDA4MzRiMjg0MjlhMjkzNDQKHAiUiN7g9dSegGkQv6ydzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDM2NWUzOGFjNzg3MjdmNDg4ZGM0ZGY3NGM1ODZjYzY3Mk4KIN_Y592M9F5ESRQpXxlIGTKs8L9f6ClTIJ9cmx9iiRoiEiBQm4eZWNWIEqHCwTQkKBKv5orasswiwoy7-zkDG0cEjxgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KGE0ZDUwZjU3YTBkNjEwMjMxNjQ4NGMzNDA4MzRiMjg0MjlhMjkzNDQKHAiUiN7g9dSegGkQv6ydzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDM2NWUzOGFjNzg3MjdmNDg4ZGM0ZGY3NGM1ODZjYzY3Mk4KIN_Y592M9F5ESRQpXxlIGTKs8L9f6ClTIJ9cmx9iiRoiEiBQm4eZWNWIEqHCwTQkKBKv5orasswiwoy7-zkDG0cEjxgCIgZ0aWt0b2s; part=stable; FPLC=RVvsJoSt%2FDFEF9QWFneHEANbCd0j1%2F6aAEXJ5LCHDY7A7auwjpqXQaUBjO5fYdUu6HvpbnA5v%2FN0%2BaW4r2uh7hBA1RBsx2yTJVDuTk7DIEVnkjE5FfxirdLoRsKDcg%3D%3D; d_ticket_ads=64a949fe585bd0b58050dfea05d2bd987856e; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; ttcsid=1774835513870::Ma6Uv2lb4XYZxVxDqYyX.18.1774835613942.0; sso_uid_tt_ads=52279b88c7863f41421a2c88685eae85eb491888a01aefe349f104b41a2d9bea; sso_uid_tt_ss_ads=52279b88c7863f41421a2c88685eae85eb491888a01aefe349f104b41a2d9bea; sso_user_ads=79307dd1771031e116cbd4e948b1e39e; sso_user_ss_ads=79307dd1771031e116cbd4e948b1e39e; sid_ucp_sso_v1_ads=1.0.1-KGQ4MTU5OGJjNWE1NjU3MjljZDljODVjNTU0OWQwOTI4N2Q2ODNmODgKIAiQiKz8zcvg22kQn6-nzgYYrwwgDDDzhN7NBjgBQOsHEAMaAm15IiA3OTMwN2RkMTc3MTAzMWUxMTZjYmQ0ZTk0OGIxZTM5ZTJOCiA35w77ldLrbcwXAwbtw0tAFYv_0T_MPPjQOtN17XE9jhIgVcyVc_R0mUkeM_iS6U1XU9J1TXfdgGAe510JbnsqVeAYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGQ4MTU5OGJjNWE1NjU3MjljZDljODVjNTU0OWQwOTI4N2Q2ODNmODgKIAiQiKz8zcvg22kQn6-nzgYYrwwgDDDzhN7NBjgBQOsHEAMaAm15IiA3OTMwN2RkMTc3MTAzMWUxMTZjYmQ0ZTk0OGIxZTM5ZTJOCiA35w77ldLrbcwXAwbtw0tAFYv_0T_MPPjQOtN17XE9jhIgVcyVc_R0mUkeM_iS6U1XU9J1TXfdgGAe510JbnsqVeAYASIGdGlrdG9r; sid_guard_ads=f899825006ec49bd881b561b656a5f98%7C1774835615%7C259200%7CThu%2C+02-Apr-2026+01%3A53%3A35+GMT; uid_tt_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; uid_tt_ss_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; sid_tt_ads=f899825006ec49bd881b561b656a5f98; sessionid_ads=f899825006ec49bd881b561b656a5f98; sessionid_ss_ads=f899825006ec49bd881b561b656a5f98; tt_session_tlb_tag_ads=sttt%7C5%7C-JmCUAbsSb2IG1YbZWpfmP________-t15I4P-OyvC9IbsR_9dyr1Y0wH1TFqcxcbGfGG5NHDAE%3D; sid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ac_csrftoken=6fc32da583e44de595ba2e891277cd49; _ga_HV1FL86553=GS2.1.s1774835513$o2$g1$t1774835616$j60$l0$h508160841; _ga_Y2RSHPPW88=GS2.1.s1774835513$o7$g1$t1774835616$j60$l0$h125334612; pre_country=VN; msToken=lX_CI-tnVmzx_l4lEQE0vLwVD55Yyzg6Xp9AlHI-dThlvvpj6IWZhcyVA8HUBlRFwteeerxNnjRGzUNWW4YDLGyJNxTsGN-3m3bnm5OuMQBkk-hs37dyOtV97UXz1a8wuSBIZ5131ILnM9TzOV6KDOvcm3Q=; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ5NDg4MDYsIm5iZiI6MTc3NDg2MTQwNn0.51Ghcv4gm5dD-EIfeKmaiyLiVhxi0su6g_BZ9io9Rc8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0OTQ4ODA2LCJuYmYiOjE3NzQ4NjE0MDZ9.OL-fidQQ5bZZ6hvVaYYYR1tPJI9Z4vg3sA1zP38m-IQ; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774867035%7C7b5d594d98b8a64564b6ba7cef37872c83ad3c4c1e058cca548636641696e29a; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a538dfa006f1f52a03f33d4b71cdb8a3fff461ea9e51d5457e5dcfc3dd6ef5c64b0e4679a94206524e47ed211b638a08447bd413081eadf09c6ae14bb0cb86575d2be3b7611bb8df1db3404bf0904bd6465fe111d1a490a3c00000000000000000000503e5906d45b21db3e63ad39ec6cc8fb775a118d12ceec8eb52d987f192bf805ced3aaa7069a93c4ee2249d7524de5dae85110fabd8d0e1886d2f6f20d2201045c1ad92c; odin_tt=27f37ff704bfa92937383235a627dc9be8deff7e0da329a2dd68513330f65f397b6df2d968145f76ff58a82c4c99e21e0515067cee13f679a6c547f3385fedfa; msToken=biUNIUXBJjKe3LDYXRaoFx0k2MkIPFe9ZTYfJUDxtmAq9PIEgpPN9-wBRKwjJ5nsaAKcLsDxyHocoLTQ5RPHeoZW0wq7acyz6wtA3-Y3bCiDs7E3iS5DNPGwd67sTRE6rpcpqy8="""
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
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")

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

