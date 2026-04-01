import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
import os
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery
import json


# =====================================================
# BIGQUERY  CONFIG (THEO CỦA BẠN)
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_creator_doitac_tiktok"


gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# =====================================================
# TIKTOK API (DÁN TỪ F12 → COPY AS CURL)
# =====================================================
URL = (
    "https://affiliate.tiktok.com/api/v1/affiliate/orders"
    "?user_language=vi-VN"
    "&aid=4331"
    "&app_name=i18n_ecom_alliance"
    "&device_platform=web"
    "&browser_language=vi"
    "&browser_platform=Win32"
    "&browser_name=Mozilla"
    "&timezone_name=Asia%2FSaigon"
    "&shop_region=VN"
    "&oec_seller_id=7494545630022240481"
    "&msToken=N39EX-R9gz5-RWGhKsnkNcTiBj_s9RJywGz1KxvmlifSCTicC2Z84UP1BYyPLNipT9_XC7wx4EAflfJ5T5jO6fenzwTdNjSF3G6012rCC0dO5dOqVZ7TNutSbVVOKQXnQvfyQQ=="
    "&X-Bogus=DFSzswVOy4LxJoKzCix8ZcVRr3Em"
    "&X-Gnarly=McKhhGvnEty2jB08p73KTJbQRgneIjbdWoj7xQcJnriCp5iY-1M7t2TNGlq1bJ43yq8-wP19G5Bgd/Yr-O0zCrmRClC2rUAuU1uJXwrCm37Bbre0Fk9-Q6236es8-uDx6MOlYHkxtOFTWZbtip118dm8E7hwJj-5YztHcX8fpV5YdDEmGUbIkk8mF4STC4/ADhTgbFvFuXkQLfxRV3LbWiZKGdKrT9EU01e1j0Z9P1dxf65b6s0JO76J7SV86-v0Z-YWMq0J-UJw"
)


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://affiliate.tiktok.com",
    "referer": "https://affiliate.tiktok.com/product/order?shop_region=VN",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; sid_guard_ads=f899825006ec49bd881b561b656a5f98%7C1774835615%7C259200%7CThu%2C+02-Apr-2026+01%3A53%3A35+GMT; uid_tt_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; uid_tt_ss_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; sid_tt_ads=f899825006ec49bd881b561b656a5f98; sessionid_ads=f899825006ec49bd881b561b656a5f98; sessionid_ss_ads=f899825006ec49bd881b561b656a5f98; sid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; _ga_HV1FL86553=GS2.1.s1774835513$o2$g1$t1774835616$j60$l0$h508160841; _ga_Y2RSHPPW88=GS2.1.s1774835513$o7$g1$t1774835616$j60$l0$h125334612; app_id_unified_seller_env=4068; pre_country=VN; part=stable; tt_session_tlb_tag_ads=sttt%7C5%7CeTB90XcQMeEWy9TpSLHjnv________-kOZQ-gfgJATLaewZc-YsmvK5DhYfQ5UVl0R61D5_45uM%3D; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; FPLC=l8qoyvg4VjVFU1tOPj0gmuiSND1kTlxTodp1MQOBrL0jlctxtuNFn9V7Qcajzwn9cwxatuvhRF9mykb8sZ2R5zGoREVii4pNco8dROlihoswcccpDqhsySnjwirkEw%3D%3D; store-country-sign=MEIEDF3Y0G9QIYyWmWmpkgQgwnfEpBduknwure1rT6lMk36oyvzKBDm9Zs4QeJJuMfEEEObOAhw6z3nxkvba-wp00Ik; ttcsid=1774941273302::U1-Fjv6__OEQU9nEFqmB.19.1774941295280.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774941273302::MgCW38qV0Mk0BEBfoLXk.14.1774941295280.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; sso_uid_tt_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_uid_tt_ss_ads=127a60fa632ca9e437e4441607482bac1da7ab4051b041446204a42bb706b737; sso_user_ads=59dc308787ebd574297f1aa3d66152e3; sso_user_ss_ads=59dc308787ebd574297f1aa3d66152e3; sid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDVkMzJhNDM1NTdkNzIzYjAzMzZlNzlhNmNhOTY1ODZiY2NjMDlhMTQKIgiUiN7g9dSegGkQ8eitzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiA1OWRjMzA4Nzg3ZWJkNTc0Mjk3ZjFhYTNkNjYxNTJlMzJOCiBkFo3mHZTcupgZLl8MBd-Y4J8qxjFa0H7IY1KZ2wxKCRIgMMZMBo3PAwMya_foCqeukRHRncZxjmvuy5Vv3jVdqDwYBCIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774941273$o15$g1$t1774941297$j36$l0$h1797522778; sid_guard_tiktokseller=3823c1a4e22721a66f3de0976fc4861e%7C1774941298%7C259199%7CFri%2C+03-Apr-2026+07%3A14%3A57+GMT; uid_tt_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; uid_tt_ss_tiktokseller=16f8bc8b83da780c1823f4e47090b4cf6c1a494f06b8311adc3a99d5389cec1e; sid_tt_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; sessionid_ss_tiktokseller=3823c1a4e22721a66f3de0976fc4861e; tt_session_tlb_tag_tiktokseller=sttt%7C1%7COCPBpOInIaZvPeCXb8SGHv_________LHeMVYk5Fl2CDKeV0khBmhOnBNjaI8VV2xOKbs0lgOlE%3D; sid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDYzMjdjM2U1NzkyYzJmNmQ5ZDIyYjc4YjRkNWMyMTYyZDcwZjRlZTYKHAiUiN7g9dSegGkQ8uitzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzODIzYzFhNGUyMjcyMWE2NmYzZGUwOTc2ZmM0ODYxZTJOCiBrE6bQZZrJ7AvbK4mF3EF20e50iz5SX5E6kh0YG-5ldxIgv-WjxA-3b4mlChatpGvoEvuHhPl8F6Y0IbLX8nE891AYBSIGdGlrdG9r; global_seller_id_unified_seller_env=7494545630022240481; oec_seller_id_unified_seller_env=7494545630022240481; msToken=HrEDPzQEv6l5S_fFD2w1NHsTbZE42n3i7L07XRsIlgULiJSxKunkMUgahrls8WFMr5cMU7xtG76j8Gs8RMnXoBrTjjmYEDnIay6vYHJhANlxzhij97bbpG3dMe4Zqw==; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774941351%7C912edb2f19e5359305c32aa98f61549c7d83479664c7e70c92e5f00fbff3a845; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=e80e7f62818b9622b2f6b4230b83e08f2b32f18fc52694dd7c19644039d2d48f8764df451ad6fa789389c009a9615046682d4a4775bccfd7318dda6accb0b8de; user_oec_info=0a532d500a8f280435f711911d0a47f3659da0893d34da57a6f61dc30fb727610c55a1bbbb2483b411590ba89ed6e8155f59838d781c95eece18140d0e074d56ed8d6aef9d9f7b4e9e5d6c9b04d984c5e31327f9581a490a3c00000000000000000000503fb20d00617928e1ce9f0f4930fea0c5af352ecfd100e63c3b8b65d3929fbda23ebe6029218d5739ee2b00e850c0b894b410dccf8d0e1886d2f6f20d2201042f5419e2; msToken=nmnX9lcYshiQv3YfU9Jh2uLRIpzaC_80nE_8AnJaOOKEinNrJQ1YCb-4seQ1VMXj_EyMyai_uwg_Y0TzB1wLaqoVXLiKyNac44UG4oi2WVrjRJRJviUG2DozTwVkQhNWkjNfokJR"
}


# =====================================================
tz_vn = timezone(timedelta(hours=7))

# =====================================================
# =====================================================
def fetch_page(page: int, start_time, end_time):
    payload = {
    "conditions": {
        "time_period": {
            "beginning_time": str(start_time),
            "ending_time": str(end_time)
        }
    },
        "page": page,
        "page_size": 100
    }

    r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


# =====================================================
def run():
    all_rows = []

    now_vn = datetime.now(tz=tz_vn)
    start_date = (now_vn - timedelta(days=6)).replace(tzinfo=None)
    end_date = now_vn.replace(tzinfo=None)

    

    current_date = start_date

    while current_date <= end_date:

        print("Fetching date:", current_date.date())

        START_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            0, 0, 0,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        END_TIME = int(datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            23, 59, 59,
            tzinfo=tz_vn
        ).timestamp() * 1000)

        page = 1
        empty_retry = 0

        while True:

            try:
                data = fetch_page(page, START_TIME, END_TIME)
            except Exception as e:
                print("API error retry:", e)
                time.sleep(5)
                continue

            orders = data.get("orders", [])

            print(f"Page {page} | Orders: {len(orders)}")

            if not orders:

                empty_retry += 1

                if empty_retry >= 3:
                    print("No more data for this day.")
                    break

                print("Empty page -> retry")
                time.sleep(2)
                continue

            empty_retry = 0

            for o in orders:
                main_order_id = o.get("main_order_id")
                create_time_ms = o.get("create_time")

                if not main_order_id or not create_time_ms:
                    continue

                create_time = datetime.fromtimestamp(
                    create_time_ms / 1000, tz=tz_vn
                )

                sku_details = o.get("sku_detail", [])
                for sku in sku_details:

                    sponsor_id_raw = sku.get("sponsor_id")
                    sponsor_name = sku.get("sponsor_name")

                    # cast sponsor_id an toàn
                    try:
                        sponsor_id = int(sponsor_id_raw) if sponsor_id_raw else None
                    except:
                        sponsor_id = None

                    # ❗ SKIP nếu BOTH sponsor_id và sponsor_name đều trống
                    if sponsor_id is None and (sponsor_name is None or str(sponsor_name).strip() == ""):
                        continue

                    sponsor_service_ratio = sku.get("sponsor_service_ratio")
                    estimated_sponsor_cos_fee = sku.get("estimated_sponsor_cos_fee")
                    actual_sponsor_cos_fee = sku.get("actual_sponsor_cos_fee")

                    shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
                    estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")
                    actual_shop_ads_commission = sku.get("actual_shop_ads_commission")


                    all_rows.append((
                        int(main_order_id),
                        sponsor_id,
                        sponsor_name,
                        create_time,
                        sponsor_service_ratio,
                        estimated_sponsor_cos_fee,
                        actual_sponsor_cos_fee,
                        shop_ads_commission_ratio,
                        estimated_shop_ads_commission,
                        actual_shop_ads_commission
                    ))

            page += 1
            time.sleep(0.3)

        # sang ngày tiếp theo
        current_date += timedelta(days=1)

    print("TOTAL ROWS TO INSERT:", len(all_rows))

    if not all_rows:
        print("NO DATA TO INSERT")
        return
    
    df = pd.DataFrame(all_rows, columns=[
        "main_order_id",
        "sponsor_id",
        "sponsor_name",
        "create_time",
        "sponsor_service_ratio",
        "estimated_sponsor_cos_fee",
        "actual_sponsor_cos_fee",
        "shop_ads_commission_ratio",
        "estimated_shop_ads_commission",
        "actual_shop_ads_commission"
    ])

    # =====================================================
    # FIX DATETIME

    df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

    def to_decimal(x):
        try:
            if pd.isna(x):
                return None
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None


    # =====================================================
    # CONVERT NUMERIC
    # =====================================================

    df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
    df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

    df["sponsor_service_ratio"] = df["sponsor_service_ratio"].apply(to_decimal)
    df["estimated_sponsor_cos_fee"] = df["estimated_sponsor_cos_fee"].apply(to_decimal)
    df["actual_sponsor_cos_fee"] = df["actual_sponsor_cos_fee"].apply(to_decimal)

    df["actual_shop_ads_commission"] = df["actual_shop_ads_commission"].apply(to_decimal)


    # =====================================================
    # DROP DUPLICATE

    df = df.dropna(subset=["main_order_id"])
    # df = df.drop_duplicates(
    #     subset=[
    #         "main_order_id",
    #         "sponsor_id", 
    #         "create_time",
    #         "sponsor_service_ratio",
    #         "estimated_sponsor_cos_fee",
    #         "actual_sponsor_cos_fee",
    #         "shop_ads_commission_ratio",
    #         "estimated_shop_ads_commission",
    #         "actual_shop_ads_commission"
    #     ]
    # )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # # ==============================
    # # DELETE DATA TODAY + YESTERDAY
    # # ==============================
    # delete_query = f"""
    # DELETE FROM `{table_ref}`
    # WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # """

    # client.query(delete_query).result()

    # print("Old data (today + yesterday) deleted.")


    # =====================================================
    # BIGQUERY LOAD
    # =====================================================

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )

    job.result()

    print(f"✅ Loaded {len(df)} rows into BigQuery")

# =====================================================
if __name__ == "__main__":
    run()







