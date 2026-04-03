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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1772797845.7614108765217882120; kura_cloud_uid=e84062d56ec7c20c87e4d6c1b2463d22; _m4b_theme_=new; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy.tt.1; d_ticket_ads=7c98ff4ee10c146a36303e96aa9ba48e7856e; sid_guard_ads=fba832a9ef3bf1c561723599114d963c%7C1775009054%7C191443%7CFri%2C+03-Apr-2026+07%3A14%3A57+GMT; _ga_HV1FL86553=GS2.1.s1775009054$o3$g0$t1775009054$j60$l0$h647018127; _ga_Y2RSHPPW88=GS2.1.s1775009054$o8$g1$t1775009054$j60$l0$h566538698; s_v_web_id=verify_mniqi3qp_28pRpboY_qxHN_4m8S_BzQU_ipQ9Oigcrcgl; store-country-sign=MEIEDKm9OTjpJX0F_q_2OQQgdc5LHZLuZPjyHbu97JVFpxq4xnpmVDowX4ncnPjUc5sEEMD9Ifw7rK9aIfR54bWjA6Q; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1775210519%7Ca2435e0ee6f9f8b3b0a2ede2eae51b51df85c81b27c222e89624749b2f78531c; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; FPGSID=1.1775210521.1775210521.G-BZBQ2QHQSP.8-anX_cWY-64v3GgJgXsGg; FPLC=fAERuVxGc%2B0FAd4VJWd99RGUE0Hf%2BcW0B7BbDhimy%2FVyihoDUO3TWwepVIZUT%2FoFZfHzwPHUaJHukpaEmtTKyr6Aeq9SdxhNwozvgB8eFlyoY9cQg5R0Pgi5L1IIDw%3D%3D; ttcsid=1775210520574::lpxhLY8mU4xaHlASeJaU.20.1775210535138.0::1.-2143.0::14563.15.1055.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1775210520573::joAYUSMCvqlYWRCVOp4B.15.1775210535138.1; sso_uid_tt_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_uid_tt_ss_ads=31451462985e265c5cb180a0ce064ba86572abf4fcc140b080b1bbf5d743ce0c; sso_user_ads=c103e6271279d24e6c4061d2c2613fbe; sso_user_ss_ads=c103e6271279d24e6c4061d2c2613fbe; sid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDhhMmU2Mjk1YTk0NWIyNDAwZGU3MDI3ZTc2YjFjYmJhY2E4OWJiMGUKIgiUiN7g9dSegGkQqaC-zgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIGMxMDNlNjI3MTI3OWQyNGU2YzQwNjFkMmMyNjEzZmJlMk4KILnbWD_VZxXcamFCV_EyJAR0Pcd9P6R9QBg8XJquXiTaEiCWBhzHe2gHO_mprFtO9iEqT91v8L1HjRZf9_OclsQOrRgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1775210520$o16$g1$t1775210537$j43$l0$h2111997584; tt_ticket_guard_server_data=eyJ0aWNrZXQiOiI5ZTMyYTA2NmU5N2VlM2Q0ZmVmMGNjZmJhMGQxNzQwNGExOThjZWZmOGY3MzQ1Y2YzMTE5NmE3NTQwZjcxOTRkIiwidHNfc2lnbiI6InRzLjEuN2Q1OTEyMjYyMGMxYmQ4OGNkODg1NDMxYzdhNGVjNTkwNzYwNDA5NDBiNzhhNGZmNDJlMGExNmUxY2NlYTg5MTBlNzBiNGJkYTgyYzEzODM2ZTVjZmExODM5NGQ3MDI0MGY4YWYxNjMxZjE2NWFlOTYwMTIyZWVmZmQ0NTMzZGQifQ%3D%3D; tt_ticket_guard_web_domain=2; sid_guard_tiktokseller=b22af622952384da29ce9cbafafbc01c%7C1775210537%7C259200%7CMon%2C+06-Apr-2026+10%3A02%3A17+GMT; uid_tt_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; uid_tt_ss_tiktokseller=40ebd45fb2bb5999431dc3bc8dd26ff9e12719038351f70df93fd0f2b34ade57; sid_tt_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_tiktokseller=b22af622952384da29ce9cbafafbc01c; sessionid_ss_tiktokseller=b22af622952384da29ce9cbafafbc01c; tt_session_tlb_tag_tiktokseller=sttt%7C1%7Csir2IpUjhNopzpy6-vvAHP_________WVUUl8r7EX6ao2IxjeENINn0gg-ndpOR0lwM-f4ToXN4%3D; sid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDRlZDM0YjExMTg4NWMzOTdmZDQyN2JkZGZjOTNjNTBkMDNlZDI3MDYKHAiUiN7g9dSegGkQqaC-zgYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiMjJhZjYyMjk1MjM4NGRhMjljZTljYmFmYWZiYzAxYzJOCiAsMRzd580z0ENSEd3eiuHMcP7GeKHvbuktIOo_SzQuoBIgwiMz3ib-j3AEEoeiIOUdMqhQAGSvxZeUe1oN9VwuCQoYAyIGdGlrdG9r; msToken=yPW74SxtrQ1t3Wfynf2mFCF0h6gc7rXx9db7opXzDHQ0ZmllsuO6kyCV3dIhtzAsdCgF5jpfbHldy81rkErlTg7aAwm7fKyGxQbfVGS9cfox2njwfLH655pHlX9aZwkm86vGTEk9; odin_tt=a6a1cc75e86c2456382e32795a95466f4c4af652d76c62ecfd5c14c462a5703d1607d290d9da493a4c3c052d828ae6b41b5029641260cf1481c049e758211c48; user_oec_info=0a530275ed50f657c477583046193c3197c0d7831df17d5f8f3a5fa5403260d786302219e8a3e31950add60587e2bbba34a5d0571d9c2fdbf1c288b83581e16ba49e7b6f14161633fc2a6a0093bc7364bd5661a3701a490a3c000000000000000000005042a43afe0d068f19293dc1a84685861747e711d4bd328beb70d71ba8da1a7408503a245ba31203afe3954b1aebbcdf6cb010f9ea8d0e1886d2f6f20d220104669e5226; msToken=XVAZsblSqQpLnMj-4VBhtuigW62ZM3mn62NTEQ7-szQU0EX9Dm54Le_LT72iHem4PBiJU_1Bb5_tD-FLLx8eqNbvFU85SGqKJj8HwpdWSujGPCJ4cUXQqvtM3yUDtZ3woI5_H8k="
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







