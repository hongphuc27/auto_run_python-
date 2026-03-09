import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
from decimal import Decimal, InvalidOperation
from google.oauth2 import service_account
from google.cloud import bigquery

# =====================================================
# BIGQUERY  CONFIG (THEO CỦA BẠN)
# =====================================================
PROJECT_ID = "rhysman-data-warehouse-488306"   # 🔥 thay bằng project GCP của bạn
DATASET_ID = "rhysman"
TABLE_ID = "fact_creator_doitac_tiktok"

credentials = service_account.Credentials.from_service_account_file(
    r"E:\hongphuc\Source code\code kéo dữ liệu SQL Sever (Thảo)\rhysman-data-warehouse-488306-8db2b940e56a.json"
)

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
    "cookie": """tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; d_ticket_ads=3efc70f037472f37f0a99d03fb4c56117856e; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; _m4b_theme_=new; _ttp=3ASnmGtCNVoiR5Q0m1ZGjkJ1Nuz.tt.1; i18next=vi-VN; tta_attr_id_mirror=0.1772797845.7614108765217882120; ttcsid_C97F14JC77U63IDI7U40=1772797849194::GdOHloCKF5kUGxOaQEGn.1.1772797859207.1; sid_guard_ads=335356ce1cf65c60fa6c0d0e292dca5a%7C1772869877%7C89785%7CSun%2C+08-Mar-2026+08%3A47%3A42+GMT; _ga_HV1FL86553=GS2.1.s1772869878$o2$g1$t1772869878$j60$l0$h1122399611; _ga_Y2RSHPPW88=GS2.1.s1772869878$o2$g1$t1772869878$j60$l0$h1899419535; s_v_web_id=verify_mmijlar6_Xt9DVEcZ_pYgK_4OxY_9FIs_6B9bZHcKH0Oy; FPLC=LNZodwkNimSMVjW6WFVE66QwDL8ol8wR4hy%2FXoMMBlUu4iOb269ct6u2Z4Y2bwsRG5kCcQYdPRB7CsIacAOCxAftvBzbg8kFWaviD1jb5mKgDuO9sS9PPSt3e99A0g%3D%3D; store-country-sign=MEIEDC3ST5uwHx8aAFp8CAQgV2xAfC61tk4WptwgT04YNprDgiy6W2zK-fZcZ7cccaoEELvAQDlAVaqiW-6Aix96UdU; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1773022155%7Cdcc0d97e7dfc595ba4efa84c945238f9a3247c65ca0d279f41db3aee33caaae2; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; ttcsid=1773022140874::YguiMQtFJT3CgwZMtJd5.4.1773022188171.0; ttcsid_CMSS13RC77U1PJEFQUB0=1773022140873::hySuDb_bJFKkYHKrurm7.3.1773022188171.1; sso_uid_tt_ads=e37e8e651d92f76fefc959fdab97412133445104e644eb610faa167b7a59c6db; sso_uid_tt_ss_ads=e37e8e651d92f76fefc959fdab97412133445104e644eb610faa167b7a59c6db; sso_user_ads=15d2227ea4f089c8d6f4d90659e85f2b; sso_user_ss_ads=15d2227ea4f089c8d6f4d90659e85f2b; sid_ucp_sso_v1_ads=1.0.1-KDYxZDYzY2ZiMjk0MWI0YWZiOGNiOThmOGIzZWUwNGRkM2FhYjliODUKIgiUiN7g9dSegGkQ69e4zQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAxNWQyMjI3ZWE0ZjA4OWM4ZDZmNGQ5MDY1OWU4NWYyYjJOCiBVWP6HY9kXH6Uj4rrUPsSJqHZt8HNH3EbEi0BMLoezgBIgVoEPYMvlnE2XvdmJysIQbwvILmBgb9wWOyj0EIN1q_oYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDYxZDYzY2ZiMjk0MWI0YWZiOGNiOThmOGIzZWUwNGRkM2FhYjliODUKIgiUiN7g9dSegGkQ69e4zQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAxNWQyMjI3ZWE0ZjA4OWM4ZDZmNGQ5MDY1OWU4NWYyYjJOCiBVWP6HY9kXH6Uj4rrUPsSJqHZt8HNH3EbEi0BMLoezgBIgVoEPYMvlnE2XvdmJysIQbwvILmBgb9wWOyj0EIN1q_oYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1773022140$o3$g1$t1773022189$j11$l0$h1604944202; tt_ticket_guard_server_data=eyJ0aWNrZXQiOiI2ZmQ5NDVkMThjOTYwODRlZTM4NWFiOTEzNjAzZWNmNDc4ZTQ1ZGVmNjQ0N2YxOWQxMzUwOTY1Y2MwN2U0MDY0IiwidHNfc2lnbiI6InRzLjEuY2Q1MTIzMDQwNWY2MTA3Mjc5N2I3ZjU0OTljODI0NDE0YjQwMjIwYjU3MDViZTVhNDExNDFhODg5ODI3MDkzYjBlNzBiNGJkYTgyYzEzODM2ZTVjZmExODM5NGQ3MDI0MGY4YWYxNjMxZjE2NWFlOTYwMTIyZWVmZmQ0NTMzZGQifQ%3D%3D; tt_ticket_guard_web_domain=2; sid_guard_tiktokseller=019b71acfb4910d32a38b1541ab64128%7C1773022188%7C259199%7CThu%2C+12-Mar-2026+02%3A09%3A47+GMT; uid_tt_tiktokseller=406e8b244bd62d66f638a0faf1271be5708281aad3987dd4cd2f5dfe6f7d52cf; uid_tt_ss_tiktokseller=406e8b244bd62d66f638a0faf1271be5708281aad3987dd4cd2f5dfe6f7d52cf; sid_tt_tiktokseller=019b71acfb4910d32a38b1541ab64128; sessionid_tiktokseller=019b71acfb4910d32a38b1541ab64128; sessionid_ss_tiktokseller=019b71acfb4910d32a38b1541ab64128; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CAZtxrPtJENMqOLFUGrZBKP________-yeP7HarL7QP19j2iEHJ9gdlbahddQ_yzvc-2EpfytUaE%3D; sid_ucp_v1_tiktokseller=1.0.1-KDg3Y2FlNzk1MjA0MThmNmE4YjZhYjNiYWU1NjQ3ZDhkYWU3NjBkYWIKHAiUiN7g9dSegGkQ7Ne4zQYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwMTliNzFhY2ZiNDkxMGQzMmEzOGIxNTQxYWI2NDEyODJOCiCkGuoRjxiht4XrCDkNW8gobsG_XfryDBcDqE6eFtfesBIgcssK6Q7haIuwNL9moTNVP7tMrGmWU0sTWRT5cxyeVbQYBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDg3Y2FlNzk1MjA0MThmNmE4YjZhYjNiYWU1NjQ3ZDhkYWU3NjBkYWIKHAiUiN7g9dSegGkQ7Ne4zQYY5B8gDDgBQOsHSAQQAxoDc2cxIiAwMTliNzFhY2ZiNDkxMGQzMmEzOGIxNTQxYWI2NDEyODJOCiCkGuoRjxiht4XrCDkNW8gobsG_XfryDBcDqE6eFtfesBIgcssK6Q7haIuwNL9moTNVP7tMrGmWU0sTWRT5cxyeVbQYBCIGdGlrdG9r; msToken=sLl8SJjB_wp_YdBa6xxCEh0K-i9nzWxDAwOkEwn-ycqF_PHcAoJBsXO0wyBBVIMlGHCzHITCSbQFPKgEWadNTb2goodx70OBxJv37Bxm99BuxDIQ3cwnrKJu_mVlVE1-Brr9CtZU; odin_tt=939e76f8acec1705e8356221bc793a8150c2362f377d3efaf2b033ee53a025f1f05bccc0faa73e4ba50dc2f39e7b639dc01a3933aaca3d852922f7ef47b41612; user_oec_info=0a532ea8cfaec6266f28d8926856f53353b6a1caf9dd6d8dfc982cafde78e737b4dfaccbd11dee83517c1bddba4f15f48e009b5387c445430cd9fff307ec92dd55a6ccb8e171e24292b3a025cfe0d82877d8798c281a490a3c000000000000000000005028c81bebf062e785563b926e1d3a3c889e09c74275c9e94860f97326bee6936938793d196f734e4f84a1fbb670d8aa8f5e10efcc8b0e1886d2f6f20d2201041edbac0d; msToken=2P_M581iRWgQs8dWJT_YQiJe99nVYmh9dyvXNe6FPlnYshhtem_OH1Eu6Y3ILf5HSmqxw5Us1FG0MtDS46cje6bOSmcedR3yI3KtC2u7PPc8yOUI3q-ecpSxG1DadXo1RGJoPkA="""
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

    # start_date = (now_vn - timedelta(days=1)).replace(tzinfo=None)
    # end_date = now_vn.replace(tzinfo=None)

    # start_date = datetime(2026, 2, 1)
    # end_date = datetime(2026, 2, 28)

    # Kéo từ đầu tháng
    start_date = now_vn.replace(day=1).replace(tzinfo=None)
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
    df = df.drop_duplicates(
        subset=[
            "main_order_id",
            "sponsor_id", 
            "create_time",
            "sponsor_service_ratio",
            "estimated_sponsor_cos_fee",
            "actual_sponsor_cos_fee",
            "shop_ads_commission_ratio",
            "estimated_shop_ads_commission",
            "actual_shop_ads_commission"
        ]
    )
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # # ==============================
    # # DELETE DATA TODAY + YESTERDAY
    # # ==============================
    # delete_query = f"""
    # DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
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
        table_id,
        job_config=job_config
    )

    job.result()

    print(f"✅ Loaded {len(df)} rows into BigQuery")

# =====================================================
if __name__ == "__main__":
    run()
