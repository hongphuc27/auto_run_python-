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
    "cookie": "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; i18next=vi-VN; tta_attr_id_mirror=0.1775537767.7625876643680124948; kura_cloud_uid=cbc02e4016d3b9499b0efa8b9ebed0c4; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; _ga_HV1FL86553=GS2.1.s1776763918$o2$g0$t1776763918$j60$l0$h350665806; _ga_Y2RSHPPW88=GS2.1.s1776763918$o2$g1$t1776763919$j59$l0$h1239915838; sid_guard_ads=28c9e866d0a9f385ee340f4869f64f3d%7C1776931942%7C259200%7CSun%2C+26-Apr-2026+08%3A12%3A22+GMT; multi_sids=7135410250375021595%3A4d5fb62310253ef04b41c1f4e1c524bc; cmpl_token=AgQYAPOF_hfkTtKzOP923HodIvO1FNm4H_-SDmCgc_Y; sid_guard=4d5fb62310253ef04b41c1f4e1c524bc%7C1777013931%7C15552000%7CWed%2C+21-Oct-2026+06%3A58%3A51+GMT; uid_tt=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; uid_tt_ss=817729a3893c4b9ad69e6b43d823e7d8a51bf3d234c533110be33a2cae9d4a9b; sid_tt=4d5fb62310253ef04b41c1f4e1c524bc; sessionid=4d5fb62310253ef04b41c1f4e1c524bc; sessionid_ss=4d5fb62310253ef04b41c1f4e1c524bc; tt_session_tlb_tag=sttt%7C3%7CTV-2IxAlPvBLQcH04cUkvP_________8T6jm_hyUA4m90n_mH4HKK8dgA2Nrc_gvk5hQ8jmvNU0%3D; sid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; ssid_ucp_v1=1.0.1-KDBhMDNkNmI0NWY4Yjg4ZGU4YmVjM2E2ZDlmN2JjMTU1YmE5OGNhMTQKIgibiKOilbqEg2MQq6mszwYYswsgDDC1pJiYBjgHQPQHSAQQAxoDc2cxIiA0ZDVmYjYyMzEwMjUzZWYwNGI0MWMxZjRlMWM1MjRiYzJOCiADSzQMSt8b18VdiCUn64H5Re-TN8rCHFH2EZXkAC7uEhIgMiqBo78Jm_7-eAtDHTGcz3fYoVCjXv1HezkmBxIgBNYYBCIGdGlrdG9r; store-idc=alisg; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign=gbWVBI0zJ2z1oO_a2TqqdeX_u1EnWnVMS8Cw1GrxNOdR6Oo7Ii0dFhFnZ9IYznFuW-1s_BBjxx9ONLmRGRhWUSD4NdH_4xh66Q2au3OCajdBngNv_uU4wUkW6eXTjpP0nzp4Q9KFW4IZfzhE-R8Y_nSGexIYQPd91GI49LaBfo7SMDKxsc2hieNcVTB1ugwuMiRo-k0ShfyA58bWpU2oVMhuXB3sY9fqrzuIEh9Ahs5lVqdnFmdaoTcGRxK8_HeDvMa0FEoE9Tqu9-ZtV4LgQVjT1Qg4EXE8RH-li-Eb8df5RcV4eJ7IrXFdVqAPJeTwk6stdXNXsOtF9NOogHVRiu1-4OD83LDhdBYo4KyqC0GHPurxjiZdL8ToNIVpQcG3DkQE7oaFy1yaGju1UKb-g-Q1XxLJafcufntkwhnH7OnzEJuhCTHyoS_NufgU4xhk9BIw2xTHe4FD9XLjG07M9AR09VXPvEkwaILGtbLrs5yzmFqQPva_vQz6ihLgh0PM; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; _m4b_theme_=new; store-country-sign=MEIEDJrrjhF0UYMmze-hJgQgtBXrbFgn9zT0pHHPFgkCcqws_UQrNtf6_XZkOdHY6yoEEFyR-HbK12kNipK0qziPw98; oec_lucifer=AQEBADBIu1sV/SjNnMtQg3Adlmr0QZMQTFtoURhK32L+ZoBNGxaxIh1+ITUW0jiW0GalAEjEYtFusX4yFxNgRPm3bxukJTk2tQ==; s_v_web_id=verify_mp7of1pc_a77BKVZQ_KJ42_4oPr_8HM8_JU6R8zipEt4D; FPGSID=1.1778895447.1778895447.G-BZBQ2QHQSP.dXsK3ITeArKbvL_Gg17dLw; _ttp=3DbaseAXzK8bb2t9zERxXQChg3F.tt.1; FPLC=CTM4roNxDiev73vyFiA%2FyZ7blLAmUd%2B1%2FH3wVnDlEy%2FIip%2B7f7MIleOU5S%2Fz8kYmboya8u9zY4g%2B3Fas9HWWWwa0RhZ%2BLkoWXW6O4FqU0RI1FgMKOkaFuK7VJ6HWUQ%3D%3D; ttcsid=1778895447326::ejTTqaBiYBv7YIW0JvAy.13.1778895494098.0::1.-2090.0::46769.14.1046.563::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1778895447326::DAdBmKQtMrZBrsTWWCCP.13.1778895494098.1; sso_uid_tt_ads=9e6e4ebbc7fad7c1620d3b62aff13f59c2160b562e9cf534e406193461ab598b; sso_uid_tt_ss_ads=9e6e4ebbc7fad7c1620d3b62aff13f59c2160b562e9cf534e406193461ab598b; sso_user_ads=15a097dfd5110079a318fea8c88fd3ee; sso_user_ss_ads=15a097dfd5110079a318fea8c88fd3ee; sid_ucp_sso_v1_ads=1.0.1-KDVlMzg3NTVhNmJhNGEzMDJiNDQ5YmI3NTkzOTExOGY5NWE3YzkwMDUKIgiUiN7g9dSegGkQiJWf0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDE1YTA5N2RmZDUxMTAwNzlhMzE4ZmVhOGM4OGZkM2VlMk4KINZgnbAY-kCN71El3J1piCuuIXcoIAPGP9M08cLnbgIBEiDxHYkwBT-heerhblJiXo2dpNhUry58zTuW0V8zBD7CNhgFIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDVlMzg3NTVhNmJhNGEzMDJiNDQ5YmI3NTkzOTExOGY5NWE3YzkwMDUKIgiUiN7g9dSegGkQiJWf0AYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDE1YTA5N2RmZDUxMTAwNzlhMzE4ZmVhOGM4OGZkM2VlMk4KINZgnbAY-kCN71El3J1piCuuIXcoIAPGP9M08cLnbgIBEiDxHYkwBT-heerhblJiXo2dpNhUry58zTuW0V8zBD7CNhgFIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1778895447$o13$g1$t1778895496$j11$l0$h265385668; sid_guard_tiktokseller=3c532ff63d296f43e1ba0cbeefc1e622%7C1778895496%7C259200%7CTue%2C+19-May-2026+01%3A38%3A16+GMT; uid_tt_tiktokseller=551909a2ef00078bafad768dd20a4e662bf3ccb5bf9d29d49c39363f39ae1eb9; uid_tt_ss_tiktokseller=551909a2ef00078bafad768dd20a4e662bf3ccb5bf9d29d49c39363f39ae1eb9; sid_tt_tiktokseller=3c532ff63d296f43e1ba0cbeefc1e622; sessionid_tiktokseller=3c532ff63d296f43e1ba0cbeefc1e622; sessionid_ss_tiktokseller=3c532ff63d296f43e1ba0cbeefc1e622; tt_session_tlb_tag_tiktokseller=sttt%7C4%7CPFMv9j0pb0Phugy-78HmIv_________wOmfnyOrGoLhIR1YVBn_Qyr8r5YW9W05PXgsjUk4KlvE%3D; sid_ucp_v1_tiktokseller=1.0.1-KDA5ZTdlNmY2NDAwYjVjZWU3NGEyNDA3N2U1YmNhZWNiMzAyNTZiYWMKHAiUiN7g9dSegGkQiJWf0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzYzUzMmZmNjNkMjk2ZjQzZTFiYTBjYmVlZmMxZTYyMjJOCiBzT4mZN8ZSbbSaDRnhbBswyiZrYCzfZkfy48IkvPJ0RRIgLqMge15Z5p2Uw0pl6pgG57TNsQaOZ3bhNMp4Nd4dE9EYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDA5ZTdlNmY2NDAwYjVjZWU3NGEyNDA3N2U1YmNhZWNiMzAyNTZiYWMKHAiUiN7g9dSegGkQiJWf0AYY5B8gDDgBQOsHSAQQAxoDc2cxIiAzYzUzMmZmNjNkMjk2ZjQzZTFiYTBjYmVlZmMxZTYyMjJOCiBzT4mZN8ZSbbSaDRnhbBswyiZrYCzfZkfy48IkvPJ0RRIgLqMge15Z5p2Uw0pl6pgG57TNsQaOZ3bhNMp4Nd4dE9EYBSIGdGlrdG9r; msToken=Ub0_h-rFSuFTA_PNbDolqb71Es1zm4Vmbu0AAzVp7idM2Au86Lcwkx3fBdIDroxxjvmIHEdz20bflJrk8hBE--2-AZ33aKu02PPnQoz8tjZVqpNJ0M5PSsUpEdjFPA==; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1778895497%7C3f2ee3c8860a4f8957ce4d28949a33b7ce52f9a389e658cc89775a7e89f23129; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3Nzg5ODE4OTcsIm5iZiI6MTc3ODg5NDQ5N30.5-Rvzoi3YUsyCyRTza4tO-Bn7pKLudB5rO5HXuqi7eE; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc4OTgxODk4LCJuYmYiOjE3Nzg4OTQ0OTh9.YcGcBLqHrAny24ZPRV6_kQtLvo6z4CVCLepunP067oM; msToken=-KIbhlOfJdz_HZX3VLrsGNuAHE2j2jfg2zrS1U0ub2Fr3qvfazPRx_e4EoyGTLYOmew7jHH5Owfi3q107HjgzxryvuK000yMap86zTUM4o7z5UqaNF8tRQz1d3YagEdKcYhHu36Qeg==; odin_tt=157faed9355169b723216d86ec50b621fa1380aa85745437b9957ff9b5cffc5f6ab66f72c003b6556e77fe1f7436ae1669ee2c49e4a365d10ef839a260c085bf; user_oec_info=0a53c7b6d5defa2f55281dd32d7a07e597c65e54f815bd237fd292e4e8ea9c457baf4892f08684c9cf3c3ccf0073c5b0cc6c99a187cee50adc124a455dd64465dbb53acfa928119fe5e77c51c7d2e3de14cecba1731a490a3c00000000000000000000506ce31f64e52f64da983906947fead4b5882088d6ed6fecb88cf4dd3489b4a2b6d868507e537e82833801617defde8ccc5110ccc8910e1886d2f6f20d220104a88dba21"
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
    start_date = (now_vn - timedelta(days=25)).replace(tzinfo=None)
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
    
    # ==============================
    # DELETE DATA TODAY + YESTERDAY
    # ==============================
    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 25 DAY)
    """

    client.query(delete_query).result()

    print("Old data (today + yesterday) deleted.")


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







