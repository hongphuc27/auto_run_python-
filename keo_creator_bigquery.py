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
TABLE_ID = "fact_creator_tiktok"


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
    "&msToken=ihykpW25F6VdjhjQU1DDg51GKyXup16943IkNc397v0qH4XA_n3jc1xbKm0aR1LIl43zjhxntq9Azpj1XfQYIulASgIHxSHaBT4HYdlWshBzB1TFg4Ju0ZALi-YVa3Y0ZDAqrg=="
    "&X-Bogus=DFSzswVOU8YoToKzCi9B6QVRr3Nw"
    "&X-Gnarly=MauTAkwV9g4WTiyTuFWfK/n0AeNkOgRztKODNVSh6M9DIaOzlP3WoSjeAy/7LGNmU-WY7dBUzWLsXgLZKbUjRgCXX3yuT58Y7/mL/XxgfRSDWMtjgNsMGWe/KVLGtvJ73b5BMFbDHUcdoxcA2RxI63nnVg1HWVd4JEEIsqCkAkXXveeMrRAxxXjzdxRyhsJYj9c5jW0nZtrXcLnRuMxCo0Wcx7ULSD4w9FqcVli1Sg6QbR7UXxmUe8M-cG8dybuoD0n2KrzV7wrY"
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
        "cookie": "tt_ticket_guard_client_web_domain=2; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; _tt_ticket_crypt_doamin=2; tta_attr_id_mirror=0.1775537767.7625876643680124948; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; ATLAS_LANG=vi-VN; ttcsid_C70N19O394AQ13GK2OV0=1777614830703::1f2ajC_LbESJE624Lqy6.1.1777614840719.1; SHOP_ID=7075901688577638662; multi_sids=7102129446639551490%3A643c9420d29a2ba0484686a91634ed00; cmpl_token=AgQYAPOF_hfkTtKysTu91_gdO_O1FNm4H_-SDmCnF_c; sid_guard=643c9420d29a2ba0484686a91634ed00%7C1779533013%7C15552000%7CThu%2C+19-Nov-2026+10%3A43%3A33+GMT; uid_tt=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; uid_tt_ss=8d065adfc3a6cd94d68069440f0d0e788db8203777ec3f8dc3204a845e8b55c3; sid_tt=643c9420d29a2ba0484686a91634ed00; sessionid=643c9420d29a2ba0484686a91634ed00; sessionid_ss=643c9420d29a2ba0484686a91634ed00; tt_session_tlb_tag=sttt%7C5%7CZDyUINKaK6BIRoapFjTtAP________-lDaDBom2-dE5RLNkottixrOAjuo5W6z0ONxuOWW1Rn1k%3D; sid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; ssid_ucp_v1=1.0.1-KGQxNTliMGZiZTBmY2M1ODkwZGE2YTZiM2JhOTBjMzlmOGM3NDQ3M2MKIgiCiKv-paP1x2IQ1YnG0AYYswsgDDDDqr-UBjgHQPQHSAQQAxoCbXkiIDY0M2M5NDIwZDI5YTJiYTA0ODQ2ODZhOTE2MzRlZDAwMk4KIK-Zf1-Gtkjg2C2hnfX--QPD1mubwq0Pu4mAvGMhWfSFEiAjXYovUHbAVwwtI2pKtdt2Ko7IPbh7Tr11uPZAsTPyOBgFIgZ0aWt0b2s; store-idc=alisg; tt-target-idc=alisg; tt-target-idc-sign=hCivmtH9T-JchVysYsCcndlb8cHGF9-pfarOnoDLuelQcasZTJEeCzYUzWLmQUcXBVDpOLROYN6Eo0cVa0XrPiSYoojtVie8mg2ix-5-v-7xQ2Hx_UQqJf4nGqYMfGIO6OQtKFG1qXaJCi78ENGdI-aYnhwLO-ATmMxq4XQTQe2L9jpoQyPI0tPNtf_2P6s4UqHsyDDFONF6-yOVaFGy8bLAYmYlAKxiQMQ3b3kC3_ue1ZoGWNwfgv4bncNUvR-urXV9f_62wiXjFz3C4xX2kccIdhr3gIeOTe0Y74tVGP2lYYyczwjfa_4759jxBxRYziDfTVX3Iu_K8Q6VGOsoWzvY4jFnVmCHC-gohBwzOP-GcsowOZLnovKHJ0JJoQRHVy5xivn57IB2cU2mq8KAfyMV2qrTQvXGWPl7ZZixKjpWb3Rp0ayhqwycKY0ApIM88uvR1VtNRMK43JgfsnbhXuoxPlANetqzAbq8schacCTwoI_KUxTsiViF81nX8lV7; passport_csrf_token=0bb46aee7ef562c7b9eeeba0f8fccfa0; passport_csrf_token_default=0bb46aee7ef562c7b9eeeba0f8fccfa0; d_ticket_ads=1077a5b51a40d29516128aa2c60f333537e1d; sso_auth_status_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; sso_auth_status_ss_ads=72a9dc878c9114011e1e3361e1939cde%2C9466b1a35753d1781bd84ae643e397e2%2C6fe04ccc72c92b6667cc81dd4021ca2b; store-country-code=vn; store-country-code-src=uid; gs_seller_type_for_report=pop; store-country-sign=MEIEDESPxrvB06qNfYdfUgQg4B_HDrbpLeP9CdIE-YJY6BdaarXL7bKKGGjiLJPICDYEEGliolUtTngR1bGAFS67g74; ttcsid_C97F14JC77U63IDI7U40=1782788239095::Z4T_7d6smYjQdLYLmokg.1.1782788239314.0; sid_guard_ads=7dce85a5ece0119837a9e49193d82721%7C1782788240%7C259199%7CFri%2C+03-Jul-2026+02%3A57%3A19+GMT; _ga_HV1FL86553=GS2.1.s1782788238$o3$g0$t1782788240$j58$l0$h765679905; _ga_Y2RSHPPW88=GS2.1.s1782788238$o3$g1$t1782788240$j58$l0$h84740190; s_v_web_id=verify_mr20y3tc_H2U7jwlJ_Kusd_4M8B_9ILr_fllXt6cz14UH; FPGSID=1.1783073627.1783073627.G-BZBQ2QHQSP.6uxyFdKQec6NTlGcnjHNwQ; _ttp=3FyeB5r9slSOXhiiWRfY0B6dR34.tt.1; FPLC=AUXMx5UjBx3mHhkYSeArq05PH5Wv7dxmE%2FtKZdh%2BqLVRDeHYgu3i9hiYEzF9n3yQN1CSt%2BIjdy7oSuTL3SoCHNu6NYK1GRtN%2B2WXc539Pcv8HzeoKimTMSxB3udVhA%3D%3D; msToken=brJObm49PATRdEqkGkCoEYcoKo8kh6k0FP3Z2ZX2YWeHA2DuD0ScVCls4oFr4ihdvHV41eTd9jo5AeXcF6nPorAMkxJ_zLlt0p_wKCXPB3qm2qSZ9qmzvCNfWfsO4aCirNHdXE0rylvvx4I=; ttcsid=1783073627631::jrbao3mJGW0ECOf8U0u1.33.1783073643613.0::1.-2436.0::15980.11.953.587::0.0.0; ttcsid_CMSS13RC77U1PJEFQUB0=1783073627630::-Cq0pu6W1VEjDcVpFzuf.33.1783073643613.1; sso_uid_tt_ads=a66e0ea72c94c80ea7ab7d791e840213b8069b9e334e571c834bae98e89e92dc; sso_uid_tt_ss_ads=a66e0ea72c94c80ea7ab7d791e840213b8069b9e334e571c834bae98e89e92dc; sso_user_ads=2a9addf46435434c4117d71fbbf1aa5a; sso_user_ss_ads=2a9addf46435434c4117d71fbbf1aa5a; sid_ucp_sso_v1_ads=1.0.1-KDA2ZjlmNjlkMDFkNTM2NmM5MjI5YzllOGNkMjE3ZDEwMDhhMTgyNjgKIgiViLWa9-_oimkQ7pae0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhOWFkZGY0NjQzNTQzNGM0MTE3ZDcxZmJiZjFhYTVhMk4KIE1y2UpFZJVudzvCmLe2-zuW3cMH1oKPBXYTsa8FxdQlEiC6F7OubJbWp0d9FOkYn-pYXrQ0XX30vF-dKCyzji8PXBgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KDA2ZjlmNjlkMDFkNTM2NmM5MjI5YzllOGNkMjE3ZDEwMDhhMTgyNjgKIgiViLWa9-_oimkQ7pae0gYY5B8gDDCZx9bIBjgBQOsHSAYQAxoCbXkiIDJhOWFkZGY0NjQzNTQzNGM0MTE3ZDcxZmJiZjFhYTVhMk4KIE1y2UpFZJVudzvCmLe2-zuW3cMH1oKPBXYTsa8FxdQlEiC6F7OubJbWp0d9FOkYn-pYXrQ0XX30vF-dKCyzji8PXBgEIgZ0aWt0b2s; _ga_BZBQ2QHQSP=GS2.1.s1783073627$o32$g1$t1783073646$j41$l0$h1463057170; odin_tt=18f5ed999d7473ffb8ae969eef939c78080d28e5fb8422276c3004e2b6cce85f5023bfcbdc0b1496035b85bb15df179ac97abdc16f7fdf1fe492d4e4043ee089; sid_guard_tiktokseller=b549083481cf182036b40a675b5a8171%7C1783073646%7C259200%7CMon%2C+06-Jul-2026+10%3A14%3A06+GMT; uid_tt_tiktokseller=6c4459d0945324456cbd8fd7beabed0e6e30813929065c47d3a79a19830f52bd; uid_tt_ss_tiktokseller=6c4459d0945324456cbd8fd7beabed0e6e30813929065c47d3a79a19830f52bd; sid_tt_tiktokseller=b549083481cf182036b40a675b5a8171; sessionid_tiktokseller=b549083481cf182036b40a675b5a8171; sessionid_ss_tiktokseller=b549083481cf182036b40a675b5a8171; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CtUkINIHPGCA2tApnW1qBcf________-8E5H1Avu1CrFS01_AFWVGVk1858Q0j17gJaAFruNfHTw%3D; sid_ucp_v1_tiktokseller=1.0.1-KDBiZmY1MzA0NjJhODk2Mjk5ODIzMzM5OGZkOWQ4NTM5OGNjZTNhYzIKHAiViLWa9-_oimkQ7pae0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiNTQ5MDgzNDgxY2YxODIwMzZiNDBhNjc1YjVhODE3MTJOCiCyZsbnrH9NvyZngt6niWGYKBMTnVYRNAu6_PJHjXTQGxIgQnTg5ftjl4mRePo4sQdXEXirfdiQySqBFk_myHbi7tcYBCIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDBiZmY1MzA0NjJhODk2Mjk5ODIzMzM5OGZkOWQ4NTM5OGNjZTNhYzIKHAiViLWa9-_oimkQ7pae0gYY5B8gDDgBQOsHSAQQAxoDc2cxIiBiNTQ5MDgzNDgxY2YxODIwMzZiNDBhNjc1YjVhODE3MTJOCiCyZsbnrH9NvyZngt6niWGYKBMTnVYRNAu6_PJHjXTQGxIgQnTg5ftjl4mRePo4sQdXEXirfdiQySqBFk_myHbi7tcYBCIGdGlrdG9r; msToken=0hHOzgkXmbz6g55HAxiZ-GZSA5zRXvPAYqYJmOlJC4JElCLGahDAMkRDI55ACoc4N0L587HzophO9xPfJxC-oSKk3jGbzKsoBXI7Lq8DDHkGFNjDyzxpRYlsGDnEIpBupA7VomiWgfXK-zQ=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIk9lY1VpZCI6NzQ5NDcwMTY3MzEyMDA0MDgzMSwiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3ODMxNjAwNDgsIm5iZiI6MTc4MzA3MjY0OH0.cpvn3XfxUvqBKlz1ql542Hjv0Ln8H7zNkl__d2i2-e0; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzIxMzgxMTYyODM5NzQ2NzcsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzY0ODg1MTA1NTIzNTc0NTUzNywiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzgzMTYwMDQ4LCJuYmYiOjE3ODMwNzI2NDh9.ToAjuka7L_OxS1RVUekHES_UUE8d1VUXDk6dAnSOh_I; oec_lucifer=AQEBAHeBbdUL92nSHhwFOjNMCbnuoSj4J+b3B9lhkpX5VWQZSTQBSYiNZQ9RyBLnfjUYAuRYLidttz90NmKc6aU33KsqDdFqgkcQ; user_oec_info=0a530111e02f4df8999041ca3ec77be852d1607abd7eafeda2d42c7c4892ab5739d4b32b0ebad94abf6caaa206c35bfaad8e1016cac72cd038c754b66b4a1d0d2bfb6265372032247505a7faa79df033f4c1b564a81a490a3c00000000000000000000509d25e5fff232cd6ba72089cb834cda5746dc123b68e6e8c02833e0b2a7806a6eaa918cb167b2645102f3143b46105563751091e9950e1886d2f6f20d22010443e3a604; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1783073655%7Ce2e717fa9bc768447d972a542b6a8b88b303b9dd8abd64ef366b52dd108d4414; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtcHVibGljLWtleSI6IkJCd242SmFiUVk0Wm5EYU9HU3hVVXNiSTVsMndYOFc4ZlpkbkQxMHB3a2Rkak56cmFOMGIwMkFYMHRlQ29TZ1gzZnpBeHg0NWdMLzhEY0t2Sm11S3hXMD0iLCJ0dC10aWNrZXQtZ3VhcmQtd2ViLXZlcnNpb24iOjF9"
}

# =====================================================
# =====================================================
tz_vn = timezone(timedelta(hours=7))

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
    start_date = (now_vn - timedelta(days=31)).replace(tzinfo=None)
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
                    create_time_ms / 1000
                ).replace(tzinfo=None)

                sku_details = o.get("sku_detail", [])

                for sku in sku_details:

                    creator_nickname = sku.get("creator_nickname")
                    creator_username = sku.get("creator_username")

                    cos_ratio = sku.get("cos_ratio")
                    estimated_cos_fee = sku.get("estimated_cos_fee")

                    shop_ads_commission_ratio = sku.get("shop_ads_commission_ratio")
                    estimated_shop_ads_commission = sku.get("estimated_shop_ads_commission")

                    promotion_position_type = (
                        sku.get("promotion_position", {})
                        .get("promotion_position_type")
                    )

                    all_rows.append((
                        int(main_order_id),
                        "7494545630022240481",
                        creator_nickname,
                        creator_username,
                        promotion_position_type,
                        create_time,
                        cos_ratio,
                        estimated_cos_fee,
                        shop_ads_commission_ratio,
                        estimated_shop_ads_commission
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
    "id_shop",
    "creator_nickname",
    "creator_username",
    "promotion_position_type",
    "create_time",
    "cos_ratio",
    "estimated_cos_fee",
    "shop_ads_commission_ratio",
    "estimated_shop_ads_commission"
])

    df["create_time"] = pd.to_datetime(df["create_time"], errors="coerce")

    def to_decimal(x):
        try:
            if pd.isna(x):
                return None
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None


    df["cos_ratio"] = df["cos_ratio"].apply(to_decimal)
    df["estimated_cos_fee"] = df["estimated_cos_fee"].apply(to_decimal)
    df["shop_ads_commission_ratio"] = df["shop_ads_commission_ratio"].apply(to_decimal)
    df["estimated_shop_ads_commission"] = df["estimated_shop_ads_commission"].apply(to_decimal)

    # drop row lỗi
    df = df.dropna(subset=["main_order_id"])
        
    # df = df.drop_duplicates(
    # subset=["main_order_id", "promotion_position_type", "cos_ratio", "estimated_cos_fee", "shop_ads_commission_ratio", "estimated_shop_ads_commission"]
    # )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # ==============================
    # DELETE DATA TODAY + YESTERDAY
    # ==============================
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE id_shop = '7494545630022240481' AND DATE(create_time) >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 31 DAY)  
    """

    client.query(delete_query).result()

    print("Old data (today + yesterday) deleted.")

    # ==============================

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




























