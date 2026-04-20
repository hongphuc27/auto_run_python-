import os
import requests
import pandas as pd
import json
from typing import List, Dict, Any
from datetime import datetime, timedelta
import copy
from google.cloud import bigquery
from google.oauth2 import service_account


# =========================================================
# 1) CẤU HÌNH
# =========================================================
URL = (
    "https://seller-vn.tiktok.com/oec_ads/shopping/v1/oec/stat/post_creative_list"
    "?locale=vi&language=vi&oec_seller_id=7494545630022240481&aadvid=7615175616880279560"
)

CAMPAIGN_PRODUCT_LIST = [
    {"campaign_id": "1860078570106065", "product_id": "1729698184033634529"},
    {"campaign_id": "1860078995260466", "product_id": "1731701920029182177"},
    {"campaign_id": "1860078489790722", "product_id": "1729470464531990753"},
    {"campaign_id": "1860080038512241", "product_id": "1730742466307262689"},
    {"campaign_id": "1860079935164657", "product_id": "1731472858961970401"},
    {"campaign_id": "1860078614631554", "product_id": "1729484964544022753"},
    {"campaign_id": "1860078749583618", "product_id": "1730349637723982049"},
    {"campaign_id": "1860078749583618", "product_id": "1729470527690541281"},
    {"campaign_id": "1860078929856625", "product_id": "1730252252855372001"},
    {"campaign_id": "1860078868262353", "product_id": "1729486385386588385"},
    {"campaign_id": "1860079816940561", "product_id": "1730302679495051489"},
    {"campaign_id": "1860079059953857", "product_id": "1730181954563311841"},
    {"campaign_id": "1860078686444673", "product_id": "1729470537122220257"},
    {"campaign_id": "1860079128984818", "product_id": "1731702051089451233"},
    {"campaign_id": "1860079175493778", "product_id": "1733727076981572833"},
    {"campaign_id": "1860079474447457", "product_id": "1729639484482947297"},
    {"campaign_id": "1860078787945793", "product_id": "1729486356177848545"},
    {"campaign_id": "1860079755328722", "product_id": "1729486349622544609"},
    {"campaign_id": "1860079682233570", "product_id": "1729486292431833313"},
    {"campaign_id": "1860079867938882", "product_id": "1729623119067384033"},
    {"campaign_id": "1860079236594994", "product_id": "1734642185429877985"},
    {"campaign_id": "1860079236594994", "product_id": "1734667833409373409"},
    {"campaign_id": "1860079395621041", "product_id": "1730251964036057313"},
    {"campaign_id": "1860079395621041", "product_id": "1729622986924591329"},
    {"campaign_id": "1860079395621041", "product_id": "1730302770380966113"},
    {"campaign_id": "1860079395621041", "product_id": "1730302825051031777"},
    {"campaign_id": "1860079395621041", "product_id": "1729486375097305313"},
    {"campaign_id": "1860079395621041", "product_id": "1730317044142147809"},
    {"campaign_id": "1860079395621041", "product_id": "1730316975142242529"},
    {"campaign_id": "1860079395621041", "product_id": "1730349611533961441"},
    {"campaign_id": "1860079395621041", "product_id": "1729622970679396577"},
    {"campaign_id": "1860079395621041", "product_id": "1731474108330640609"},
    {"campaign_id": "1860079395621041", "product_id": "1729641410604337377"},
    {"campaign_id": "1860079395621041", "product_id": "1730349732165814497"},
    {"campaign_id": "1860079395621041", "product_id": "1732191833568282849"},
    {"campaign_id": "1860079395621041", "product_id": "1729743427249866977"},
    {"campaign_id": "1860079395621041", "product_id": "1730213816466180321"},
    {"campaign_id": "1860079395621041", "product_id": "1729743369763129569"},
    {"campaign_id": "1860079395621041", "product_id": "1731495950021658849"},
    {"campaign_id": "1860079395621041", "product_id": "1730317334915221729"},
    {"campaign_id": "1860079395621041", "product_id": "1729978295606413537"},
    {"campaign_id": "1860079395621041", "product_id": "1731473522040145121"},
    {"campaign_id": "1860079395621041", "product_id": "1729641412512942305"},
    {"campaign_id": "1860079395621041", "product_id": "1731110599028672737"},
    {"campaign_id": "1860079395621041", "product_id": "1729641403661781217"},
    {"campaign_id": "1860079395621041", "product_id": "1729576315233077473"},
    {"campaign_id": "1860079395621041", "product_id": "1729576314845563105"},
    {"campaign_id": "1860079395621041", "product_id": "1729612585699084513"},
    {"campaign_id": "1860079395621041", "product_id": "1730941406672226529"},
    {"campaign_id": "1860079395621041", "product_id": "1730305084493105377"},
    {"campaign_id": "1860079395621041", "product_id": "1729743387720124641"},
    {"campaign_id": "1860079395621041", "product_id": "1730941478478055649"},
    {"campaign_id": "1860079395621041", "product_id": "1730349772519409889"},
    {"campaign_id": "1860079395621041", "product_id": "1730317275336509665"},
]

END_TIME = datetime.today().strftime("%Y-%m-%d")
START_TIME = (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d")

# ===== BIGQUERY CONFIG =====
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_detail_video_ahuy"

gcp_key = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(gcp_key)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://seller-vn.tiktok.com",
    "referer": "https://seller-vn.tiktok.com/",
    "user-agent": "Mozilla/5.0",
    "x-csrftoken": "pICMUok8m661IP2kRQPf2HpsAkfm4daB",
}
COOKIE_STRING = "tt_ticket_guard_client_web_domain=2; passport_csrf_token=aa075ec20b79358d976cd57c0a250dc5; passport_csrf_token_default=aa075ec20b79358d976cd57c0a250dc5; _ga=GA1.1.240625919.1775537645; FPID=FPID2.2.2vwfEqQnm1fbTyLngcbupbCN7jTzwZsEQcaBdDulNzI%3D.1775537645; FPAU=1.2.1153587569.1775537646; _gtmeec=e30%3D; _fbp=fb.1.1775537646211.1947283501; _tt_enable_cookie=1; d_ticket_tiktokseller=81c4e79c89ed9489803c6e50a087c08237e1d; d_ticket_ads=d73109808bb6dceaf21d2442848ad97c37e1d; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; tta_attr_id_mirror=0.1775537767.7625876643680124948; _ga_HV1FL86553=GS2.1.s1775537769$o1$g0$t1775537769$j60$l0$h558951516; _ga_Y2RSHPPW88=GS2.1.s1775537769$o1$g1$t1775537769$j60$l0$h276479430; i18next=vi-VN; pre_country=VN; csrftoken=IEsxTIiBGAofRD43UXSQ2LACUc63dxAN; tta_attr_id=0.1775786611.7626945287505608722; gs_seller_type_for_report=pop; tt_chain_token=pYcepJnv2MfRs/dgRrAtIg==; store-country-sign=MEIEDF5Ci3SOZQIa2rkn9wQgOVLDS924YVbjxxJXAlOHnZa_Ie-PuenaEf6vLGpKBSAEEOz8JDeYBYk45DUGWs-YGtc; ATLAS_LANG=vi-VN; sid_guard_ads=88785dd3f5535735d8cc06391d57fc85%7C1776160388%7C259200%7CFri%2C+17-Apr-2026+09%3A53%3A08+GMT; ttcsid_CMSS13RC77U1PJEFQUB0=1776396362719::btvc1oDwuuCD4SHmMMg3.4.1776396421402.1; ttcsid=1776396362720::eCO-JscRKJ1amZpKTiMW.4.1776396421402.0::1.44961.46765::58687.22.846.390::0.0.0; sso_uid_tt_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_uid_tt_ss_ads=14ed041c438c35f2c563261d30cc53a71279da3be5743849ac862de5cdebf635; sso_user_ads=454dba0da10f723807a2be83b8845e3c; sso_user_ss_ads=454dba0da10f723807a2be83b8845e3c; sid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; ssid_ucp_sso_v1_ads=1.0.1-KGY5MDJhYTc0NDUwNWM0MTIyY2U4OThmNzgxYWRjMDdhOGVhOWMwMDgKIgiUiN7g9dSegGkQhtGGzwYY5B8gDDDN9YHIBjgBQOsHSAYQAxoCbXkiIDQ1NGRiYTBkYTEwZjcyMzgwN2EyYmU4M2I4ODQ1ZTNjMk4KIJvQHljSc6EEZ9dfnP6GXBQTA-pAFEvWeSIt_DYtjhqyEiC6JvfSENMq9Xzui3W1er9-v6XdGnZYCbq4KHefO7VgdhgEIgZ0aWt0b2s; sid_guard_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913%7C1776396422%7C259200%7CMon%2C+20-Apr-2026+03%3A27%3A02+GMT; uid_tt_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; uid_tt_ss_tiktokseller=c943b7714085e63b47b2a36f9e0e17b3418184152af3dd7ea845b031cee254c1; sid_tt_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; sessionid_ss_tiktokseller=d0ae92a9c5edfedfd516966a7cfdc913; tt_session_tlb_tag_tiktokseller=sttt%7C1%7C0K6SqcXt_t_VFpZqfP3JE__________cW5jmUlATYZWcde9YMLX3C86hQU8NH-ScWt71tyTLaXg%3D; sid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KGM0ZTI1NzU0M2ZmNGJlZjNkZTg2ZTRjMDcwNzlhMjViMTc4ZGE3ZTIKHAiUiN7g9dSegGkQhtGGzwYY5B8gDDgBQOsHSAQQAxoDc2cxIiBkMGFlOTJhOWM1ZWRmZWRmZDUxNjk2NmE3Y2ZkYzkxMzJOCiC9o5Xx8QhYHhH9MJEvBm7XxL3NoE-E-3l-qZpiy9EPaxIgOAM_fhGraxA9OQqb24OrkRpchOaRdjM8AXidXOAFuYEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1776396362$o4$g1$t1776396423$j59$l0$h690599615; msToken=QLijwVK5-ZogyMVyZPEzfeet_OapgJCL42Qq1wsoVeX7Fo0-_NPsEobwqHTly918EhjFs7f9mD-NAmwcNu8-KvGlqUPAHRh1m9EQ7FLOrq6LB221nhpa_FHy3_eZnHbYrRaOVtw=; msToken=tAhKl93DpAx6dp_LJ8RugcoyihEPRW2s5qbV8u7DHsfKyToVt-Wfsnmo9__xpv4-otB3BJoVOvO6H85TLcSbWPv_3bLPiHe73Vdp9FvCowHWfcnI6c7UhFqF2iKr; _ttp=3CWN4yQ928TeDMcFGYwiZda8idI; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzY3MzUzMjUsIm5iZiI6MTc3NjY0NzkyNX0.RWl-ufbIvl8O6H_zzouop3d1HHFR1NKuKwqTmgyO7vc; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc2NzM1MzI1LCJuYmYiOjE3NzY2NDc5MjV9.2NS4FAqgAnEY52PxHmDkvbyN8X1cf-zmqQz2L8Ym2A8; s_v_web_id=verify_mo6iwdpl_tN6yiO07_JlFn_4v10_8Iro_d84SEvNpjd0M; lang_type=vi; _tea_utm_cache_4068={%22campaign_id%22:1862554752900321}; ttwid=1%7CTmXh6848r_2XsP1Gbwr63VSXlqkhSlrl1cSPSd0M-uY%7C1776648989%7Cc144e13a2e5536c0394f4e7d37c67e5191c693e1d384b90aec738b560cb543eb; _tea_utm_cache_1583={%22campaign_id%22:1862554752900321}; user_oec_info=0a53f97465b52aebba4fbed0f6920c61d14e7b6df8ffe67c85087057e9b038744b4b6372f1adc13c60e773e911e5d47dc608665873a25205cf35cde0ac538c18d3cf6677a31b3feae30594fde83dfab99ef13a3dc71a490a3c000000000000000000005052b0c506e45a9fc0b189d979fbee057e70745e6b6731a459e657f0088f22518f152ca90f2f9348e35434b40cebd58b8e5110cda48f0e1886d2f6f20d2201043e284dc3; odin_tt=1cf7742cc6d6f68a1da02f4ea9592102e4a487c055892058b33f51f21f4bd6f081ae6e65960f5eba30fb945d10633b97837296a34507c58087c47820d578644d; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnK2d2YWhHT3dlbFkxVzkwVlpiZDd1L3hrT0xHRU93OUQyenpuMFJIU3VwK2hSQU5DQUFRY0oraVdtMEdPR1p3Mmpoa3NWRkxHeU9aZHNGL0Z2SDJYWnc5ZEtjSkhYWXpjNjJqZEc5TmdGOUxYZ3FFb0Y5Mzh3TWNlT1lDLy9BM0NyeVpyaXNWdFxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVIQ2ZvbHB0QmpobWNObzRaTEZSU3hzam1YYkJmeGJ4OWwyY1BYU25DUjEyTTNPdG8zUnZUWUJmUzE0S2hLQmZkL01ESEhqbUF2L3dOd3E4bWE0ckZiUT09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkJ3bjZKYWJRWTRabkRhT0dTeFVVc2JJNWwyd1g4VzhmWmRuRDEwcHdrZGRqTnpyYU4wYjAyQVgwdGVDb1NnWDNmekF4eDQ1Z0wvOERjS3ZKbXVLeFcwPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
BASE_PAYLOAD = {
    "query_list": [
        "material_name",
        "material_video_info",
        "tt_account_name",
        "tt_account_avatar_icon",
        "shop_content_type",
        "item_authorization_type",
        "item_creative_source",
        "item_public_status",
        "item_delivery_status",
        "authorize_remove_time",
        "item_delivery_secondary_status",
        "item_authorization_priority",
        "item_create_time",
        "mixed_real_cost",
        "onsite_roi2_shopping_sku",
        "mixed_real_cost_per_onsite_roi2_shopping_sku",
        "onsite_roi2_shopping_value",
        "onsite_mixed_real_roi2_shopping",
        "roi2_ctr",
        "onsite_shopping_sku_cvr",
        "session_info",
        "unavailable_reason_enum",
        "video_bi_appeal_info"
    ],
    "start_time": START_TIME,
    "end_time": END_TIME,
    "order_field": "mixed_real_cost",
    "order_type": 1,
    "page": 1,
    "campaign_id": "",
    "page_size": 50,
    "spu_id_list": [],
    "api_version": 2,
    "filters": [
        {
            "field": "item_delivery_status",
            "in_field_values": ["10"],
            "filter_type": 10
        }
    ]
}

OUTPUT_COLUMNS = [
    "ngay",
    "campaign_id",
    "product_id",
    "item_id",
    "shop_content_type",
    "onsite_roi2_shopping_value",
    "mixed_real_cost",
    "onsite_shopping_sku_cvr",
    "roi2_ctr",
]

ZERO_CHECK_COLUMNS = [
    "onsite_roi2_shopping_value",
    "mixed_real_cost",
    "onsite_shopping_sku_cvr",
    "roi2_ctr",
]

# =========================================================
# 2) HÀM PHỤ
# =========================================================
def get_date_list(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_list


def to_float(value):
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def is_all_zero(record):
    return all(to_float(record.get(col)) == 0 for col in ZERO_CHECK_COLUMNS)


def find_records(obj):
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and "item_id" in obj[0]:
            return obj
        for item in obj:
            result = find_records(item)
            if result:
                return result

    elif isinstance(obj, dict):
        for key in ["data", "list", "records", "items", "result", "table"]:
            if key in obj:
                result = find_records(obj[key])
                if result:
                    return result

        for value in obj.values():
            result = find_records(value)
            if result:
                return result

    return []


# =========================================================
# 3) FETCH
# =========================================================
def fetch_one_page(session, page, campaign_id, product_id, run_date):
    payload = copy.deepcopy(BASE_PAYLOAD)
    payload["page"] = page
    payload["campaign_id"] = campaign_id
    payload["spu_id_list"] = [product_id]
    payload["start_time"] = run_date
    payload["end_time"] = run_date

    res = session.post(URL, headers=HEADERS, data=json.dumps(payload), timeout=60)
    res.raise_for_status()

    data = res.json()

    if data.get("code") not in (0, None):
        raise Exception(data)

    return find_records(data)


def fetch_all(run_date):
    session = requests.Session()
    session.headers.update({"cookie": COOKIE_STRING})

    all_rows = []

    for config in CAMPAIGN_PRODUCT_LIST:
        campaign_id = config["campaign_id"]
        product_id = config["product_id"]
        page = 1

        print(f"\n--- campaign={campaign_id} | product={product_id} | date={run_date} ---")

        while True:
            records = fetch_one_page(session, page, campaign_id, product_id, run_date)

            if not records:
                print(f"Campaign {campaign_id} | Product {product_id} | Page {page}: không còn data → dừng")
                break

            print(f"Campaign {campaign_id} | Product {product_id} | Page {page}: raw records = {len(records)}")

            page_all_zero = True
            for item in records:
                row_check = {
                    "onsite_roi2_shopping_value": item.get("onsite_roi2_shopping_value"),
                    "mixed_real_cost": item.get("mixed_real_cost"),
                    "onsite_shopping_sku_cvr": item.get("onsite_shopping_sku_cvr"),
                    "roi2_ctr": item.get("roi2_ctr"),
                }
                if not is_all_zero(row_check):
                    page_all_zero = False
                    break

            if page_all_zero:
                print(f"Campaign {campaign_id} | Product {product_id} | Page {page}: toàn bộ record = 0 → dừng sớm")
                break

            filtered_count = 0

            for item in records:
                row = {
                    "ngay": run_date,
                    "campaign_id": str(campaign_id) if campaign_id is not None else None,
                    "product_id": str(product_id) if product_id is not None else None,
                    "item_id": str(item.get("item_id")) if item.get("item_id") is not None else None,
                    "shop_content_type": str(item.get("shop_content_type")) if item.get("shop_content_type") is not None else None,
                    "onsite_roi2_shopping_value": to_float(item.get("onsite_roi2_shopping_value")),
                    "mixed_real_cost": to_float(item.get("mixed_real_cost")),
                    "onsite_shopping_sku_cvr": to_float(item.get("onsite_shopping_sku_cvr")),
                    "roi2_ctr": to_float(item.get("roi2_ctr")),
                }

                if is_all_zero(row):
                    continue

                all_rows.append(row)
                filtered_count += 1

            print(f"Campaign {campaign_id} | Product {product_id} | Page {page}: records sau lọc = {filtered_count}")

            if len(records) < BASE_PAYLOAD["page_size"]:
                print(f"Campaign {campaign_id} | Product {product_id} | Page {page}: < page_size → dừng")
                break

            page += 1

    df = pd.DataFrame(all_rows, columns=OUTPUT_COLUMNS)

    if df.empty:
        return df

    # Chuẩn hóa type trước khi load BQ
    df["ngay"] = pd.to_datetime(df["ngay"], errors="coerce").dt.date
    df["campaign_id"] = df["campaign_id"].astype("string")
    df["product_id"] = df["product_id"].astype("string")
    df["item_id"] = df["item_id"].astype("string")
    df["shop_content_type"] = df["shop_content_type"].astype("string")

    for col in ZERO_CHECK_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


# =========================================================
# 4) BIGQUERY
# =========================================================
def get_bq_schema():
    return [
        bigquery.SchemaField("ngay", "DATE"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("item_id", "STRING"),
        bigquery.SchemaField("shop_content_type", "STRING"),
        bigquery.SchemaField("onsite_roi2_shopping_value", "FLOAT64"),
        bigquery.SchemaField("mixed_real_cost", "FLOAT64"),
        bigquery.SchemaField("onsite_shopping_sku_cvr", "FLOAT64"),
        bigquery.SchemaField("roi2_ctr", "FLOAT64"),
    ]


def delete_last_n_days(bq_client, days=4):
    delete_query = f"""
    DELETE FROM `{table_ref}`
    WHERE ngay >= DATE_SUB(CURRENT_DATE(), INTERVAL {days-1} DAY)
    """

    bq_client.query(delete_query).result()
    print(f"Đã xóa dữ liệu {days} ngày gần nhất trong BigQuery")


def load_to_bigquery(bq_client, df):
    if df.empty:
        print("DataFrame rỗng, bỏ qua load BigQuery")
        return

    job_config = bigquery.LoadJobConfig(
        schema=get_bq_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = bq_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )
    job.result()

    print(f"Đã load {len(df)} dòng lên {table_ref}")


# =========================================================
# 5) MAIN
# =========================================================
if __name__ == "__main__":
    bq_client = client

    date_list = get_date_list(START_TIME, END_TIME)

    # 🔥 XÓA 1 LẦN (LOGIC MỚI)
    print("\n🧹 Deleting last 20 days data in BigQuery...")
    delete_last_n_days(bq_client, days=3)
    print("✅ Delete done")

    # 💾 FETCH + GỘP DATA
    print("\n💾 Writing to BigQuery...")

    all_df = []

    for run_date in date_list:
        print(f"\n====== DATE {run_date} ======")
        df_day = fetch_all(run_date)
        print(f"Rows ngày {run_date}: {len(df_day)}")

        if not df_day.empty:
            all_df.append(df_day)

    if all_df:
        final_df = pd.concat(all_df, ignore_index=True)
        load_to_bigquery(bq_client, final_df)
        print(f"✅ Insert done: {len(final_df)} rows")
    else:
        print("⚠️ Không có dữ liệu để insert")

    print("Hoàn tất load dữ liệu lên BigQuery")
