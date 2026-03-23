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
START_TIME = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# ===== BIGQUERY CONFIG =====
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "fact_detail_video_ahuy"



credentials = service_account.Credentials.from_service_account_file(
    r"E:\hongphuc\Source code\code kéo dữ liệu SQL Sever (Thảo)\rhysman-data-warehouse-488306-8db2b940e56a.json"
)

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

COOKIE_STRING = "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; sso_uid_tt_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_uid_tt_ss_ads=178a8dcc2d7ade8f320cbe66f246253d4fbd538e80630553bda7e26e8f4cde95; sso_user_ads=14d85f2b0adb9b74de123cec9b36ea8d; sso_user_ss_ads=14d85f2b0adb9b74de123cec9b36ea8d; sid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDExMTYyYzYyZTA2NTQyYmI3MTVhNDdhNWM5NTRhYTY2MzkzMDk1YjcKIgiUiN7g9dSegGkQhaXvzQYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDc2cxIiAxNGQ4NWYyYjBhZGI5Yjc0ZGUxMjNjZWM5YjM2ZWE4ZDJOCiDchbZtW2qLronqrDy1SmJCgAlaGz5RRmchW8YA-6j4fRIgM6lbbpHKmnh2wCG-ujtZWqz5PkvSUVTE3di627as_M0YAyIGdGlrdG9r; uid_tt_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; uid_tt_ss_tiktokseller=29ac06efb6640b856f365af7fa6774e2ce3cb778ab1ec499da1774b1637c977b; sid_tt_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; sessionid_ss_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; _tea_utm_cache_4068={%22campaign_id%22:1860078570106065}; _tea_utm_cache_1583={%22campaign_id%22:1860078570106065}; sid_guard_ads=3847856ccedf5c11a59bc9d9ef5b272a%7C1773990826%7C185179%7CSun%2C+22-Mar-2026+10%3A40%3A05+GMT; uid_tt_ads=c686f51058f130ef1806164c6ef3bc3a446561caa9efc08860d9d06bec99202f; uid_tt_ss_ads=c686f51058f130ef1806164c6ef3bc3a446561caa9efc08860d9d06bec99202f; sid_tt_ads=3847856ccedf5c11a59bc9d9ef5b272a; sessionid_ads=3847856ccedf5c11a59bc9d9ef5b272a; sessionid_ss_ads=3847856ccedf5c11a59bc9d9ef5b272a; tt_session_tlb_tag_ads=sttt%7C2%7COEeFbM7fXBGlm8nZ71snKv_________-Zj7MEwTZ5DNPgCt3mR6ehndCIk6ac6F5621DWJVKQCg%3D; sid_ucp_v1_ads=1.0.1-KDM3MDNhYmJmZWY1N2MyYjAwNDlmZDY2ZDg1NjgzNmQ4Yzc0MjBlZDAKHAiUiN7g9dSegGkQqufzzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiAzODQ3ODU2Y2NlZGY1YzExYTU5YmM5ZDllZjViMjcyYTJOCiCtN7iuJiyVUsBn41tYiRil8ynJHrbqMxyxqrTVl03djhIgFIaZYMVIW_52gLwBjMtFhecar3JWQ4so-_XpoBo7bhMYBCIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDM3MDNhYmJmZWY1N2MyYjAwNDlmZDY2ZDg1NjgzNmQ4Yzc0MjBlZDAKHAiUiN7g9dSegGkQqufzzQYYrwwgDDgBQOsHSAQQAxoDc2cxIiAzODQ3ODU2Y2NlZGY1YzExYTU5YmM5ZDllZjViMjcyYTJOCiCtN7iuJiyVUsBn41tYiRil8ynJHrbqMxyxqrTVl03djhIgFIaZYMVIW_52gLwBjMtFhecar3JWQ4so-_XpoBo7bhMYBCIGdGlrdG9r; _ga_HV1FL86553=GS2.1.s1773990828$o6$g1$t1773990828$j60$l0$h1985046469; _ga_Y2RSHPPW88=GS2.1.s1773990828$o5$g1$t1773990828$j60$l0$h1334679047; ttcsid_C97F14JC77U63IDI7U40=1773990829246::_nFQcqJTBcfmOF3E39x_.4.1773990829644.0; s_v_web_id=verify_mmzrpxs3_Hj2dnJhi_28GL_4azl_8i7H_Q0jBLqgDXqbN; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQxNTAwOTcsIm5iZiI6MTc3NDA2MjY5N30.NKUY7VLeOAeiIFEXfxmR7j0akQ3kwQyPM0-8MHF-A6M; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0MTUwMDk3LCJuYmYiOjE3NzQwNjI2OTd9.GsKu89YI4O-JHdFRx8bZuqZpJ6p6Y3QVQUczoOQkUj0; lang_type=vi; sid_guard_tiktokseller=b44ea1e23643e7b2074c9c83e801afbf%7C1774067888%7C259200%7CTue%2C+24-Mar-2026+04%3A38%3A08+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C5%7CtE6h4jZD57IHTJyD6AGvv_________-dacVMJGl4Fveol7T9ZtNFvqzydTbDPfdwIAwd4DKlNao%3D; sid_ucp_v1_tiktokseller=1.0.1-KDYyODJjZmM4MjYwM2RjNmJhMzhjMjgxNjMxMjI0ZTE4YTk2MGJiZDAKHAiUiN7g9dSegGkQsMH4zQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIETVLkW47JOm7avVAaI0TL3OP472v606UT7Kxupff0SREiDy3G0C3juuxqv5RVMvWBE7UBrwzqifnJ815gt9D4RBdBgBIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KDYyODJjZmM4MjYwM2RjNmJhMzhjMjgxNjMxMjI0ZTE4YTk2MGJiZDAKHAiUiN7g9dSegGkQsMH4zQYY5B8gDDgBQOsHSAQQAxoCbXkiIGI0NGVhMWUyMzY0M2U3YjIwNzRjOWM4M2U4MDFhZmJmMk4KIETVLkW47JOm7avVAaI0TL3OP472v606UT7Kxupff0SREiDy3G0C3juuxqv5RVMvWBE7UBrwzqifnJ815gt9D4RBdBgBIgZ0aWt0b2s; FPLC=XfkL6dp69Y8C9wamxcCutBUYbLqKiFmUdKEXvQv9iVtznG2IEW%2Fn3AKtNZ4ueZY0zr31CQUZXiEL%2FpaWJVCdQmoz8CF16JXew5JaeLZSCke0ZY2yAUGxsFjNT09BRg%3D%3D; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; ttcsid=1774067894540::1F-dYHKHvVEnaFe8UPEh.14.1774067894841.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774067894540::kpIK0vtQhzOZfds72-xM.11.1774067894841.0; msToken=pSO12g8qRPXuGfYF6RYOK1joU8A5wAvc6oAgDaOvZA6oMPsTNunTF8D6Qpap2hGQu7t-Pz-UFFw9vzcsKMLk7QJok6Hr2E1dOCgFssZhppTz5KXoGjCtYwDIDTDk_A==; passport_fe_beating_status=false; msToken=hb1zIYKTB0yZrtrpELH23pXSQj0BYIxNampXlA8OuAjg83aF6a2huZEVkyhhTOgvpRX1kuUb3VBniy6EDk7fbaXNX84YFJ6O2kGaVIEyWvA1ElzDj6cECVJ8q7Nt; _ga_BZBQ2QHQSP=GS2.1.s1774067893$o12$g0$t1774067901$j52$l0$h1395986713; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774082716%7Ce889fe07640564806deaf764b52d0595397c54cea88c0c6d8677dfdacda61b7c; user_oec_info=0a530234dd72ab3fc50d33cd121e4f8afca242094799993b922e857ee382b5b7f437da8e341d37a0d7e8487bc26431b69e47f77b80e022536d862fcbe6cbbecd509153d284e627cc8311ad426ad5af11652397534e1a490a3c000000000000000000005035ab88641630c603274faed7b5364b2d90caaba3c19a52d152d6a4eca6f9b0806e944b12581ba7b87d59511a46bec14dca10c6d78c0e1886d2f6f20d220104eef02760; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; odin_tt=3c9313f1bdf47aeb7596011d81aae011e1ee52ca05b682d79f230e90399b06636bed508c35b77f59f5e02cec7d13137df40a36442d39ac09367c6cfaf7c8aeda"

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


def delete_last_n_days(bq_client, days=2):
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
    delete_last_n_days(bq_client, days=20)
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