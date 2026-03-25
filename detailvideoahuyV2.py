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

COOKIE_STRING = "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; store-country-sign=MEIEDAmRLXexg3d3p-udqgQgjd1xQi_ClXN5OWqgYMK6W_jPgEsbGxDsOpCloIYxRXMEEDzgeCS2IuQUBJrZcdlaacI; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; d_ticket_ads=b70f0eac0afa1fe58bd2b69ab4c83c317856e; _ga_HV1FL86553=GS2.1.s1773990828$o6$g1$t1773990828$j60$l0$h1985046469; _ga_Y2RSHPPW88=GS2.1.s1773990828$o5$g1$t1773990828$j60$l0$h1334679047; ttcsid_C97F14JC77U63IDI7U40=1773990829246::_nFQcqJTBcfmOF3E39x_.4.1773990829644.0; _ttp=3BCmBQlzlHCJUroXhfColyeRaT1.tt.1; msToken=qzPbrqm-8uJeodLC28_TzbbeLI_2Mv3mCs6gp0gkttWZhvnUUZuwccj2lEN3R7x7svuxXDk4fvo2R_wt0xZacwiJHk23a-Y8pP3a7O72pqGQrQnij60utMUu_nwmfmQWS9yItls=; ttcsid=1774328558618::vo01iITGmxLRpspmTycH.15.1774328581572.0; ttcsid_CMSS13RC77U1PJEFQUB0=1774328558618::yaGX_UIJG4xKpNNcedlG.12.1774328581572.1; sso_uid_tt_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_uid_tt_ss_ads=4ff55b89d4c564fad4cd4306c2905755014b1f8c41d9f3c6c4f2209332323b74; sso_user_ads=28923db567af453c44258468669f6c64; sso_user_ss_ads=28923db567af453c44258468669f6c64; sid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KDI3MDczOWQ4ZWFiYjJjMzY4YzhlODI2Y2FlYjliZDcwNjRmN2YyN2YKIgiUiN7g9dSegGkQh7aIzgYY5B8gDDDN9YHIBjgBQOsHSAYQAxoDbXkyIiAyODkyM2RiNTY3YWY0NTNjNDQyNTg0Njg2NjlmNmM2NDJOCiARq-e-85q-xd47WO0BFr908HndmpL0Cw-wwdcpDBqzNRIgJ-80HGKnL0jL_QUSWKrQIABZzujHGRurkk9LhUHzufEYBSIGdGlrdG9r; _ga_BZBQ2QHQSP=GS2.1.s1774328558$o13$g1$t1774328584$j34$l0$h182365910; sid_guard_tiktokseller=89fc588555d8994266248c8b5c2794f7%7C1774328583%7C259200%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; uid_tt_ss_tiktokseller=d119853d9f6b3d2c147747ac7097eae2bca7dffc0ca5c2ae891a225170c2d5a5; sid_tt_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_tiktokseller=89fc588555d8994266248c8b5c2794f7; sessionid_ss_tiktokseller=89fc588555d8994266248c8b5c2794f7; tt_session_tlb_tag_tiktokseller=sttt%7C2%7CifxYhVXYmUJmJIyLXCeU9__________6r4d-Iyrp0mENXHEHHfoMCN3BQVGhnLrugOXdiVOSilI%3D; sid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; ssid_ucp_v1_tiktokseller=1.0.1-KDhmMDJiOWE0MmRlNzJjZDhhMGI3ZmY3YjNjN2IyYTc5ZDZmNjk0MWYKHAiUiN7g9dSegGkQh7aIzgYY5B8gDDgBQOsHSAQQAxoDc2cxIiA4OWZjNTg4NTU1ZDg5OTQyNjYyNDhjOGI1YzI3OTRmNzJOCiCQYY1xUT5Zg001OHOaUY77hakeYSZQln1av2YahCAXzhIghj6YwFhCTAJn3Az_8chlvrWZstL6oFXYkUrz53ATb7kYASIGdGlrdG9r; part=stable; sid_guard_ads=c506bb03011584fbc778c79fd05d2514%7C1774405638%7C182145%7CFri%2C+27-Mar-2026+05%3A03%3A03+GMT; uid_tt_ads=bf54c5a17681444f503392eb812e2acee8b9112aabd07f74205a1b8c7e30c151; uid_tt_ss_ads=bf54c5a17681444f503392eb812e2acee8b9112aabd07f74205a1b8c7e30c151; sid_tt_ads=c506bb03011584fbc778c79fd05d2514; sessionid_ads=c506bb03011584fbc778c79fd05d2514; sessionid_ss_ads=c506bb03011584fbc778c79fd05d2514; tt_session_tlb_tag_ads=sttt%7C1%7CxQa7AwEVhPvHeMef0F0lFP________-hpWG4_nAPrs9G6ZPvifVViERvQclRY-HUGpUjezy4E7k%3D; sid_ucp_v1_ads=1.0.1-KDU1MzdhZTliZGVhMTY1YjZiM2U5MDViMTY4MTgxN2U2ZjcyNWM3MmYKHAiUiN7g9dSegGkQhpCNzgYYrwwgDDgBQOsHSAQQAxoDbXkyIiBjNTA2YmIwMzAxMTU4NGZiYzc3OGM3OWZkMDVkMjUxNDJOCiB4rDPRkoSgpLwvqPU_BMiOxMyodOTvw3XX3fzBoRBhnhIgPIIQHl7mB3LarXI9O3DAHafsKRE96aO8OehVVjeHKOoYBSIGdGlrdG9r; ssid_ucp_v1_ads=1.0.1-KDU1MzdhZTliZGVhMTY1YjZiM2U5MDViMTY4MTgxN2U2ZjcyNWM3MmYKHAiUiN7g9dSegGkQhpCNzgYYrwwgDDgBQOsHSAQQAxoDbXkyIiBjNTA2YmIwMzAxMTU4NGZiYzc3OGM3OWZkMDVkMjUxNDJOCiB4rDPRkoSgpLwvqPU_BMiOxMyodOTvw3XX3fzBoRBhnhIgPIIQHl7mB3LarXI9O3DAHafsKRE96aO8OehVVjeHKOoYBSIGdGlrdG9r; ac_csrftoken=8cd79cf1c74c45ccac78a07fc4a75c3e; pre_country=VN; msToken=5wldyEIEbBrXCCKq0fIQT2NM2qBBnexIv3-xCl5vbev2uLDzHcnC3xgZUggcLr4YNOtYLBZE2wkTga4NVcI1osFCsgseRT6Rv4G2ZqP23tdYe2jvUkl_cMy0fJk8u-K0RSEwrVdyEfDG4ks=; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; s_v_web_id=verify_mn5k02f0_gLiVxFJo_SRHH_4z8W_A8Md_4zwcbUJGnYYA; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ0OTk5MjksIm5iZiI6MTc3NDQxMjUyOX0.eW9QIEO9DnGvlEaYJr_-8px2yVO-1LIgsgQ0Zxjs88k; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0NDk5OTI5LCJuYmYiOjE3NzQ0MTI1Mjl9.CDgMVWF2QU9q6eaOY_V3sWKcqo5TVcTML0WNkWZG-4M; lang_type=vi; _tea_utm_cache_4068={%22campaign_id%22:1860078570106065}; _tea_utm_cache_1583={%22campaign_id%22:1860078570106065}; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774413688%7C912c7f8429cefccd05036a7f3403ef4a1e8648c33d0e71eced8f5037342b4463; odin_tt=93b323e02604993a386557713a2a96ecce556a245f6bb38191abfe138612ae9be7b2829363ca434386418c36f3076217391f2b889a06079d0ba7fada28169ec5; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnRTNPYVd0cDVka3cvNTVwVTVJT3A2VlY5T3VnU0Y3S0JZNkdKOHIzL3NXdWhSQU5DQUFSbnVyejBacHRqSkg2SnBXbTd0NEdDZDRVMGpXUURlMHJET0JLMG1Qb2kweDFyUis2ckp1NzFvRmJwZzNUVmJWNW92NkJReFlhc3NrQXVxWklYcGpFaVxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVaN3E4OUdhYll5UitpYVZwdTdlQmduZUZOSTFrQTN0S3d6Z1N0Smo2SXRNZGEwZnVxeWJ1OWFCVzZZTjAxVzFlYUwrZ1VNV0dyTEpBTHFtU0Y2WXhJZz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkdlNnZQUm1tMk1rZm9tbGFidTNnWUozaFRTTlpBTjdTc000RXJTWStpTFRIV3RIN3FzbTd2V2dWdW1EZE5WdFhtaS9vRkRGaHF5eVFDNnBraGVtTVNJPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D; user_oec_info=0a53622c5ec101e6e26ae6b2f5b6a4156af98724024941394b24b5cec231d86316c72cf4dca5f2fdc7c79bef3a767a964b9be02842c232ae1987cb81d734129b43746b8a185aed8170f19680a862028a14c8ba77e21a490a3c000000000000000000005039621330e6033bb570828975be2aa6364e4a37f34412e59262c31150074ea5b705ec630a3fb299f6ab31740fbf700a4fdb10d7818d0e1886d2f6f20d2201048151969d"
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
    delete_last_n_days(bq_client, days=1)
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
