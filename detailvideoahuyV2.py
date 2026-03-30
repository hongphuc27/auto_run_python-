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
START_TIME = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

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

COOKIE_STRING = "tt_ticket_guard_client_web_domain=2; passport_csrf_token=75a1f2d773a771d4a355628e3f0481b2; passport_csrf_token_default=75a1f2d773a771d4a355628e3f0481b2; _ga=GA1.1.1239180537.1772415590; FPID=FPID2.2.pLnNpOh1CtSRRUJXutQxyRlWfeWZIv2jVWyj2yrChjM%3D.1772415590; FPAU=1.2.1440130235.1772415591; _tt_enable_cookie=1; _gtmeec=e30%3D; _fbp=fb.1.1772415590632.1280856204; sso_auth_status_ads=dee191494e7c7f183bd2043fb34b12b7; sso_auth_status_ss_ads=dee191494e7c7f183bd2043fb34b12b7; _tt_ticket_crypt_doamin=2; SHOP_ID=7075901688577638662; gs_seller_type_for_report=pop; pre_country=VN; csrftoken=pICMUok8m661IP2kRQPf2HpsAkfm4daB; tta_attr_id_mirror=0.1772797845.7614108765217882120; _m4b_theme_=new; goofy_grayscale_uid=92258635e3d3ac017404a6b2ea891a5d; test_flag=0; tta_attr_id=0.1773636090.7617708817361666066; i18next=vi-VN; ATLAS_LANG=vi-VN; d_ticket_tiktokseller=83b3c28c014dfd9c3ae0622f8559ff5b7856e; _hjSessionUser_6487441=eyJpZCI6ImIwNjUwNTcyLWI1Y2QtNTRjYy1iZjY5LTg0NDEzYjUxODA2NyIsImNyZWF0ZWQiOjE3NzQ0MzU5MDExMDksImV4aXN0aW5nIjp0cnVlfQ==; _ga_ER02CH5NW5=GS1.1.1774435922.1.0.1774435942.0.0.2069784347; store-country-sign=MEIEDE-88jxRRItooDkeuAQgyNf51T8j_RbM8i7X6N8MydRP3mhuPPELzRGzc5W-GzMEEEiE68ECKnIIxz4D_d8ormw; ttcsid_CMSS13RC77U1PJEFQUB0=1774453289314::wBKJppyDS9tNsWZdLO-B.13.1774453592016.1; uid_tt_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; uid_tt_ss_tiktokseller=dd62eacbf4a3b9858eccd0d7ab1a603c5f5b014afe4594caa6772f0b2f30b9eb; sid_tt_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_tiktokseller=365e38ac78727f488dc4df74c586cc67; sessionid_ss_tiktokseller=365e38ac78727f488dc4df74c586cc67; _ga_BZBQ2QHQSP=GS2.1.s1774453288$o14$g1$t1774453596$j38$l0$h1431874121; msToken=U2XIFaNF4td0503O8Y9QX6_XaJi3xbNTbLbA6iBqu0sBLVSuEOlr_XzSoXgunUo625J34I39QBZKP8MJicnYuGilMh661DtKZQbBEpJCMUM5Ez1bLoAtx3ZhAnnROA==; sid_guard_tiktokseller=365e38ac78727f488dc4df74c586cc67%7C1774671423%7C259200%7CTue%2C+31-Mar-2026+04%3A17%3A03+GMT; tt_session_tlb_tag_tiktokseller=sttt%7C3%7CNl44rHhyf0iNxN90xYbMZ__________huEsqr_IGWehVOAuNZ5yefvR6fTMLbJBFIH_W7VHTOew%3D; sid_ucp_v1_tiktokseller=1.0.1-KGE0ZDUwZjU3YTBkNjEwMjMxNjQ4NGMzNDA4MzRiMjg0MjlhMjkzNDQKHAiUiN7g9dSegGkQv6ydzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDM2NWUzOGFjNzg3MjdmNDg4ZGM0ZGY3NGM1ODZjYzY3Mk4KIN_Y592M9F5ESRQpXxlIGTKs8L9f6ClTIJ9cmx9iiRoiEiBQm4eZWNWIEqHCwTQkKBKv5orasswiwoy7-zkDG0cEjxgCIgZ0aWt0b2s; ssid_ucp_v1_tiktokseller=1.0.1-KGE0ZDUwZjU3YTBkNjEwMjMxNjQ4NGMzNDA4MzRiMjg0MjlhMjkzNDQKHAiUiN7g9dSegGkQv6ydzgYY5B8gDDgBQOsHSAQQAxoCbXkiIDM2NWUzOGFjNzg3MjdmNDg4ZGM0ZGY3NGM1ODZjYzY3Mk4KIN_Y592M9F5ESRQpXxlIGTKs8L9f6ClTIJ9cmx9iiRoiEiBQm4eZWNWIEqHCwTQkKBKv5orasswiwoy7-zkDG0cEjxgCIgZ0aWt0b2s; part=stable; FPLC=RVvsJoSt%2FDFEF9QWFneHEANbCd0j1%2F6aAEXJ5LCHDY7A7auwjpqXQaUBjO5fYdUu6HvpbnA5v%2FN0%2BaW4r2uh7hBA1RBsx2yTJVDuTk7DIEVnkjE5FfxirdLoRsKDcg%3D%3D; d_ticket_ads=64a949fe585bd0b58050dfea05d2bd987856e; ttcsid_C97F14JC77U63IDI7U40=1774835513869::uJNds63hdshEhFt1cn8x.6.1774835613942.1; ttcsid=1774835513870::Ma6Uv2lb4XYZxVxDqYyX.18.1774835613942.0; sso_uid_tt_ads=52279b88c7863f41421a2c88685eae85eb491888a01aefe349f104b41a2d9bea; sso_uid_tt_ss_ads=52279b88c7863f41421a2c88685eae85eb491888a01aefe349f104b41a2d9bea; sso_user_ads=79307dd1771031e116cbd4e948b1e39e; sso_user_ss_ads=79307dd1771031e116cbd4e948b1e39e; sid_ucp_sso_v1_ads=1.0.1-KGQ4MTU5OGJjNWE1NjU3MjljZDljODVjNTU0OWQwOTI4N2Q2ODNmODgKIAiQiKz8zcvg22kQn6-nzgYYrwwgDDDzhN7NBjgBQOsHEAMaAm15IiA3OTMwN2RkMTc3MTAzMWUxMTZjYmQ0ZTk0OGIxZTM5ZTJOCiA35w77ldLrbcwXAwbtw0tAFYv_0T_MPPjQOtN17XE9jhIgVcyVc_R0mUkeM_iS6U1XU9J1TXfdgGAe510JbnsqVeAYASIGdGlrdG9r; ssid_ucp_sso_v1_ads=1.0.1-KGQ4MTU5OGJjNWE1NjU3MjljZDljODVjNTU0OWQwOTI4N2Q2ODNmODgKIAiQiKz8zcvg22kQn6-nzgYYrwwgDDDzhN7NBjgBQOsHEAMaAm15IiA3OTMwN2RkMTc3MTAzMWUxMTZjYmQ0ZTk0OGIxZTM5ZTJOCiA35w77ldLrbcwXAwbtw0tAFYv_0T_MPPjQOtN17XE9jhIgVcyVc_R0mUkeM_iS6U1XU9J1TXfdgGAe510JbnsqVeAYASIGdGlrdG9r; sid_guard_ads=f899825006ec49bd881b561b656a5f98%7C1774835615%7C259200%7CThu%2C+02-Apr-2026+01%3A53%3A35+GMT; uid_tt_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; uid_tt_ss_ads=1189a9c5c2db4445d171053a35fa9967a7ca0c727f94da5fbbabf1ac6bcd8991; sid_tt_ads=f899825006ec49bd881b561b656a5f98; sessionid_ads=f899825006ec49bd881b561b656a5f98; sessionid_ss_ads=f899825006ec49bd881b561b656a5f98; tt_session_tlb_tag_ads=sttt%7C5%7C-JmCUAbsSb2IG1YbZWpfmP________-t15I4P-OyvC9IbsR_9dyr1Y0wH1TFqcxcbGfGG5NHDAE%3D; sid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ssid_ucp_v1_ads=1.0.1-KDJkNzBiNWU2MzExNjkwYTVmN2YwNmRmYTJkNzVlZmQxYzFiODliNDIKHAiQiKz8zcvg22kQn6-nzgYYrwwgDDgBQOsHSAQQAxoCbXkiIGY4OTk4MjUwMDZlYzQ5YmQ4ODFiNTYxYjY1NmE1Zjk4Mk4KIL8Yhwnok8pEUhABtMNW9YPUyNSFkqrI_ygwTz1zy-S3EiDdAA-PcUfkWTEpf0Vv4RheDgSWqaP1k1STLUlFCd2UrhgEIgZ0aWt0b2s; ac_csrftoken=6fc32da583e44de595ba2e891277cd49; _ga_HV1FL86553=GS2.1.s1774835513$o2$g1$t1774835616$j60$l0$h508160841; _ga_Y2RSHPPW88=GS2.1.s1774835513$o7$g1$t1774835616$j60$l0$h125334612; pre_country=VN; msToken=lX_CI-tnVmzx_l4lEQE0vLwVD55Yyzg6Xp9AlHI-dThlvvpj6IWZhcyVA8HUBlRFwteeerxNnjRGzUNWW4YDLGyJNxTsGN-3m3bnm5OuMQBkk-hs37dyOtV97UXz1a8wuSBIZ5131ILnM9TzOV6KDOvcm3Q=; _ttp=3BeDVOBgxInCyEDnVU9LJHvqUyy; global_seller_id_unified_seller_env=7494545630022240481; app_id_unified_seller_env=4068; oec_seller_id_unified_seller_env=7494545630022240481; s_v_web_id=verify_mncz91gn_INbrE0eZ_Zcln_4Rbu_9v4p_W3e4hPQd8eIM; SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIk9lY1VpZCI6NzQ5NDIyMzQyNjQ0OTY3MTg0NywiT2VjU2hvcElkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJTaG9wUmVnaW9uIjoiIiwiR2xvYmFsU2VsbGVySWQiOjc0OTQ1NDU2MzAwMjIyNDA0ODEsIlNlbGxlcklkIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJleHAiOjE3NzQ5NDg4MDYsIm5iZiI6MTc3NDg2MTQwNn0.51Ghcv4gm5dD-EIfeKmaiyLiVhxi0su6g_BZ9io9Rc8; UNIFIED_SELLER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NjYxODIyMzMyMDU2MDUzOTYsIkdsb2JhbFNlbGxlck1hcCI6eyI3NDk0NTQ1NjMwMDIyMjQwNDgxIjp7IlNlbGxlcnMiOlt7IlNlbGxlcklEIjo3NDk0NTQ1NjMwMDIyMjQwNDgxLCJJTVNob3BJRCI6NzA3NTkwMTY4ODU3NzYzODY2MiwiSU1DdXN0b21lclNlcnZpY2VJRCI6NzU2NjE4MjM3MjczMTkzMDM4NCwiU2hvcFJlZ2lvbiI6IlZOIiwiSXNBY3Jvc3MiOmZhbHNlfV19fSwiZXhwIjoxNzc0OTQ4ODA2LCJuYmYiOjE3NzQ4NjE0MDZ9.OL-fidQQ5bZZ6hvVaYYYR1tPJI9Z4vg3sA1zP38m-IQ; lang_type=vi; _tea_utm_cache_4068={%22campaign_id%22:1860078489790722}; _tea_utm_cache_1583={%22campaign_id%22:1860078489790722}; ttwid=1%7CNOjT0tbBg2b45ofUf7xYMHIe1niGo5DDkpr_va067uM%7C1774867035%7C7b5d594d98b8a64564b6ba7cef37872c83ad3c4c1e058cca548636641696e29a; user_oec_info=0a53f26879cf9e44918428f2418aae0f8296a2cabf760f9f54cf7105992c2d0311100d33c95148bf5b7582fda2386e1f6a6c111f9a89e5225e85b7dd88fe355efb2f40754ff5ac995e3ce15036d8d16a965dea353a1a490a3c00000000000000000000503e5906d45b21db3e63ad39ec6cc8fb775a118d12ceec8eb52d987f192bf805ced3aaa7069a93c4ee2249d7524de5dae8511092bc8d0e1886d2f6f20d220104e2321dca; odin_tt=a8eec4ad2fd356d40b3ece767869bd69315fa920412ef17e5224c054b088616331ef8aa8f020c4f839f1e21a42f592bce12e1a319f904e4c448a0fc5895fde90; tt_ticket_guard_client_data=eyJ0dC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwidHQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJ0dC10aWNrZXQtZ3VhcmQtc2NlbmUiOiJ0dF9zZWxsZXIiLCJ0dC10aWNrZXQtZ3VhcmQtb3JpZ2luLWNyeXB0Ijoie1wiZWNfcHJpdmF0ZUtleVwiOlwiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXFxuTUlHSEFnRUFNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQkcwd2F3SUJBUVFnNmg3bDVTS0NGOTl0RWhBcXJXRURab01GcXdDQVJrbDFRUkZoelN1Y3pCS2hSQU5DQUFSRTlHQ0k5YlM5ZE04S0l6RWkzZGlhb0lnU1h3clFVaWt5WlNEYlJ5Q2k5OXZqUEJia0hDM2NsUFFwbTFGM0FRT2ZkemM1VUVseCtSTXZjYXdicnNJYlxcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cIixcImVjX3B1YmxpY0tleVwiOlwiLS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS1cXG5NRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVSUFJnaVBXMHZYVFBDaU14SXQzWW1xQ0lFbDhLMEZJcE1tVWcyMGNnb3ZmYjR6d1c1Qnd0M0pUMEtadFJkd0VEbjNjM09WQkpjZmtUTDNHc0c2N0NHdz09XFxuLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tXCIsXCJlY19jc3JcIjpcIlwifSIsInR0LXRpY2tldC1ndWFyZC1wdWJsaWMta2V5IjoiQkVUMFlJajF0TDEwendvak1TTGQySnFnaUJKZkN0QlNLVEpsSU50SElLTDMyK004RnVRY0xkeVU5Q21iVVhjQkE1OTNOemxRU1hINUV5OXhyQnV1d2hzPSIsInR0LXRpY2tldC1ndWFyZC13ZWItdmVyc2lvbiI6MX0%3D"
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


def delete_last_n_days(bq_client, days=7):
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
    delete_last_n_days(bq_client, days=7)
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
