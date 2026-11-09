# """TikTok Ads cấp creative theo ngày -> BigQuery fact_tiktok_ads_creative (hoặc Excel).
# File chạy một mình, không cần file nào khác. Giống tab Ad trên Ads Manager ("Total of N creatives"):
# statistics/op/ad/list/ với dimensions = creative_id, mỗi ngày gọi st = et = ngày đó.

# GitHub Actions (file .py đặt ở gốc repo, chỉ cần cài: pip install google-cloud-bigquery requests):
#   - Cookie  : env TIKTOK_COOKIE <- secret COOKIE_ADS (cả chuỗi cookie). Hết hạn (~3 ngày) thì cập nhật secret, không sửa code.
#   - BigQuery: env GOOGLE_SERVICE_ACCOUNT_JSON <- secret cùng tên (JSON inline, không ghi ra file).
# Chạy ở máy: đặt env TIKTOK_COOKIE (PowerShell: $env:TIKTOK_COOKIE = '...'); BigQuery dùng ADC (gcloud) nếu không có
# GOOGLE_SERVICE_ACCOUNT_JSON.

# Campaign Smart+ bản mới: "Ad group" trên giao diện = ad_id trong API, "Ad" trên giao diện = creative_id trong API.
# Tên cột theo giao diện: adgroup_id / adgroup_name = API ad_id / ad_name; ad_id / ad_name = API creative_id / creative_name.

# Cột campaign_reach = Reach của cả campaign trong ngày (lấy ở tab Campaign, cùng lượt gọi kiểm chi phí): Reach không cộng
# từ creative được, báo cáo cấp campaign dùng cột này.
# 12 cột cuối (thêm 05/11/2026, sau campaign_reach): nhóm Product details page view — 7 cột bản (TikTok) + 5 cột bản Shop.

# Lấy creative ở mọi trạng thái, kể cả đã xóa (Deleted). Mỗi lần chạy: xóa rồi nạp lại 5 ngày gần nhất
# (hôm nay + 4 ngày trước, giờ VN) trong 1 transaction; --start/--end để chạy khoảng ngày khác.
# Mặc định chỉ chạy thử (kéo + kiểm, không ghi gì). Ghi BigQuery phải thêm --write.

#     python tiktok_ads_creative_to_bq.py                                                 # chạy thử 5 ngày gần nhất
#     python tiktok_ads_creative_to_bq.py --write                                         # xóa + nạp lại 5 ngày gần nhất
#     python tiktok_ads_creative_to_bq.py --start 2026-10-01 --end 2026-10-31 --write    # khoảng ngày tùy chọn
#     python tiktok_ads_creative_to_bq.py --start 2026-10-28 --end 2026-10-31 --excel tiktok_creative.xlsx
# """
# import argparse
# import json
# import os
# import re
# import sys
# import time
# import uuid
# from datetime import date, datetime, timedelta, timezone

# import requests
# from google.cloud import bigquery

# TABLE = os.environ.get("BQ_TABLE_CREATIVE",
#                        "rhysman-data-warehouse-488306.rhysman_bigcampaigns.fact_tiktok_ads_creative")
# DAYS = 5
# EXTRA_METRICS = ["creative_platform_status", "material_primary_source", "material_secondary_source"]

# # ---------------------------------------------------------------- Cấu hình chung (giữ giống tiktok_ads_to_bq.py)
# VN_TZ = timezone(timedelta(hours=7))
# ENV_COOKIE = "TIKTOK_COOKIE"  # GitHub Actions: map từ secret COOKIE_ADS
# ADVERTISER_ID = os.environ.get("TIKTOK_ADVERTISER_ID", "7628879252477231124")  # aadvid trên URL Ads Manager 7625589331687948309
# API_URL = "https://ads.tiktok.com/api/v4/i18n/statistics/op/campaign/list/"
# USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
#               "Chrome/156.0.0.0 Safari/537.36")
# # Body như trang Campaign của Ads Manager gửi; st / et / page / filters / metrics điền lúc gọi.
# BASE_BODY = {
#     "common_req": {"lifetime": 0, "compare_st": "", "compare_et": "", "sort_stat": "create_time", "sort_order": 1,
#                    "dimensions": ["campaign_id"], "page_size": 20},
#     "workflow_card": {"type": "0"},
#     "extra": {"get_compare_value": "0", "get_compare_rate": "0", "get_compare_diff": "0",
#               "scene": "campaign_list_v2", "list_version": "v2", "page_view_scene": "view_non_first",
#               "region_schedule": "1", "ab_primary_status": "v2", "enable_status_action": 1},
# }

# # 63 chỉ số: 44 cột đầu đúng thứ tự cột J..BA của file Excel, các cột thêm sau. (tên API, kiểu, ý nghĩa, tên trên TikTok Ads)
# # kiểu: vnd -> NUMERIC, cnt -> INTEGER, pct -> FLOAT dạng thập phân (API trả 2.56 = 2,56%), ratio -> FLOAT
# METRICS = [
#     ("stat_cost", "vnd", "Chi phí đã tiêu", "Cost"),
#     ("cpc", "vnd", "Chi phí trung bình mỗi click", "CPC (destination)"),
#     ("cpm", "vnd", "Chi phí cho 1.000 lượt hiển thị", "CPM"),
#     ("show_cnt", "cnt", "Lượt hiển thị", "Impressions"),
#     ("click_cnt", "cnt", "Lượt click", "Clicks (destination)"),
#     ("ctr", "pct", "Tỉ lệ click = Clicks / Impressions", "CTR (destination)"),
#     ("time_attr_convert_cnt", "cnt", "Số chuyển đổi (theo thời điểm chuyển đổi)", "Conversions (conversion time)"),
#     ("skan_convert_cnt", "cnt", "Chuyển đổi iOS SKAdNetwork", "SKAN conversions"),
#     ("time_attr_conversion_cost", "vnd", "Chi phí / chuyển đổi (theo thời điểm chuyển đổi)", "Cost per conversion (conversion time)"),
#     ("skan_conversion_cost", "vnd", "Chi phí / chuyển đổi SKAN", "SKAN cost per conversion"),
#     ("time_attr_conversion_rate", "pct", "Tỉ lệ chuyển đổi trên click", "CVR (conversion time)"),
#     ("time_attr_conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi trên hiển thị", "CVR impression (conversion time)"),
#     ("skan_conversion_rate", "pct", "Tỉ lệ chuyển đổi SKAN (trên click)", "SKAN CVR"),
#     ("skan_conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi SKAN (trên hiển thị)", "SKAN CVR (impression)"),
#     ("time_attr_effect_cnt", "cnt", "Kết quả theo mục tiêu tối ưu", "Results (conversion time)"),
#     ("time_attr_effect_cost", "vnd", "Chi phí / kết quả", "Cost per result (conversion time)"),
#     ("time_attr_effect_rate", "pct", "Tỉ lệ kết quả", "Result rate (conversion time)"),
#     ("time_attr_deep_convert_cnt_v2", "cnt", "Chuyển đổi sâu", "Deep conversions (conversion time)"),
#     ("time_attr_cost_per_deep_convert_v2", "vnd", "Chi phí / chuyển đổi sâu", "Cost per deep conversion"),
#     ("time_attr_deep_convert_rate_v2", "pct", "Tỉ lệ chuyển đổi sâu", "Deep CVR"),
#     ("show_uv", "cnt", "Số người (unique) được tiếp cận", "Reach"),
#     ("cost_per_show_uv_mille", "vnd", "Chi phí cho 1.000 người tiếp cận", "Cost per 1,000 people reached"),
#     ("frequency", "ratio", "Tần suất = Impressions / Reach", "Frequency"),
#     ("convert_cnt", "cnt", "Số chuyển đổi (theo thời điểm hiển thị)", "Conversions"),
#     ("conversion_cost", "vnd", "Chi phí / chuyển đổi", "Cost per conversion"),
#     ("conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi trên hiển thị", "CVR (impression)"),
#     ("effect_cnt", "cnt", "Kết quả", "Results"),
#     ("effect_cost", "vnd", "Chi phí / kết quả", "Cost per result"),
#     ("effect_rate", "pct", "Tỉ lệ kết quả", "Result rate"),
#     ("total_play", "cnt", "Lượt xem video", "Video views"),
#     ("play_duration_2s", "cnt", "Lượt xem ≥ 2 giây", "2-second video views"),
#     ("play_duration_6s", "cnt", "Lượt xem ≥ 6 giây", "6-second video views"),
#     ("play_over", "cnt", "Lượt xem hết 100% video", "Video views at 100%"),
#     ("play_third_quartile", "cnt", "Lượt xem hết 75% video", "Video views at 75%"),
#     ("play_midpoint", "cnt", "Lượt xem hết 50% video", "Video views at 50%"),
#     ("play_first_quartile", "cnt", "Lượt xem hết 25% video", "Video views at 25%"),
#     ("stat_total_play_rate", "pct", "Lượt xem / hiển thị", "Video view rate"),
#     ("stat_play_duration_2s_rate", "pct", "Tỉ lệ xem ≥ 2s / hiển thị", "2-second view rate"),
#     ("stat_play_duration_6s_rate", "pct", "Tỉ lệ xem ≥ 6s / hiển thị", "6-second view rate"),
#     ("stat_play_first_quartile_rate", "pct", "Tỉ lệ xem hết 25% / hiển thị", "25% view rate"),
#     ("stat_play_midpoint_rate", "pct", "Tỉ lệ xem hết 50% / hiển thị", "50% view rate"),
#     ("stat_play_third_quartile_rate", "pct", "Tỉ lệ xem hết 75% / hiển thị", "75% view rate"),
#     ("stat_play_over_rate", "pct", "Tỉ lệ xem hết 100% / hiển thị", "100% view rate"),
#     ("total_engagement_rate", "pct", "Tỉ lệ tương tác tổng (like, comment, share, follow…)", "Engagement rate"),
#     # 5 cột thêm 19/10/2026
#     ("average_video_play", "ratio", "Thời gian xem trung bình mỗi lượt xem video (giây)", "Average play time per video view"),
#     ("ad_net_like", "cnt", "Lượt thích từ quảng cáo", "Likes"),
#     ("ad_comment", "cnt", "Bình luận từ quảng cáo", "Comments"),
#     ("ad_share", "cnt", "Chia sẻ từ quảng cáo", "Shares"),
#     ("ad_net_favorite", "cnt", "Lượt lưu (favorite) từ quảng cáo", "Favorites"),
#     # 14 cột thêm 02/11/2026. Nhóm (TikTok) là sự kiện mua hàng trên TikTok ngoài TikTok Shop (API: nonshop_onsite)
#     ("time_attr_nonshop_onsite_shopping_per_click", "pct", "Tỉ lệ mua hàng trên TikTok / click", "Purchase rate (TikTok)"),
#     ("time_attr_nonshop_onsite_shopping", "cnt", "Lượt mua hàng trên TikTok", "Purchases (TikTok)"),
#     ("time_attr_total_nonshop_onsite_shopping_value", "vnd", "Giá trị mua hàng trên TikTok", "Purchase value (TikTok)"),
#     ("time_attr_nonshop_onsite_initiate_checkout_count_per_click", "pct", "Tỉ lệ bắt đầu thanh toán trên TikTok / click",
#      "Checkout initiation rate (TikTok)"),
#     ("time_attr_nonshop_onsite_initiate_checkout_count", "cnt", "Lượt bắt đầu thanh toán trên TikTok",
#      "Checkouts initiated (TikTok)"),
#     ("time_attr_total_nonshop_onsite_initiate_checkout_count_value", "vnd", "Giá trị bắt đầu thanh toán trên TikTok",
#      "Checkout initiation value (TikTok)"),
#     ("time_attr_nonshop_onsite_on_web_cart_per_click", "pct", "Tỉ lệ thêm vào giỏ trên TikTok / click",
#      "Add to cart rate (TikTok)"),
#     ("time_attr_nonshop_onsite_on_web_cart", "cnt", "Lượt thêm vào giỏ trên TikTok", "Adds to cart (TikTok)"),
#     ("time_attr_total_nonshop_onsite_on_web_cart_value", "vnd", "Giá trị thêm vào giỏ trên TikTok", "Add to cart value (TikTok)"),
#     ("time_attr_nonshop_onsite_shopping_roas", "ratio", "ROAS mua hàng trên TikTok = giá trị mua / chi phí",
#      "Purchases ROAS (TikTok)"),
#     ("time_attr_total_repetitive_in_app_detail_uv_value", "vnd", "Giá trị xem nội dung trong app", "Content view value (app)"),
#     ("time_attr_total_omni_detail_value", "vnd", "Tổng giá trị xem nội dung (mọi kênh)",
#      "Total view content value (all-channels)"),
#     ("time_attr_total_on_web_detail_standardized_value", "vnd", "Giá trị xem nội dung trên website",
#      "Content view value (website)"),
#     ("time_attr_total_offline_detail_value", "vnd", "Giá trị xem nội dung offline", "Content view value (offline)"),
# ]
# # 12 cột thêm 05/11/2026 (nhóm Product details page view), nằm SAU campaign_reach vì BigQuery thêm cột vào cuối bảng.
# # Bản (TikTok) (API nonshop_onsite_*) = trang sản phẩm trên TikTok ngoài TikTok Shop: đủ 7 cột như dòng chọn cột trên
# # Ads Manager. Bản Shop (API onsite_*) = trang sản phẩm trong TikTok Shop: API chỉ có 5 cột (không có unique / cost per
# # unique). 11/2026: bản TikTok cả tài khoản = 0; bản Shop có lượt xem / tỉ lệ / chi phí, value trả "-" (ghi NULL).
# METRICS_AFTER_REACH = [
#     ("time_attr_unique_nonshop_onsite_on_web_detail", "cnt", "Số người (unique) xem trang chi tiết sản phẩm trên TikTok",
#      "Unique product details page views (TikTok)"),
#     ("time_attr_cost_per_unique_nonshop_onsite_on_web_detail", "vnd",
#      "Chi phí / người xem trang chi tiết sản phẩm trên TikTok", "Cost per unique product details page view (TikTok)"),
#     ("time_attr_nonshop_onsite_on_web_detail_per_click", "pct", "Lượt xem trang chi tiết sản phẩm trên TikTok / click",
#      "Product details page view rate (TikTok)"),
#     ("time_attr_nonshop_onsite_on_web_detail", "cnt", "Lượt xem trang chi tiết sản phẩm trên TikTok",
#      "Product details page views (TikTok)"),
#     ("time_attr_cost_per_nonshop_onsite_on_web_detail", "vnd", "Chi phí / lượt xem trang chi tiết sản phẩm trên TikTok",
#      "Cost per product details page view (TikTok)"),
#     ("time_attr_value_per_nonshop_onsite_on_web_detail", "vnd", "Giá trị / lượt xem trang chi tiết sản phẩm trên TikTok",
#      "Value per product details page view (TikTok)"),
#     ("time_attr_total_nonshop_onsite_on_web_detail_value", "vnd", "Giá trị xem trang chi tiết sản phẩm trên TikTok",
#      "Product details page view value (TikTok)"),
#     ("time_attr_onsite_on_web_detail_per_click", "pct", "Lượt xem trang chi tiết sản phẩm trên TikTok Shop / click",
#      "Product details page view rate (Shop)"),
#     ("time_attr_onsite_on_web_detail", "cnt", "Lượt xem trang chi tiết sản phẩm trên TikTok Shop",
#      "Product details page views (Shop)"),
#     ("time_attr_cost_per_onsite_on_web_detail", "vnd", "Chi phí / lượt xem trang chi tiết sản phẩm trên TikTok Shop",
#      "Cost per product details page view (Shop)"),
#     ("time_attr_value_per_onsite_on_web_detail", "vnd", "Giá trị / lượt xem trang chi tiết sản phẩm trên TikTok Shop",
#      "Value per product details page view (Shop)"),
#     ("time_attr_total_onsite_on_web_detail_value", "vnd", "Giá trị xem trang chi tiết sản phẩm trên TikTok Shop",
#      "Product details page view value (Shop)"),
# ]
# ALL_METRICS = METRICS + METRICS_AFTER_REACH
# BQ_TYPE = {"vnd": "NUMERIC", "cnt": "INTEGER", "pct": "FLOAT", "ratio": "FLOAT"}
# UNIT = {"vnd": " (VND)", "pct": " (dạng thập phân, 0.0225 = 2,25%)"}


# def col_name(label):
#     """Tên cột BigQuery = tên cột trên TikTok Ads bỏ ký tự đặc biệt, viết thường, nối bằng '_'.
#     Vd 'CTR (destination)' -> ctr_destination, 'Cost per 1,000 people reached' -> cost_per_1000_people_reached."""
#     return re.sub(r"[^0-9A-Za-z]+", "_", label.replace(",", "")).strip("_").lower()


# COL = {k: col_name(ui) for k, _, _, ui in ALL_METRICS}  # tên API -> tên cột BigQuery
# assert len(set(COL.values())) == len(COL), "trùng tên cột"

# # Mã trạng thái đã đối chiếu với giao diện TikTok Ads; mã khác sẽ ghi "Khác (mã X)".
# PLATFORM_STATUS = {"5": "Active", "2": "Not delivered", "7": "Paused"}
# OPT_STATUS = {"0": "Bật", "1": "Tắt"}
# # Không truyền filters = mọi trạng thái kể cả đã xóa (bộ lọc "999" của giao diện bỏ creative đã xóa).
# STATUS = {**PLATFORM_STATUS, "8": "Deleted"}

# F = bigquery.SchemaField
# SCHEMA = [
#     F("report_date", "DATE", "REQUIRED", description="Ngày dữ liệu theo múi giờ tài khoản quảng cáo TikTok (UTC+7)"),
#     F("advertiser_id", "STRING", "REQUIRED", description="ID tài khoản quảng cáo TikTok (aadvid)"),
#     F("campaign_id", "STRING", "REQUIRED", description="Campaign ID"),
#     F("campaign_name", "STRING", "REQUIRED", description="Campaign name"),
#     F("adgroup_id", "STRING", "REQUIRED", description="Ad group ID trên TikTok Ads (Smart+: API ad_id)"),
#     F("adgroup_name", "STRING", description="Ad group name trên TikTok Ads (Smart+: API ad_name)"),
#     F("ad_id", "STRING", "REQUIRED", description="Ad ID trên TikTok Ads = 1 creative (Smart+: API creative_id)"),
#     F("ad_name", "STRING", description="Ad name trên TikTok Ads (API creative_name)"),
#     F("video_id", "STRING", description="Mã video TikTok của creative (API video_info.video_id); trống với bài ảnh"),
#     F("post_text", "STRING", description="Nội dung bài đăng / caption (API creative_post_name)"),
#     F("source", "STRING", description="Cột Source trên TikTok Ads, vd 'Your own content · TikTok account' "
#                                       "(API material_primary_source · material_secondary_source)"),
#     F("on_off", "STRING", description="Cột On/Off trên TikTok Ads: Bật / Tắt (API creative_opt_status), "
#                                       "tại lần kéo dữ liệu gần nhất của ngày đó"),
#     F("status", "STRING", description="Cột Status trên TikTok Ads: Active / Not delivered / Paused / Deleted "
#                                       "(API creative_platform_status), tại lần kéo dữ liệu gần nhất của ngày đó"),
# ]


# def metric_field(k, kind, vi, ui):
#     return F(COL[k], BQ_TYPE[kind], description=f"{vi}{UNIT.get(kind, '')}. TikTok Ads: {ui}. API: {k}")


# SCHEMA += [metric_field(*m) for m in METRICS] + [  # 63 cột chỉ số: cùng tên, kiểu, mô tả với bảng campaign cũ
#     F("campaign_reach", "INTEGER",
#       description="Reach của cả campaign trong ngày (tab Campaign trên TikTok Ads, API show_uv cấp campaign), lặp lại "
#                   "trên mọi dòng creative của campaign. Reach không cộng từ creative được (1 người xem nhiều creative "
#                   "bị đếm nhiều lần): tính Reach / Frequency / Cost per 1,000 people reached của campaign dùng cột này, "
#                   "lấy 1 giá trị cho mỗi campaign × ngày."),
# ] + [metric_field(*m) for m in METRICS_AFTER_REACH]


# RETRY_WAITS = [10, 30, 60, 120]  # giây; TikTok thỉnh thoảng trả code 1300400001 "Couldn't load" khi gọi liên tục
# DAY_TRIES = 3  # ngày đang chạy: chi phí tăng giữa lúc gọi creative và lúc gọi campaign → gọi lại ngày đó
# TODAY_WAIT = 60  # giây chờ trước khi gọi lại ngày hôm nay: trong ngày số cấp creative cập nhật trễ hơn cấp campaign vài phút
# TODAY_TOLERANCE = 0.01  # ngày hôm nay hết lượt gọi vẫn lệch: chấp nhận nếu creative <= campaign và lệch <= 1%


# class TikTokAuthError(RuntimeError):
#     pass


# # ---------------------------------------------------------------- TikTok
# def read_cookie():
#     """Cookie TikTok Ads từ env TIKTOK_COOKIE (GitHub Actions: secret COOKIE_ADS) — cả chuỗi cookie của trình duyệt,
#     có hoặc không chữ 'cookie:' ở đầu, dạng copy từ "Copy as cURL (cmd)" (có dấu ^) cũng được."""
#     raw = os.environ.get(ENV_COOKIE, "").strip()
#     if not raw:
#         raise SystemExit(f"[CONFIG] env {ENV_COOKIE} trống — GitHub Actions: map từ secret COOKIE_ADS")
#     if "^" in raw:  # chuỗi copy từ "Copy as cURL (cmd)"
#         raw = re.sub(r"\^(.)", r"\1", raw)
#     raw = re.sub(r"^(cookie:\s*)", "", raw, flags=re.I).strip().strip('"')
#     return raw.replace("\r", "").replace("\n", "")


# def api_request(cookie):
#     """(url, headers) gọi API thống kê của ADVERTISER_ID; header x-csrftoken = cookie csrftoken (thiếu là bị từ chối)."""
#     m = re.search(r"(?:^|;\s*)csrftoken=([^;\s]+)", cookie)
#     if not m:
#         raise SystemExit("[CONFIG] cookie trong secret COOKIE_ADS thiếu csrftoken — API thống kê cần cả chuỗi cookie: "
#                          "copy lại toàn bộ giá trị cookie của request campaign/list rồi cập nhật secret")
#     url = f"{API_URL}?aadvid={ADVERTISER_ID}&req_src=bidding"
#     headers = {
#         "accept": "application/json, text/plain, */*",
#         "content-type": "application/json; charset=UTF-8",
#         "origin": "https://ads.tiktok.com",
#         "referer": f"https://ads.tiktok.com/i18n/manage/campaign?aadvid={ADVERTISER_ID}",
#         "user-agent": USER_AGENT,
#         "cookie": cookie,
#         "x-csrftoken": m.group(1),
#     }
#     return url, headers


# def post_with_retry(session, url, headers, body):
#     """POST, thử lại khi lỗi tạm thời (5xx, mạng, code != 0); 401/403 hoặc hết lượt thử -> TikTokAuthError."""
#     data = json.dumps(body, separators=(",", ":")).encode()
#     for attempt, wait in enumerate(RETRY_WAITS + [None], 1):
#         try:
#             r = session.post(url, headers=headers, data=data, timeout=60)
#             if r.status_code in (401, 403):
#                 raise TikTokAuthError(f"HTTP {r.status_code}")
#             r.raise_for_status()
#             j = r.json()
#             if j.get("code") == 0:
#                 return j
#             err = f"code={j.get('code')} msg={j.get('msg')}"
#         except (requests.RequestException, ValueError) as e:
#             err = f"{type(e).__name__}: {str(e)[:200]}"
#         if wait is None:
#             raise TikTokAuthError(f"{err} (sau {attempt} lần thử)")
#         print(f"    lỗi tạm ({err}), thử lại sau {wait}s", flush=True)
#         time.sleep(wait)


# def fetch_all(session, url, headers, base_body, day, level, metrics):
#     """Mọi dòng (mọi trạng thái, kể cả đã xóa) của 1 ngày: level "creative" = tab Ad, "campaign" = tab Campaign."""
#     rows, page, page_count = [], 1, 1
#     while page <= page_count:
#         body = json.loads(json.dumps(base_body))
#         req = body["common_req"]
#         req["st"] = req["et"] = day.isoformat()
#         req["page"], req["page_size"] = page, 100
#         req["dimensions"] = [f"{level}_id"]
#         req["filters"] = []
#         req["metrics"] = metrics
#         path = "/ad/list/" if level == "creative" else "/campaign/list/"
#         j = post_with_retry(session, url.replace("/campaign/list/", path), headers, body)
#         rows += j["data"]["table"]
#         page_count = int(j["data"]["pagination"]["page_count"])
#         if int(j["data"]["pagination"]["total_count"]) == 0:
#             raise RuntimeError(f"API trả 0 {level} cho ngày {day} — kiểm tra lại curl/tài khoản")
#         page += 1
#         time.sleep(1)
#     return rows


# def cost_of(rows):
#     return sum(float(r["row_data"]["stat_cost"] or 0) for r in rows)


# def fetch_day(session, url, headers, base_body, day, is_today):
#     """Creative + campaign của 1 ngày; tổng chi phí creative phải khớp cấp campaign. Ngày đang chạy chi phí tăng giữa các
#     lần gọi → gọi lại, chấp nhận khi chi phí creative nằm giữa chi phí campaign lần gọi trước và lần gọi ngay sau
#     (chi phí trong ngày chỉ tăng; thiếu creative thật thì vẫn lệch). Trả (creatives, camps, chi phí campaign lần trước).
#     Ngày hôm nay: TikTok cập nhật số cấp campaign trước, cấp creative trễ vài phút → chờ TODAY_WAIT giây rồi gọi lại;
#     hết lượt vẫn lệch mà creative <= campaign và lệch <= TODAY_TOLERANCE thì cảnh báo rồi vẫn ghi (các lần chạy sau kéo
#     lại ngày này, lúc đó đã là ngày qua nên phải khớp tuyệt đối). Ngày đã qua lệch là dừng, không ghi."""
#     prev = None
#     for attempt in range(1, DAY_TRIES + 1):
#         creatives = fetch_all(session, url, headers, base_body, day, "creative",
#                               [m[0] for m in ALL_METRICS] + EXTRA_METRICS)
#         camps = fetch_all(session, url, headers, base_body, day, "campaign", ["stat_cost", "show_uv"])
#         cr, hi = round(cost_of(creatives)), round(cost_of(camps))
#         lo = hi if prev is None else round(cost_of(prev))
#         if lo <= cr <= hi:
#             return creatives, camps, lo
#         print(f"    {day}: chi phí cấp creative {cr:,} ngoài khoảng cấp campaign {lo:,} – {hi:,} "
#               f"(lần {attempt}/{DAY_TRIES})", flush=True)
#         prev = camps
#         if is_today and attempt < DAY_TRIES:
#             time.sleep(TODAY_WAIT)
#     if is_today and cr <= hi and hi - cr <= TODAY_TOLERANCE * hi:
#         print(f"::warning::{day} là hôm nay, số cấp creative chưa cập nhật kịp: creative {cr:,} thấp hơn campaign {hi:,} "
#               f"{hi - cr:,} ({(hi - cr) / hi:.2%}) — vẫn ghi, lần chạy ngày mai kéo lại số chốt", flush=True)
#         return creatives, camps, hi
#     raise SystemExit(f"Chi phí ngày {day} lệch cấp campaign sau {DAY_TRIES} lần gọi — dừng, không ghi")


# def video_id(cr):
#     try:
#         return ", ".join(v["video_id"] for v in json.loads(cr.get("video_info") or "[]")) or None
#     except (ValueError, TypeError, KeyError):
#         return None


# def conv(v, kind):
#     if v in (None, "", "-"):
#         return None
#     f = float(v)
#     if kind == "pct":
#         return round(f / 100, 10)
#     if kind == "vnd":
#         return str(int(f)) if f.is_integer() else str(f)  # NUMERIC: gửi dạng chuỗi cho chính xác
#     if kind == "cnt":
#         return int(f)
#     return f


# def to_bq_row(cr, day, advertiser_id, campaign_reach):
#     rd = cr["row_data"]
#     missing = [k for k, *_ in ALL_METRICS if k not in rd]
#     if missing:
#         raise RuntimeError(f"API không trả các cột {missing} cho creative {cr['creative_id']}")
#     src = " · ".join(s for s in (rd.get("material_primary_source"), rd.get("material_secondary_source")) if s)
#     row = {
#         "report_date": day.isoformat(),
#         "advertiser_id": advertiser_id,
#         "campaign_id": str(cr["campaign_id"]),
#         "campaign_name": cr["campaign_name"],
#         "adgroup_id": str(cr["ad_id"]),
#         "adgroup_name": cr.get("ad_name"),
#         "ad_id": str(cr["creative_id"]),
#         "ad_name": (cr.get("creative_name") or "").strip() or None,
#         "video_id": video_id(cr),
#         "post_text": cr.get("creative_post_name") if cr.get("creative_post_name") not in (None, "", "-") else None,
#         "source": src or None,
#         "on_off": OPT_STATUS.get(str(cr.get("creative_opt_status")), str(cr.get("creative_opt_status"))),
#         "status": STATUS.get(rd.get("creative_platform_status"), f"Khác (mã {rd.get('creative_platform_status')})"),
#     }
#     row.update({COL[k]: conv(rd[k], kind) for k, kind, *_ in METRICS})
#     row["campaign_reach"] = campaign_reach.get(str(cr["campaign_id"]))
#     row.update({COL[k]: conv(rd[k], kind) for k, kind, *_ in METRICS_AFTER_REACH})
#     return row


# # ---------------------------------------------------------------- BigQuery
# def bq_client():
#     """Giống các job khác trong repo: JSON inline GOOGLE_SERVICE_ACCOUNT_JSON -> ADC (gcloud / key file)."""
#     project = TABLE.split(".")[0]
#     key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
#     if not key_json:
#         print("[BQ] credentials: ADC của máy (gcloud)", flush=True)
#         return bigquery.Client(project=project)
#     from google.oauth2 import service_account
#     try:
#         info = json.loads(key_json)
#     except json.JSONDecodeError as e:
#         raise SystemExit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
#     print(f"[BQ] credentials: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email', '?')})", flush=True)
#     return bigquery.Client(project=project, credentials=service_account.Credentials.from_service_account_info(info))


# def ensure_table(client):
#     table = bigquery.Table(TABLE, schema=SCHEMA)
#     table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="report_date")
#     table.clustering_fields = ["campaign_id", "adgroup_id", "ad_id"]
#     table.description = ("TikTok Ads – chỉ số theo creative (tab Ad trên Ads Manager) theo ngày. Nguồn: ads.tiktok.com "
#                          "/api/v4/i18n/statistics/op/ad/list, dimensions = creative_id, mỗi ngày gọi với "
#                          "st = et = report_date. Ghi cả creative không phát sinh trong ngày (số = 0).")
#     table = client.create_table(table, exists_ok=True)
#     have, want = [f.name for f in table.schema], [f.name for f in SCHEMA]
#     if have != want:
#         raise RuntimeError(f"Schema bảng {TABLE} khác script.\n  bảng:   {have}\n  script: {want}")


# def replace_range(client, rows, start, end, advertiser_id):
#     """Xóa [start, end] của advertiser rồi chèn rows — trong 1 transaction để không bao giờ mất dữ liệu giữa chừng."""
#     dataset = TABLE.rsplit(".", 1)[0]
#     staging = f"{dataset}._staging_tiktok_creative_{uuid.uuid4().hex[:8]}"
#     tmp = bigquery.Table(staging, schema=SCHEMA)
#     tmp.expires = datetime.now(timezone.utc) + timedelta(hours=1)
#     client.create_table(tmp)
#     try:
#         if rows:
#             client.load_table_from_json(
#                 rows, staging, job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE")
#             ).result()
#         cols = ", ".join(f"`{f.name}`" for f in SCHEMA)
#         script = f"""
#         BEGIN TRANSACTION;
#         DELETE FROM `{TABLE}` WHERE advertiser_id = @adv AND report_date BETWEEN @start AND @end;
#         INSERT INTO `{TABLE}` ({cols}) SELECT {cols} FROM `{staging}`;
#         COMMIT TRANSACTION;
#         """
#         job = client.query(script, job_config=bigquery.QueryJobConfig(query_parameters=[
#             bigquery.ScalarQueryParameter("adv", "STRING", advertiser_id),
#             bigquery.ScalarQueryParameter("start", "DATE", start),
#             bigquery.ScalarQueryParameter("end", "DATE", end),
#         ]))
#         job.result()
#         affected = {c.statement_type: c.num_dml_affected_rows for c in client.list_jobs(parent_job=job.job_id)
#                     if c.statement_type in ("DELETE", "INSERT")}
#         return affected.get("DELETE", 0), affected.get("INSERT", 0)
#     finally:
#         client.delete_table(staging, not_found_ok=True)


# # ---------------------------------------------------------------- Excel
# def write_excel(rows, path):
#     from openpyxl import Workbook

#     fmt = {"vnd": "#,##0", "cnt": "#,##0", "pct": "0.00%", "ratio": "0.00"}
#     cols = [("report_date", "Date", "dd/mm/yyyy"), ("campaign_id", "Campaign ID", "@"),
#             ("campaign_name", "Campaign name", None), ("adgroup_id", "Ad group ID", "@"),
#             ("adgroup_name", "Ad group name", None), ("ad_id", "Ad ID", "@"), ("ad_name", "Ad name", None),
#             ("video_id", "Video ID", "@"), ("post_text", "Post text", None), ("source", "Source", None),
#             ("on_off", "On/Off", None), ("status", "Status", None)]
#     cols += ([(COL[k], ui, fmt[kind]) for k, kind, _, ui in METRICS] + [("campaign_reach", "Campaign reach", "#,##0")]
#              + [(COL[k], ui, fmt[kind]) for k, kind, _, ui in METRICS_AFTER_REACH])
#     vnd_cols = {COL[k] for k, kind, *_ in ALL_METRICS if kind == "vnd"}
#     wide = {"Campaign name": 32, "Ad group name": 28, "Ad name": 45, "Post text": 45, "Video ID": 36}
#     wb = Workbook()
#     ws = wb.active
#     ws.title = "TikTok Ads - Creative"
#     ws.append([label for _, label, _ in cols])
#     for r in rows:
#         ws.append([date.fromisoformat(r[c]) if c == "report_date"
#                    else float(r[c]) if c in vnd_cols and r[c] is not None  # NUMERIC đang là chuỗi
#                    else r[c] for c, _, _ in cols])
#     for i, (_, label, f) in enumerate(cols, 1):
#         letter = ws.cell(row=1, column=i).column_letter
#         ws.column_dimensions[letter].width = wide.get(label, max(12, min(len(label) + 2, 30)))
#         if f:
#             for (cell,) in ws.iter_rows(min_row=2, min_col=i, max_col=i):
#                 cell.number_format = f
#     ws.freeze_panes = "H2"
#     ws.auto_filter.ref = ws.dimensions
#     wb.save(path)


# # ---------------------------------------------------------------- main
# def date_range(args):
#     if args.start or args.end:
#         start = date.fromisoformat(args.start or args.end)
#         end = date.fromisoformat(args.end or args.start)
#     else:
#         end = datetime.now(VN_TZ).date()
#         start = end - timedelta(days=args.days - 1)
#     if start > end:
#         raise SystemExit("--start phải <= --end")
#     if (end - start).days > 92:
#         raise SystemExit("Mỗi lần chạy tối đa 93 ngày")
#     return start, end


# def main():
#     p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
#     p.add_argument("--days", type=int, default=DAYS, help=f"số ngày gần nhất tính cả hôm nay (mặc định {DAYS})")
#     p.add_argument("--start", help="YYYY-MM-DD")
#     p.add_argument("--end", help="YYYY-MM-DD")
#     p.add_argument("--write", action="store_true", help="ghi BigQuery (mặc định chỉ chạy thử)")
#     p.add_argument("--excel", help="ghi ra file Excel này thay vì BigQuery")
#     args = p.parse_args()
#     sys.stdout.reconfigure(encoding="utf-8")
#     start, end = date_range(args)

#     url, headers = api_request(read_cookie())
#     base_body, advertiser_id = BASE_BODY, ADVERTISER_ID
#     print(f"Advertiser {advertiser_id} | {start} -> {end} | bảng {TABLE}")
#     session = requests.Session()
#     rows, day, today = [], start, datetime.now(VN_TZ).date()
#     try:
#         while day <= end:
#             creatives, camps, camp_before = fetch_day(session, url, headers, base_body, day, day == today)
#             campaign_reach = {str(c["campaign_id"]): conv(c["row_data"].get("show_uv"), "cnt") for c in camps}
#             day_rows = sorted((to_bq_row(c, day, advertiser_id, campaign_reach) for c in creatives),
#                               key=lambda r: -float(r["cost"] or 0))
#             cr_cost = sum(float(r["cost"] or 0) for r in day_rows)
#             camp_cost = cost_of(camps)
#             active = sum(1 for r in day_rows if float(r["cost"] or 0) > 0)
#             deleted = sum(1 for r in day_rows if r["status"] == "Deleted")
#             moving = f" (lần gọi trước {camp_before:,})" if camp_before != round(camp_cost) else ""
#             print(f"  {day}: {len(day_rows)} creative ({deleted} đã xóa, {active} có chi phí)"
#                   f" | chi phí cấp creative {cr_cost:,.0f} | cấp campaign {camp_cost:,.0f}{moving}", flush=True)
#             rows += day_rows
#             day += timedelta(days=1)
#     except TikTokAuthError as e:
#         print(f"::error::TikTok từ chối request ({e}). Cookie có thể đã hết hạn: copy lại cookie của request "
#               "campaign/list và cập nhật secret COOKIE_ADS.", file=sys.stderr)
#         sys.exit(2)

#     keys = {(r["report_date"], r["ad_id"]) for r in rows}
#     if len(keys) != len(rows):
#         raise SystemExit("Trùng (report_date, ad_id) — dừng, không ghi")
#     if args.excel:
#         write_excel(rows, args.excel)
#         print(f"Excel: {len(rows)} dòng -> {args.excel}")
#         return
#     if not args.write:
#         print(f"Chạy thử: {len(rows)} dòng × {len(SCHEMA)} cột, chưa ghi BigQuery (thêm --write để ghi).")
#         print(json.dumps(rows[0], ensure_ascii=False, indent=1))
#         return
#     client = bq_client()
#     ensure_table(client)
#     deleted, inserted = replace_range(client, rows, start, end, advertiser_id)
#     print(f"BigQuery: xóa {deleted} dòng cũ, chèn {inserted} dòng mới ({start} -> {end})")


# if __name__ == "__main__":
#     main()

















"""TikTok Ads cấp creative theo ngày -> BigQuery fact_tiktok_ads_creative (hoặc Excel).
File chạy một mình, không cần file nào khác. Giống tab Ad trên Ads Manager ("Total of N creatives"):
statistics/op/ad/list/ với dimensions = creative_id, mỗi ngày gọi st = et = ngày đó.

GitHub Actions (file .py đặt ở gốc repo, chỉ cần cài: pip install google-cloud-bigquery requests):
  - Cookie  : env TIKTOK_COOKIE <- secret COOKIE_ADS (cả chuỗi cookie). Hết hạn (~3 ngày) thì cập nhật secret, không sửa code.
  - BigQuery: env GOOGLE_SERVICE_ACCOUNT_JSON <- secret cùng tên (JSON inline, không ghi ra file).
Chạy ở máy: đặt env TIKTOK_COOKIE (PowerShell: $env:TIKTOK_COOKIE = '...'); BigQuery dùng ADC (gcloud) nếu không có
GOOGLE_SERVICE_ACCOUNT_JSON.

Campaign Smart+ bản mới: "Ad group" trên giao diện = ad_id trong API, "Ad" trên giao diện = creative_id trong API.
Tên cột theo giao diện: adgroup_id / adgroup_name = API ad_id / ad_name; ad_id / ad_name = API creative_id / creative_name.

Cột campaign_reach = Reach của cả campaign trong ngày (lấy ở tab Campaign, cùng lượt gọi kiểm chi phí): Reach không cộng
từ creative được, báo cáo cấp campaign dùng cột này.
12 cột sau campaign_reach (thêm 05/11/2026): nhóm Product details page view — 7 cột bản (TikTok) + 5 cột bản Shop.
2 cột cuối (thêm 09/11/2026): objective_type (mã mục tiêu campaign, API objective_type) + objective_name (tên mục tiêu —
API chỉ trả mã, script đổi sang tên theo bảng OBJECTIVE; mã lạ ghi "Khác (mã X)").

Lấy creative ở mọi trạng thái, kể cả đã xóa (Deleted). Mỗi lần chạy: xóa rồi nạp lại 5 ngày gần nhất
(hôm nay + 4 ngày trước, giờ VN) trong 1 transaction; --start/--end để chạy khoảng ngày khác.
Mặc định chỉ chạy thử (kéo + kiểm, không ghi gì). Ghi BigQuery phải thêm --write.

    python tiktok_ads_creative_to_bq.py                                                 # chạy thử 5 ngày gần nhất
    python tiktok_ads_creative_to_bq.py --write                                         # xóa + nạp lại 5 ngày gần nhất
    python tiktok_ads_creative_to_bq.py --start 2026-10-01 --end 2026-10-31 --write    # khoảng ngày tùy chọn
    python tiktok_ads_creative_to_bq.py --start 2026-10-28 --end 2026-10-31 --excel tiktok_creative.xlsx
"""
import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests
from google.cloud import bigquery

TABLE = os.environ.get("BQ_TABLE_CREATIVE",
                       "rhysman-data-warehouse-488306.rhysman_bigcampaigns.fact_tiktok_ads_creative")
DAYS = 5
EXTRA_METRICS = ["creative_platform_status", "material_primary_source", "material_secondary_source"]

# ---------------------------------------------------------------- Cấu hình chung (giữ giống tiktok_ads_to_bq.py)
VN_TZ = timezone(timedelta(hours=7))
ENV_COOKIE = "TIKTOK_COOKIE"  # GitHub Actions: map từ secret COOKIE_ADS
ADVERTISER_ID = os.environ.get("TIKTOK_ADVERTISER_ID", "7625589331687948309")  # aadvid trên URL Ads Manager
API_URL = "https://ads.tiktok.com/api/v4/i18n/statistics/op/campaign/list/"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/156.0.0.0 Safari/537.36")
# Body như trang Campaign của Ads Manager gửi; st / et / page / filters / metrics điền lúc gọi.
BASE_BODY = {
    "common_req": {"lifetime": 0, "compare_st": "", "compare_et": "", "sort_stat": "create_time", "sort_order": 1,
                   "dimensions": ["campaign_id"], "page_size": 20},
    "workflow_card": {"type": "0"},
    "extra": {"get_compare_value": "0", "get_compare_rate": "0", "get_compare_diff": "0",
              "scene": "campaign_list_v2", "list_version": "v2", "page_view_scene": "view_non_first",
              "region_schedule": "1", "ab_primary_status": "v2", "enable_status_action": 1},
}

# 63 chỉ số: 44 cột đầu đúng thứ tự cột J..BA của file Excel, các cột thêm sau. (tên API, kiểu, ý nghĩa, tên trên TikTok Ads)
# kiểu: vnd -> NUMERIC, cnt -> INTEGER, pct -> FLOAT dạng thập phân (API trả 2.56 = 2,56%), ratio -> FLOAT
METRICS = [
    ("stat_cost", "vnd", "Chi phí đã tiêu", "Cost"),
    ("cpc", "vnd", "Chi phí trung bình mỗi click", "CPC (destination)"),
    ("cpm", "vnd", "Chi phí cho 1.000 lượt hiển thị", "CPM"),
    ("show_cnt", "cnt", "Lượt hiển thị", "Impressions"),
    ("click_cnt", "cnt", "Lượt click", "Clicks (destination)"),
    ("ctr", "pct", "Tỉ lệ click = Clicks / Impressions", "CTR (destination)"),
    ("time_attr_convert_cnt", "cnt", "Số chuyển đổi (theo thời điểm chuyển đổi)", "Conversions (conversion time)"),
    ("skan_convert_cnt", "cnt", "Chuyển đổi iOS SKAdNetwork", "SKAN conversions"),
    ("time_attr_conversion_cost", "vnd", "Chi phí / chuyển đổi (theo thời điểm chuyển đổi)", "Cost per conversion (conversion time)"),
    ("skan_conversion_cost", "vnd", "Chi phí / chuyển đổi SKAN", "SKAN cost per conversion"),
    ("time_attr_conversion_rate", "pct", "Tỉ lệ chuyển đổi trên click", "CVR (conversion time)"),
    ("time_attr_conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi trên hiển thị", "CVR impression (conversion time)"),
    ("skan_conversion_rate", "pct", "Tỉ lệ chuyển đổi SKAN (trên click)", "SKAN CVR"),
    ("skan_conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi SKAN (trên hiển thị)", "SKAN CVR (impression)"),
    ("time_attr_effect_cnt", "cnt", "Kết quả theo mục tiêu tối ưu", "Results (conversion time)"),
    ("time_attr_effect_cost", "vnd", "Chi phí / kết quả", "Cost per result (conversion time)"),
    ("time_attr_effect_rate", "pct", "Tỉ lệ kết quả", "Result rate (conversion time)"),
    ("time_attr_deep_convert_cnt_v2", "cnt", "Chuyển đổi sâu", "Deep conversions (conversion time)"),
    ("time_attr_cost_per_deep_convert_v2", "vnd", "Chi phí / chuyển đổi sâu", "Cost per deep conversion"),
    ("time_attr_deep_convert_rate_v2", "pct", "Tỉ lệ chuyển đổi sâu", "Deep CVR"),
    ("show_uv", "cnt", "Số người (unique) được tiếp cận", "Reach"),
    ("cost_per_show_uv_mille", "vnd", "Chi phí cho 1.000 người tiếp cận", "Cost per 1,000 people reached"),
    ("frequency", "ratio", "Tần suất = Impressions / Reach", "Frequency"),
    ("convert_cnt", "cnt", "Số chuyển đổi (theo thời điểm hiển thị)", "Conversions"),
    ("conversion_cost", "vnd", "Chi phí / chuyển đổi", "Cost per conversion"),
    ("conversion_rate_imp", "pct", "Tỉ lệ chuyển đổi trên hiển thị", "CVR (impression)"),
    ("effect_cnt", "cnt", "Kết quả", "Results"),
    ("effect_cost", "vnd", "Chi phí / kết quả", "Cost per result"),
    ("effect_rate", "pct", "Tỉ lệ kết quả", "Result rate"),
    ("total_play", "cnt", "Lượt xem video", "Video views"),
    ("play_duration_2s", "cnt", "Lượt xem ≥ 2 giây", "2-second video views"),
    ("play_duration_6s", "cnt", "Lượt xem ≥ 6 giây", "6-second video views"),
    ("play_over", "cnt", "Lượt xem hết 100% video", "Video views at 100%"),
    ("play_third_quartile", "cnt", "Lượt xem hết 75% video", "Video views at 75%"),
    ("play_midpoint", "cnt", "Lượt xem hết 50% video", "Video views at 50%"),
    ("play_first_quartile", "cnt", "Lượt xem hết 25% video", "Video views at 25%"),
    ("stat_total_play_rate", "pct", "Lượt xem / hiển thị", "Video view rate"),
    ("stat_play_duration_2s_rate", "pct", "Tỉ lệ xem ≥ 2s / hiển thị", "2-second view rate"),
    ("stat_play_duration_6s_rate", "pct", "Tỉ lệ xem ≥ 6s / hiển thị", "6-second view rate"),
    ("stat_play_first_quartile_rate", "pct", "Tỉ lệ xem hết 25% / hiển thị", "25% view rate"),
    ("stat_play_midpoint_rate", "pct", "Tỉ lệ xem hết 50% / hiển thị", "50% view rate"),
    ("stat_play_third_quartile_rate", "pct", "Tỉ lệ xem hết 75% / hiển thị", "75% view rate"),
    ("stat_play_over_rate", "pct", "Tỉ lệ xem hết 100% / hiển thị", "100% view rate"),
    ("total_engagement_rate", "pct", "Tỉ lệ tương tác tổng (like, comment, share, follow…)", "Engagement rate"),
    # 5 cột thêm 19/10/2026
    ("average_video_play", "ratio", "Thời gian xem trung bình mỗi lượt xem video (giây)", "Average play time per video view"),
    ("ad_net_like", "cnt", "Lượt thích từ quảng cáo", "Likes"),
    ("ad_comment", "cnt", "Bình luận từ quảng cáo", "Comments"),
    ("ad_share", "cnt", "Chia sẻ từ quảng cáo", "Shares"),
    ("ad_net_favorite", "cnt", "Lượt lưu (favorite) từ quảng cáo", "Favorites"),
    # 14 cột thêm 02/11/2026. Nhóm (TikTok) là sự kiện mua hàng trên TikTok ngoài TikTok Shop (API: nonshop_onsite)
    ("time_attr_nonshop_onsite_shopping_per_click", "pct", "Tỉ lệ mua hàng trên TikTok / click", "Purchase rate (TikTok)"),
    ("time_attr_nonshop_onsite_shopping", "cnt", "Lượt mua hàng trên TikTok", "Purchases (TikTok)"),
    ("time_attr_total_nonshop_onsite_shopping_value", "vnd", "Giá trị mua hàng trên TikTok", "Purchase value (TikTok)"),
    ("time_attr_nonshop_onsite_initiate_checkout_count_per_click", "pct", "Tỉ lệ bắt đầu thanh toán trên TikTok / click",
     "Checkout initiation rate (TikTok)"),
    ("time_attr_nonshop_onsite_initiate_checkout_count", "cnt", "Lượt bắt đầu thanh toán trên TikTok",
     "Checkouts initiated (TikTok)"),
    ("time_attr_total_nonshop_onsite_initiate_checkout_count_value", "vnd", "Giá trị bắt đầu thanh toán trên TikTok",
     "Checkout initiation value (TikTok)"),
    ("time_attr_nonshop_onsite_on_web_cart_per_click", "pct", "Tỉ lệ thêm vào giỏ trên TikTok / click",
     "Add to cart rate (TikTok)"),
    ("time_attr_nonshop_onsite_on_web_cart", "cnt", "Lượt thêm vào giỏ trên TikTok", "Adds to cart (TikTok)"),
    ("time_attr_total_nonshop_onsite_on_web_cart_value", "vnd", "Giá trị thêm vào giỏ trên TikTok", "Add to cart value (TikTok)"),
    ("time_attr_nonshop_onsite_shopping_roas", "ratio", "ROAS mua hàng trên TikTok = giá trị mua / chi phí",
     "Purchases ROAS (TikTok)"),
    ("time_attr_total_repetitive_in_app_detail_uv_value", "vnd", "Giá trị xem nội dung trong app", "Content view value (app)"),
    ("time_attr_total_omni_detail_value", "vnd", "Tổng giá trị xem nội dung (mọi kênh)",
     "Total view content value (all-channels)"),
    ("time_attr_total_on_web_detail_standardized_value", "vnd", "Giá trị xem nội dung trên website",
     "Content view value (website)"),
    ("time_attr_total_offline_detail_value", "vnd", "Giá trị xem nội dung offline", "Content view value (offline)"),
]
# 12 cột thêm 05/11/2026 (nhóm Product details page view), nằm SAU campaign_reach vì BigQuery thêm cột vào cuối bảng.
# Bản (TikTok) (API nonshop_onsite_*) = trang sản phẩm trên TikTok ngoài TikTok Shop: đủ 7 cột như dòng chọn cột trên
# Ads Manager. Bản Shop (API onsite_*) = trang sản phẩm trong TikTok Shop: API chỉ có 5 cột (không có unique / cost per
# unique). 11/2026: bản TikTok cả tài khoản = 0; bản Shop có lượt xem / tỉ lệ / chi phí, value trả "-" (ghi NULL).
METRICS_AFTER_REACH = [
    ("time_attr_unique_nonshop_onsite_on_web_detail", "cnt", "Số người (unique) xem trang chi tiết sản phẩm trên TikTok",
     "Unique product details page views (TikTok)"),
    ("time_attr_cost_per_unique_nonshop_onsite_on_web_detail", "vnd",
     "Chi phí / người xem trang chi tiết sản phẩm trên TikTok", "Cost per unique product details page view (TikTok)"),
    ("time_attr_nonshop_onsite_on_web_detail_per_click", "pct", "Lượt xem trang chi tiết sản phẩm trên TikTok / click",
     "Product details page view rate (TikTok)"),
    ("time_attr_nonshop_onsite_on_web_detail", "cnt", "Lượt xem trang chi tiết sản phẩm trên TikTok",
     "Product details page views (TikTok)"),
    ("time_attr_cost_per_nonshop_onsite_on_web_detail", "vnd", "Chi phí / lượt xem trang chi tiết sản phẩm trên TikTok",
     "Cost per product details page view (TikTok)"),
    ("time_attr_value_per_nonshop_onsite_on_web_detail", "vnd", "Giá trị / lượt xem trang chi tiết sản phẩm trên TikTok",
     "Value per product details page view (TikTok)"),
    ("time_attr_total_nonshop_onsite_on_web_detail_value", "vnd", "Giá trị xem trang chi tiết sản phẩm trên TikTok",
     "Product details page view value (TikTok)"),
    ("time_attr_onsite_on_web_detail_per_click", "pct", "Lượt xem trang chi tiết sản phẩm trên TikTok Shop / click",
     "Product details page view rate (Shop)"),
    ("time_attr_onsite_on_web_detail", "cnt", "Lượt xem trang chi tiết sản phẩm trên TikTok Shop",
     "Product details page views (Shop)"),
    ("time_attr_cost_per_onsite_on_web_detail", "vnd", "Chi phí / lượt xem trang chi tiết sản phẩm trên TikTok Shop",
     "Cost per product details page view (Shop)"),
    ("time_attr_value_per_onsite_on_web_detail", "vnd", "Giá trị / lượt xem trang chi tiết sản phẩm trên TikTok Shop",
     "Value per product details page view (Shop)"),
    ("time_attr_total_onsite_on_web_detail_value", "vnd", "Giá trị xem trang chi tiết sản phẩm trên TikTok Shop",
     "Product details page view value (Shop)"),
]
ALL_METRICS = METRICS + METRICS_AFTER_REACH
BQ_TYPE = {"vnd": "NUMERIC", "cnt": "INTEGER", "pct": "FLOAT", "ratio": "FLOAT"}
UNIT = {"vnd": " (VND)", "pct": " (dạng thập phân, 0.0225 = 2,25%)"}


def col_name(label):
    """Tên cột BigQuery = tên cột trên TikTok Ads bỏ ký tự đặc biệt, viết thường, nối bằng '_'.
    Vd 'CTR (destination)' -> ctr_destination, 'Cost per 1,000 people reached' -> cost_per_1000_people_reached."""
    return re.sub(r"[^0-9A-Za-z]+", "_", label.replace(",", "")).strip("_").lower()


COL = {k: col_name(ui) for k, _, _, ui in ALL_METRICS}  # tên API -> tên cột BigQuery
assert len(set(COL.values())) == len(COL), "trùng tên cột"

# Mã trạng thái đã đối chiếu với giao diện TikTok Ads; mã khác sẽ ghi "Khác (mã X)".
PLATFORM_STATUS = {"5": "Active", "2": "Not delivered", "7": "Paused"}
OPT_STATUS = {"0": "Bật", "1": "Tắt"}
# Không truyền filters = mọi trạng thái kể cả đã xóa (bộ lọc "999" của giao diện bỏ creative đã xóa).
STATUS = {**PLATFORM_STATUS, "8": "Deleted"}
# Mã mục tiêu campaign (Advertising objective) -> tên trên TikTok Ads. API không trả tên; đối chiếu 09/11/2026 trên 2 tài
# khoản: 5 = campaign tên mặc định "Phạm vi tiếp cận<giờ tạo>", tối ưu Reach / Consideration audience acquisition;
# 9 = "Tương tác với cộng đồng…", tối ưu TikTok page visits; 1 = "CLICK TO LIVE…", tối ưu Click.
OBJECTIVE = {"1": "Traffic", "5": "Reach", "9": "Community interaction"}

F = bigquery.SchemaField
SCHEMA = [
    F("report_date", "DATE", "REQUIRED", description="Ngày dữ liệu theo múi giờ tài khoản quảng cáo TikTok (UTC+7)"),
    F("advertiser_id", "STRING", "REQUIRED", description="ID tài khoản quảng cáo TikTok (aadvid)"),
    F("campaign_id", "STRING", "REQUIRED", description="Campaign ID"),
    F("campaign_name", "STRING", "REQUIRED", description="Campaign name"),
    F("adgroup_id", "STRING", "REQUIRED", description="Ad group ID trên TikTok Ads (Smart+: API ad_id)"),
    F("adgroup_name", "STRING", description="Ad group name trên TikTok Ads (Smart+: API ad_name)"),
    F("ad_id", "STRING", "REQUIRED", description="Ad ID trên TikTok Ads = 1 creative (Smart+: API creative_id)"),
    F("ad_name", "STRING", description="Ad name trên TikTok Ads (API creative_name)"),
    F("video_id", "STRING", description="Mã video TikTok của creative (API video_info.video_id); trống với bài ảnh"),
    F("post_text", "STRING", description="Nội dung bài đăng / caption (API creative_post_name)"),
    F("source", "STRING", description="Cột Source trên TikTok Ads, vd 'Your own content · TikTok account' "
                                      "(API material_primary_source · material_secondary_source)"),
    F("on_off", "STRING", description="Cột On/Off trên TikTok Ads: Bật / Tắt (API creative_opt_status), "
                                      "tại lần kéo dữ liệu gần nhất của ngày đó"),
    F("status", "STRING", description="Cột Status trên TikTok Ads: Active / Not delivered / Paused / Deleted "
                                      "(API creative_platform_status), tại lần kéo dữ liệu gần nhất của ngày đó"),
]


def metric_field(k, kind, vi, ui):
    return F(COL[k], BQ_TYPE[kind], description=f"{vi}{UNIT.get(kind, '')}. TikTok Ads: {ui}. API: {k}")


SCHEMA += [metric_field(*m) for m in METRICS] + [  # 63 cột chỉ số: cùng tên, kiểu, mô tả với bảng campaign cũ
    F("campaign_reach", "INTEGER",
      description="Reach của cả campaign trong ngày (tab Campaign trên TikTok Ads, API show_uv cấp campaign), lặp lại "
                  "trên mọi dòng creative của campaign. Reach không cộng từ creative được (1 người xem nhiều creative "
                  "bị đếm nhiều lần): tính Reach / Frequency / Cost per 1,000 people reached của campaign dùng cột này, "
                  "lấy 1 giá trị cho mỗi campaign × ngày."),
] + [metric_field(*m) for m in METRICS_AFTER_REACH] + [
    F("objective_type", "INTEGER",
      description="Mã mục tiêu quảng cáo (Advertising objective) của campaign, API objective_type: 1 = Traffic "
                  "(Lưu lượng truy cập), 5 = Reach (Phạm vi tiếp cận), 9 = Community interaction (Tương tác với cộng "
                  "đồng). Cố định từ lúc tạo campaign, lặp trên mọi dòng creative của campaign."),
    F("objective_name", "STRING",
      description="Tên mục tiêu quảng cáo của campaign trên TikTok Ads (Traffic / Reach / Community interaction), "
                  "script đổi từ objective_type vì API không trả tên; mã chưa biết ghi 'Khác (mã X)'."),
]


RETRY_WAITS = [10, 30, 60, 120]  # giây; TikTok thỉnh thoảng trả code 1300400001 "Couldn't load" khi gọi liên tục
DAY_TRIES = 3  # ngày đang chạy: chi phí tăng giữa lúc gọi creative và lúc gọi campaign → gọi lại ngày đó
TODAY_WAIT = 60  # giây chờ trước khi gọi lại ngày hôm nay: trong ngày số cấp creative cập nhật trễ hơn cấp campaign vài phút
TODAY_TOLERANCE = 0.01  # ngày hôm nay hết lượt gọi vẫn lệch: chấp nhận nếu creative <= campaign và lệch <= 1%


class TikTokAuthError(RuntimeError):
    pass


# ---------------------------------------------------------------- TikTok
def read_cookie():
    """Cookie TikTok Ads từ env TIKTOK_COOKIE (GitHub Actions: secret COOKIE_ADS) — cả chuỗi cookie của trình duyệt,
    có hoặc không chữ 'cookie:' ở đầu, dạng copy từ "Copy as cURL (cmd)" (có dấu ^) cũng được."""
    raw = os.environ.get(ENV_COOKIE, "").strip()
    if not raw:
        raise SystemExit(f"[CONFIG] env {ENV_COOKIE} trống — GitHub Actions: map từ secret COOKIE_ADS")
    if "^" in raw:  # chuỗi copy từ "Copy as cURL (cmd)"
        raw = re.sub(r"\^(.)", r"\1", raw)
    raw = re.sub(r"^(cookie:\s*)", "", raw, flags=re.I).strip().strip('"')
    return raw.replace("\r", "").replace("\n", "")


def api_request(cookie):
    """(url, headers) gọi API thống kê của ADVERTISER_ID; header x-csrftoken = cookie csrftoken (thiếu là bị từ chối)."""
    m = re.search(r"(?:^|;\s*)csrftoken=([^;\s]+)", cookie)
    if not m:
        raise SystemExit("[CONFIG] cookie trong secret COOKIE_ADS thiếu csrftoken — API thống kê cần cả chuỗi cookie: "
                         "copy lại toàn bộ giá trị cookie của request campaign/list rồi cập nhật secret")
    url = f"{API_URL}?aadvid={ADVERTISER_ID}&req_src=bidding"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://ads.tiktok.com",
        "referer": f"https://ads.tiktok.com/i18n/manage/campaign?aadvid={ADVERTISER_ID}",
        "user-agent": USER_AGENT,
        "cookie": cookie,
        "x-csrftoken": m.group(1),
    }
    return url, headers


def post_with_retry(session, url, headers, body):
    """POST, thử lại khi lỗi tạm thời (5xx, mạng, code != 0); 401/403 hoặc hết lượt thử -> TikTokAuthError."""
    data = json.dumps(body, separators=(",", ":")).encode()
    for attempt, wait in enumerate(RETRY_WAITS + [None], 1):
        try:
            r = session.post(url, headers=headers, data=data, timeout=60)
            if r.status_code in (401, 403):
                raise TikTokAuthError(f"HTTP {r.status_code}")
            r.raise_for_status()
            j = r.json()
            if j.get("code") == 0:
                return j
            err = f"code={j.get('code')} msg={j.get('msg')}"
        except (requests.RequestException, ValueError) as e:
            err = f"{type(e).__name__}: {str(e)[:200]}"
        if wait is None:
            raise TikTokAuthError(f"{err} (sau {attempt} lần thử)")
        print(f"    lỗi tạm ({err}), thử lại sau {wait}s", flush=True)
        time.sleep(wait)


def fetch_all(session, url, headers, base_body, day, level, metrics):
    """Mọi dòng (mọi trạng thái, kể cả đã xóa) của 1 ngày: level "creative" = tab Ad, "campaign" = tab Campaign."""
    rows, page, page_count = [], 1, 1
    while page <= page_count:
        body = json.loads(json.dumps(base_body))
        req = body["common_req"]
        req["st"] = req["et"] = day.isoformat()
        req["page"], req["page_size"] = page, 100
        req["dimensions"] = [f"{level}_id"]
        req["filters"] = []
        req["metrics"] = metrics
        path = "/ad/list/" if level == "creative" else "/campaign/list/"
        j = post_with_retry(session, url.replace("/campaign/list/", path), headers, body)
        rows += j["data"]["table"]
        page_count = int(j["data"]["pagination"]["page_count"])
        if int(j["data"]["pagination"]["total_count"]) == 0:
            raise RuntimeError(f"API trả 0 {level} cho ngày {day} — kiểm tra lại curl/tài khoản")
        page += 1
        time.sleep(1)
    return rows


def cost_of(rows):
    return sum(float(r["row_data"]["stat_cost"] or 0) for r in rows)


def fetch_day(session, url, headers, base_body, day, is_today):
    """Creative + campaign của 1 ngày; tổng chi phí creative phải khớp cấp campaign. Ngày đang chạy chi phí tăng giữa các
    lần gọi → gọi lại, chấp nhận khi chi phí creative nằm giữa chi phí campaign lần gọi trước và lần gọi ngay sau
    (chi phí trong ngày chỉ tăng; thiếu creative thật thì vẫn lệch). Trả (creatives, camps, chi phí campaign lần trước).
    Ngày hôm nay: TikTok cập nhật số cấp campaign trước, cấp creative trễ vài phút → chờ TODAY_WAIT giây rồi gọi lại;
    hết lượt vẫn lệch mà creative <= campaign và lệch <= TODAY_TOLERANCE thì cảnh báo rồi vẫn ghi (các lần chạy sau kéo
    lại ngày này, lúc đó đã là ngày qua nên phải khớp tuyệt đối). Ngày đã qua lệch là dừng, không ghi."""
    prev = None
    for attempt in range(1, DAY_TRIES + 1):
        creatives = fetch_all(session, url, headers, base_body, day, "creative",
                              [m[0] for m in ALL_METRICS] + EXTRA_METRICS)
        camps = fetch_all(session, url, headers, base_body, day, "campaign", ["stat_cost", "show_uv"])
        cr, hi = round(cost_of(creatives)), round(cost_of(camps))
        lo = hi if prev is None else round(cost_of(prev))
        if lo <= cr <= hi:
            return creatives, camps, lo
        print(f"    {day}: chi phí cấp creative {cr:,} ngoài khoảng cấp campaign {lo:,} – {hi:,} "
              f"(lần {attempt}/{DAY_TRIES})", flush=True)
        prev = camps
        if is_today and attempt < DAY_TRIES:
            time.sleep(TODAY_WAIT)
    if is_today and cr <= hi and hi - cr <= TODAY_TOLERANCE * hi:
        print(f"::warning::{day} là hôm nay, số cấp creative chưa cập nhật kịp: creative {cr:,} thấp hơn campaign {hi:,} "
              f"{hi - cr:,} ({(hi - cr) / hi:.2%}) — vẫn ghi, lần chạy ngày mai kéo lại số chốt", flush=True)
        return creatives, camps, hi
    raise SystemExit(f"Chi phí ngày {day} lệch cấp campaign sau {DAY_TRIES} lần gọi — dừng, không ghi")


def video_id(cr):
    try:
        return ", ".join(v["video_id"] for v in json.loads(cr.get("video_info") or "[]")) or None
    except (ValueError, TypeError, KeyError):
        return None


def conv(v, kind):
    if v in (None, "", "-"):
        return None
    f = float(v)
    if kind == "pct":
        return round(f / 100, 10)
    if kind == "vnd":
        return str(int(f)) if f.is_integer() else str(f)  # NUMERIC: gửi dạng chuỗi cho chính xác
    if kind == "cnt":
        return int(f)
    return f


def to_bq_row(cr, day, advertiser_id, campaign_reach):
    rd = cr["row_data"]
    missing = [k for k, *_ in ALL_METRICS if k not in rd]
    if missing:
        raise RuntimeError(f"API không trả các cột {missing} cho creative {cr['creative_id']}")
    src = " · ".join(s for s in (rd.get("material_primary_source"), rd.get("material_secondary_source")) if s)
    row = {
        "report_date": day.isoformat(),
        "advertiser_id": advertiser_id,
        "campaign_id": str(cr["campaign_id"]),
        "campaign_name": cr["campaign_name"],
        "adgroup_id": str(cr["ad_id"]),
        "adgroup_name": cr.get("ad_name"),
        "ad_id": str(cr["creative_id"]),
        "ad_name": (cr.get("creative_name") or "").strip() or None,
        "video_id": video_id(cr),
        "post_text": cr.get("creative_post_name") if cr.get("creative_post_name") not in (None, "", "-") else None,
        "source": src or None,
        "on_off": OPT_STATUS.get(str(cr.get("creative_opt_status")), str(cr.get("creative_opt_status"))),
        "status": STATUS.get(rd.get("creative_platform_status"), f"Khác (mã {rd.get('creative_platform_status')})"),
    }
    row.update({COL[k]: conv(rd[k], kind) for k, kind, *_ in METRICS})
    row["campaign_reach"] = campaign_reach.get(str(cr["campaign_id"]))
    row.update({COL[k]: conv(rd[k], kind) for k, kind, *_ in METRICS_AFTER_REACH})
    ot = cr.get("objective_type")
    ot = None if ot in (None, "", "-") else str(ot)
    row["objective_type"] = int(ot) if ot is not None else None
    row["objective_name"] = None if ot is None else OBJECTIVE.get(ot, f"Khác (mã {ot})")
    return row


# ---------------------------------------------------------------- BigQuery
def bq_client():
    """Giống các job khác trong repo: JSON inline GOOGLE_SERVICE_ACCOUNT_JSON -> ADC (gcloud / key file)."""
    project = TABLE.split(".")[0]
    key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not key_json:
        print("[BQ] credentials: ADC của máy (gcloud)", flush=True)
        return bigquery.Client(project=project)
    from google.oauth2 import service_account
    try:
        info = json.loads(key_json)
    except json.JSONDecodeError as e:
        raise SystemExit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
    print(f"[BQ] credentials: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email', '?')})", flush=True)
    return bigquery.Client(project=project, credentials=service_account.Credentials.from_service_account_info(info))


def ensure_table(client):
    table = bigquery.Table(TABLE, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="report_date")
    table.clustering_fields = ["campaign_id", "adgroup_id", "ad_id"]
    table.description = ("TikTok Ads – chỉ số theo creative (tab Ad trên Ads Manager) theo ngày. Nguồn: ads.tiktok.com "
                         "/api/v4/i18n/statistics/op/ad/list, dimensions = creative_id, mỗi ngày gọi với "
                         "st = et = report_date. Ghi cả creative không phát sinh trong ngày (số = 0).")
    table = client.create_table(table, exists_ok=True)
    have, want = [f.name for f in table.schema], [f.name for f in SCHEMA]
    if have != want:
        raise RuntimeError(f"Schema bảng {TABLE} khác script.\n  bảng:   {have}\n  script: {want}")


def replace_range(client, rows, start, end, advertiser_id):
    """Xóa [start, end] của advertiser rồi chèn rows — trong 1 transaction để không bao giờ mất dữ liệu giữa chừng."""
    dataset = TABLE.rsplit(".", 1)[0]
    staging = f"{dataset}._staging_tiktok_creative_{uuid.uuid4().hex[:8]}"
    tmp = bigquery.Table(staging, schema=SCHEMA)
    tmp.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(tmp)
    try:
        if rows:
            client.load_table_from_json(
                rows, staging, job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE")
            ).result()
        cols = ", ".join(f"`{f.name}`" for f in SCHEMA)
        script = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{TABLE}` WHERE advertiser_id = @adv AND report_date BETWEEN @start AND @end;
        INSERT INTO `{TABLE}` ({cols}) SELECT {cols} FROM `{staging}`;
        COMMIT TRANSACTION;
        """
        job = client.query(script, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("adv", "STRING", advertiser_id),
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]))
        job.result()
        affected = {c.statement_type: c.num_dml_affected_rows for c in client.list_jobs(parent_job=job.job_id)
                    if c.statement_type in ("DELETE", "INSERT")}
        return affected.get("DELETE", 0), affected.get("INSERT", 0)
    finally:
        client.delete_table(staging, not_found_ok=True)


# ---------------------------------------------------------------- Excel
def write_excel(rows, path):
    from openpyxl import Workbook

    fmt = {"vnd": "#,##0", "cnt": "#,##0", "pct": "0.00%", "ratio": "0.00"}
    cols = [("report_date", "Date", "dd/mm/yyyy"), ("campaign_id", "Campaign ID", "@"),
            ("campaign_name", "Campaign name", None), ("adgroup_id", "Ad group ID", "@"),
            ("adgroup_name", "Ad group name", None), ("ad_id", "Ad ID", "@"), ("ad_name", "Ad name", None),
            ("video_id", "Video ID", "@"), ("post_text", "Post text", None), ("source", "Source", None),
            ("on_off", "On/Off", None), ("status", "Status", None),
            ("objective_type", "Objective type", None), ("objective_name", "Objective", None)]
    cols += ([(COL[k], ui, fmt[kind]) for k, kind, _, ui in METRICS] + [("campaign_reach", "Campaign reach", "#,##0")]
             + [(COL[k], ui, fmt[kind]) for k, kind, _, ui in METRICS_AFTER_REACH])
    vnd_cols = {COL[k] for k, kind, *_ in ALL_METRICS if kind == "vnd"}
    wide = {"Campaign name": 32, "Ad group name": 28, "Ad name": 45, "Post text": 45, "Video ID": 36}
    wb = Workbook()
    ws = wb.active
    ws.title = "TikTok Ads - Creative"
    ws.append([label for _, label, _ in cols])
    for r in rows:
        ws.append([date.fromisoformat(r[c]) if c == "report_date"
                   else float(r[c]) if c in vnd_cols and r[c] is not None  # NUMERIC đang là chuỗi
                   else r[c] for c, _, _ in cols])
    for i, (_, label, f) in enumerate(cols, 1):
        letter = ws.cell(row=1, column=i).column_letter
        ws.column_dimensions[letter].width = wide.get(label, max(12, min(len(label) + 2, 30)))
        if f:
            for (cell,) in ws.iter_rows(min_row=2, min_col=i, max_col=i):
                cell.number_format = f
    ws.freeze_panes = "H2"
    ws.auto_filter.ref = ws.dimensions
    wb.save(path)


# ---------------------------------------------------------------- main
def date_range(args):
    if args.start or args.end:
        start = date.fromisoformat(args.start or args.end)
        end = date.fromisoformat(args.end or args.start)
    else:
        end = datetime.now(VN_TZ).date()
        start = end - timedelta(days=args.days - 1)
    if start > end:
        raise SystemExit("--start phải <= --end")
    if (end - start).days > 92:
        raise SystemExit("Mỗi lần chạy tối đa 93 ngày")
    return start, end


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--days", type=int, default=DAYS, help=f"số ngày gần nhất tính cả hôm nay (mặc định {DAYS})")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD")
    p.add_argument("--write", action="store_true", help="ghi BigQuery (mặc định chỉ chạy thử)")
    p.add_argument("--excel", help="ghi ra file Excel này thay vì BigQuery")
    args = p.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    start, end = date_range(args)

    url, headers = api_request(read_cookie())
    base_body, advertiser_id = BASE_BODY, ADVERTISER_ID
    print(f"Advertiser {advertiser_id} | {start} -> {end} | bảng {TABLE}")
    session = requests.Session()
    rows, day, today = [], start, datetime.now(VN_TZ).date()
    try:
        while day <= end:
            creatives, camps, camp_before = fetch_day(session, url, headers, base_body, day, day == today)
            campaign_reach = {str(c["campaign_id"]): conv(c["row_data"].get("show_uv"), "cnt") for c in camps}
            day_rows = sorted((to_bq_row(c, day, advertiser_id, campaign_reach) for c in creatives),
                              key=lambda r: -float(r["cost"] or 0))
            cr_cost = sum(float(r["cost"] or 0) for r in day_rows)
            camp_cost = cost_of(camps)
            active = sum(1 for r in day_rows if float(r["cost"] or 0) > 0)
            deleted = sum(1 for r in day_rows if r["status"] == "Deleted")
            moving = f" (lần gọi trước {camp_before:,})" if camp_before != round(camp_cost) else ""
            print(f"  {day}: {len(day_rows)} creative ({deleted} đã xóa, {active} có chi phí)"
                  f" | chi phí cấp creative {cr_cost:,.0f} | cấp campaign {camp_cost:,.0f}{moving}", flush=True)
            rows += day_rows
            day += timedelta(days=1)
    except TikTokAuthError as e:
        print(f"::error::TikTok từ chối request ({e}). Cookie có thể đã hết hạn: copy lại cookie của request "
              "campaign/list và cập nhật secret COOKIE_ADS.", file=sys.stderr)
        sys.exit(2)

    keys = {(r["report_date"], r["ad_id"]) for r in rows}
    if len(keys) != len(rows):
        raise SystemExit("Trùng (report_date, ad_id) — dừng, không ghi")
    if args.excel:
        write_excel(rows, args.excel)
        print(f"Excel: {len(rows)} dòng -> {args.excel}")
        return
    if not args.write:
        print(f"Chạy thử: {len(rows)} dòng × {len(SCHEMA)} cột, chưa ghi BigQuery (thêm --write để ghi).")
        print(json.dumps(rows[0], ensure_ascii=False, indent=1))
        return
    client = bq_client()
    ensure_table(client)
    deleted, inserted = replace_range(client, rows, start, end, advertiser_id)
    print(f"BigQuery: xóa {deleted} dòng cũ, chèn {inserted} dòng mới ({start} -> {end})")


if __name__ == "__main__":
    main()



