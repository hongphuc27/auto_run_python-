# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# TikTok Seller — NGƯỜI NHẬN HOA HỒNG theo đơn, nạp BigQuery fact_creator_tiktok.

# Vì sao có script này: job affiliate cũ ngừng ghi nhận 3 tài khoản nhà (rhysman.com,
# rhysman.shopping, rhysman_channel) từ 29/06/2026, trong khi TikTok VẪN trả dữ liệu.
# Script này lấy thẳng từ API danh sách đơn của Seller Center.

# NGUỒN DỮ LIỆU (quan trọng, đừng đổi sang file export):
#   Người nhận hoa hồng nằm ở  main_orders[].sku_module[].creator_info_name.items[]
#   phần tử position=3 -> "Người nhận hoa hồng: <handle>".
#   Cột "Creator Handle" trong file export .xlsx KHÔNG phải trường này — nó để trống
#   cho toàn bộ đơn Thẻ sản phẩm, thiếu ~27% dữ liệu. Đã kiểm chứng 06/08/2026.

#   promotion_position_type lấy bằng cách lọc sale_source (mỗi đơn thuộc đúng 1 kênh,
#   đã đối chiếu 501/501 đơn ngày 05/08/2026 khớp với cột Order Channel của file export):
#       sale_source 1 = LIVE          -> promotion_position_type 3
#       sale_source 2 = Video         -> promotion_position_type 2
#       sale_source 3 = Thẻ sản phẩm  -> promotion_position_type 1

# CÁCH GỌI API — LANDING MẶC ĐỊNH KHÔNG PHẢI /order, ĐÂY LÀ CHỦ Ý:
#   Script chạy fetch bên trong Chromium (Playwright) để request đi từ một trang cùng
#   origin với Seller Center. Trang đó mặc định là /robots.txt.
#   /order là SPA: load xong nó còn hydrate, tự gọi CHÍNH cái API này, và nạp SDK captcha
#   — tất cả trên đúng main thread mà page.evaluate() cần. Máy nhiều nhân nuốt trôi nên
#   không ai thấy; runner 2 vCPU của GitHub Actions thì evaluate bị bỏ đói và treo.
#   Hai đường cho dữ liệu giống hệt nhau (đối chiếu 288 dòng ngày 31/08/2026).

# CHỐNG TREO — BA TẦNG, ĐỪNG GỠ TẦNG NÀO:
#   1. CHẾT HẲN, thoát ngay, không xoá gì:
#        HTTP 401/403, code 98001002 / 98001008, bị đá sang trang login/passport.
#   2. BỊ CHẶN, đập browser dựng lại tại đúng cursor:
#        overlay CAPTCHA, URL verify/captcha, code 10000, context bị huỷ, evaluate quá hạn.
#        Cursor chỉ tăng sau khi một trang trả thành công -> không mất, không trùng dữ liệu.
#   3. TRỤC TRẶC TẠM THỜI, thử lại TẠI CHỖ, không đập browser:
#        code lạ (vd 21008301 "Lỗi hệ thống"), HTTP 5xx, JS fetch lỗi mạng.
#        Xếp nhóm này vào tầng 2 là sai: một blip gateway bị khuếch đại thành reload nguyên
#        Chromium, mà reload mới chính là lúc dễ treo nhất.

#   page.evaluate() BẮT BUỘC phải bọc asyncio.wait_for. Playwright không có timeout cho
#   evaluate và set_default_timeout() KHÔNG áp vào nó; AbortController đặt trong JS cũng vô
#   dụng vì setTimeout của nó nằm trên đúng main thread đang bị khoá. Gỡ lớp này ra là job
#   treo tới khi CI cancel — đã xảy ra thật, treo 2 tiếng, hỏng ~11/12 run mỗi ngày.
#   Script KHÔNG giải CAPTCHA: chỉ phát hiện, bỏ browser đó đi, dựng cái sạch, đi tiếp.
#   Hết ngân sách restart thì DỪNG TRƯỚC bước BigQuery, dữ liệu cũ giữ nguyên.

# VÍ DỤ:
#   py -X utf8 keo_creator_IH.py                      # 30 ngày gần nhất
#   py -X utf8 keo_creator_IH.py --date 2026-08-05 --no-bq --csv out.csv
#   py -X utf8 keo_creator_IH.py --from 2026-06-29 --to 2026-07-31   # backfill
#   py -X utf8 keo_creator_IH.py --last-days 3 --cookie cookie_shop2.txt

# Idempotent: load vào bảng staging, rồi DELETE (id_shop x creator_username x khoảng
# create_time) + INSERT trong MỘT transaction. Chạy lại bao nhiêu lần cũng không nhân đôi,
# và load hỏng thì bảng thật không bị thủng. Service account cần quyền tạo/xoá bảng trong
# dataset (roles/bigquery.dataEditor là đủ).

# create_time ghi xuống BigQuery là UTC (không tzinfo) — đúng quy ước sẵn có của bảng,
# đã đối chiếu phân bố giờ của dữ liệu cũ ngày 06/08/2026. Đừng đổi sang giờ VN.

# Kéo thiếu là hỏng dữ liệu chứ không phải chạy chậm: vì nap_bq xoá cả khoảng ngày rồi ghi
# lại, script sẽ DỪNG HẲN (không đụng BigQuery) nếu chạm trần MAX_PAGES mà TikTok còn báo
# dữ liệu, hoặc nếu số đơn lấy được ít hơn total_count TikTok tự báo. Chỉ dùng
# --skip-total-check khi đã kiểm tay và biết chắc total_count của TikTok sai.

# BỘ TEST: D:\\Kho\\_lab\\creator-captcha\\test_offline.py — 21 kịch bản / 76 assert, chạy
# với Seller Center giả lập, không cần cookie và không đụng BigQuery. Sửa file này xong
# BẮT BUỘC chạy lại:  cd D:\\Kho\\_lab\\creator-captcha && python -X utf8 test_offline.py
# """
# import argparse, asyncio, datetime, json, os, re, sys, time, urllib.parse, uuid

# # ---------------------------------------------------------------- cấu hình
# # Hằng số cứng, KHÔNG đọc từ biến môi trường. Test offline đổi host bằng cách gán thẳng
# # lab.BASE / lab.API (xem test_offline.py), nên không cần cửa hậu qua env — mà cửa hậu đó
# # trên Actions chỉ là một đường để set nhầm rồi trỏ script sang host lạ.
# BASE = "https://seller-vn.tiktok.com"
# API = BASE + "/api/fulfillment/order/list"
# COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
# ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
# TZ = datetime.timezone(datetime.timedelta(hours=7))

# BQ_PROJECT = "rhysman-data-warehouse-488306"
# BQ_DATASET = "rhysman"
# BQ_TABLE = "fact_creator_tiktok"

# # Số ngày kéo khi không truyền --date/--from/--to. Đổi ở ĐÂY và ở LAST_DAYS trong
# # workflow (cả hai phải khớp, nếu không chạy tay và chạy tự động sẽ ra khác nhau).
# # Mỗi run XOÁ rồi GHI LẠI trọn cửa sổ này, nên tăng số là tăng cả thời gian chạy, số
# # partition BigQuery bị viết lại, lẫn thời gian phơi nhiễm với lỗi treo.
# LAST_DAYS_MAC_DINH = 30

# CREATORS = {
#     "rhysman.com": "Rhys Man Chính Hãng",
#     "rhysman.shopping": "Rhys Man",
#     "rhysman_channel": "Rhys Man Chăm Sóc Cơ Thể",
# }

# SALE_SOURCE_TO_POSITION = {"1": 3, "2": 2, "3": 1}
# SALE_SOURCE_TEN = {"1": "LIVE", "2": "Video", "3": "Thẻ sản phẩm"}

# RE_NGUOI_NHAN = re.compile(r"người nhận hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
# RE_LONG = re.compile(r"hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
# PAGE_SIZE = 100
# MAX_PAGES = 600

# # --- thời gian ---------------------------------------------------------------
# # Thứ tự phải là: JS abort < timeout Python < deadline tổng. JS abort bắn trước cho ra
# # lỗi sạch (thử lại tại chỗ được); timeout Python là lưới cuối khi renderer chết hẳn và
# # JS abort không chạy nổi.
# PAGE_TIMEOUT_MS = 90_000
# JS_FETCH_TIMEOUT_MS = 60_000
# EVAL_TIMEOUT_S = 90.0          # trần cứng cho page.evaluate — thứ bản prod THIẾU
# CANARY_TIMEOUT_S = 15.0        # renderer còn sống không, hỏi ngay sau khi launch
# CLOSE_TIMEOUT_S = 20.0         # browser.close() cũng treo được khi renderer đang spin
# BROWSER_SETTLE_MS = 4_000
# DEADLINE_PHUT_MAC_DINH = 45

# # Trang để inject cookie. Chỉ cần CÙNG ORIGIN là fetch gửi kèm cookie — không cần mở SPA.
# # Mặc định KHÔNG phải /order, và đây là chủ ý: /order là SPA, sau khi load xong nó còn
# # hydrate + tự gọi chính /api/fulfillment/order/list + nạp SDK captcha, tất cả trên đúng
# # cái main thread mà page.evaluate() cần. Máy nhiều nhân nuốt trôi nên local không thấy;
# # runner 2 vCPU của GitHub Actions thì evaluate bị bỏ đói -> "renderer treo".
# # Test spa_nang_order_vs_robots chạy cùng một kịch bản với 2 landing: /order chết cả run,
# # /robots.txt sạch. Dữ liệu hai đường giống hệt nhau (đối chiếu 288 dòng ngày 31/08/2026).
# LANDING_MAC_DINH = "/robots.txt"

# # --- ngân sách thử lại -------------------------------------------------------
# BROWSER_RESTARTS = 3           # số restart LIÊN TIẾP cho phép (reset sau mỗi lần thành công)
# MAX_RESTART_TONG = 12          # trần cộng dồn cả run, chống flap vô tận
# BROWSER_RESTART_WAIT_SECONDS = 5
# API_RETRY_TAI_CHO = 5          # thử lại tại chỗ trước khi nghĩ tới chuyện đập browser
# # Thang chờ kéo dài tới 2 phút vì tầng 3 nay gánh cả code 10000 (TikTok bắt xác minh),
# # vốn cần thời gian nguội chứ không phải thử lại ngay. Blip gateway thường vẫn qua ở
# # lần đầu 3s nên không bị chậm oan.
# API_RETRY_SLEEP = (3, 10, 30, 60, 120)
# HEARTBEAT_MOI = 20             # log tiến độ mỗi N trang

# # Cắt khoảng ngày thành khối nhỏ. MẶC ĐỊNH TẮT (0) — giữ lại cờ vì có thể còn cần, nhưng
# # đừng bật nếu không có lý do cụ thể.
# #
# # Vì sao tắt: ban đầu bật để chống giả thuyết "cursor vỡ khi đi sâu quá trang 79". Log
# # 10/09/2026 phủ nhận giả thuyết đó — với chia khối, run vẫn chết nhưng ở TRANG 14 của
# # khối 3, tức không liên quan độ sâu. Đếm lại thì lộ ra sự thật:
# #     run không chia khối : 78 request thành công, chặn ở request 79, sau 5m35s
# #     run có chia khối    : 78 request thành công, chặn ở request 79, sau 5m28s
# # Hai cấu trúc hoàn toàn khác nhau, cùng một bức tường => HẠN MỨC CHO CẢ RUN (~78 request
# # hoặc ~5,5 phút; hai con số lẫn nhau vì nhịp đều 4,2s/request).
# #
# # Dưới hạn mức đó, chia khối còn PHẢN TÁC DỤNG: mỗi khối phải gọi lại trang đầu cho cả 3
# # kênh, nên 5 khối = 15 request mở đầu thay vì 3 — đốt mất ~12 trong 78 request.
# CHUNK_NGAY_MAC_DINH = 0

# # Chỉ dùng để CẢNH BÁO TRƯỚC khi chạy, không ảnh hưởng dữ liệu.
# # DON_MOI_NGAY_UOC: đo từ log run 08:45 ngày 10/09/2026 — khối 1+2 đã kéo trọn 14 ngày,
# #   tổng 6.159 đơn cả 3 kênh => ~440 đơn/ngày. Shop lớn dần thì nên đo lại.
# # HAN_MUC_REQUEST_UOC: số request lớn nhất một run đi được trước khi TikTok trả code
# #   10000 liên tục. Quan sát 78 ở CẢ HAI run ngày 10/09/2026.
# DON_MOI_NGAY_UOC = 440
# HAN_MUC_REQUEST_UOC = 78

# # Overlay CAPTCHA của Seller Center nằm TRÊN CÙNG url /order, không đổi URL — nên bản
# # prod (chỉ dò chuỗi trong URL) không bao giờ thấy nó.
# CAPTCHA_SELECTORS = ", ".join([
#     "#captcha_container",
#     "#captcha-verify-page",
#     "[class*='captcha']",
#     "[id*='captcha']",
#     "[class*='secsdk']",
# ])

# # CHỈ khớp selector là KHÔNG ĐỦ. Seller Center LUÔN nhúng sẵn 2 thẻ script
# #   <script id="oec-ttweb-captcha-config">  và  <script id="lucifer-captcha-loader-js">
# # nên bản dò đầu tiên báo "có CAPTCHA" ở mọi lần mở trang, trong khi API vẫn trả code=0
# # (đã kiểm chứng 31/08/2026: total_count=192, không hề bị chặn). Vì vậy phải loại thẻ
# # không phải giao diện VÀ bắt buộc element thật sự hiện ra màn hình.
# JS_DO_CAPTCHA = """
# (sel) => {
#     const bo_qua = new Set(['script', 'style', 'link', 'meta', 'template', 'noscript', 'head']);
#     for (const el of document.querySelectorAll(sel)) {
#         const tag = el.tagName.toLowerCase();
#         if (bo_qua.has(tag)) continue;
#         const r = el.getBoundingClientRect();
#         if (r.width <= 0 || r.height <= 0) continue;
#         const st = window.getComputedStyle(el);
#         if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') continue;
#         return {
#             tag: tag,
#             id: el.id || '',
#             cls: (el.className || '').toString().slice(0, 100),
#             w: Math.round(r.width), h: Math.round(r.height)
#         };
#     }
#     return null;
# }
# """

# # Dấu hiệu bị chặn khi phản hồi không phải JSON (TikTok trả thẳng trang HTML xác minh).
# DAU_HIEU_CHAN = ("captcha", "secsdk", "verify", "/passport", "risk_control")


# def uoc_so_request(so_ngay, page_size):
#     """Ước số request cần cho cửa sổ `so_ngay` ngày. CHỈ để cảnh báo trước khi chạy —
#     không ảnh hưởng dữ liệu. +3 vì mỗi kênh tốn tối thiểu 1 trang."""
#     return -(-(DON_MOI_NGAY_UOC * so_ngay) // page_size) + 3


# def log(*a):
#     print(datetime.datetime.now(TZ).strftime("%H:%M:%S"), *a, flush=True)


# # ---------------------------------------------------------------- cookie
# def chuan_hoa_cookie(cookie):
#     """Cookie phải nằm trên một dòng để dùng làm HTTP header."""
#     return (cookie or "").strip().replace("\r", "").replace("\n", "")


# def seller_id_tu_cookie(cookie):
#     m = re.search(r"oec_seller_id_unified_seller_env=(\d+)", cookie)
#     if not m:
#         m = re.search(r"(?:^|;\s*)SHOP_ID=(\d+)", cookie)
#     return m.group(1) if m else None


# class CookieState:
#     """Giữ cookie gốc để inject lại mỗi lần dựng browser mới."""

#     def __init__(self, cookie, seller_id, source_kind, path=None):
#         self.cookie = cookie
#         self.seller_id = seller_id
#         self.source_kind = source_kind
#         self.path = path

#     def reload_if_changed(self):
#         try:
#             if self.source_kind == "env":
#                 candidate = chuan_hoa_cookie(os.environ.get(ENV_COOKIE))
#             else:
#                 with open(self.path, encoding="utf-8") as cookie_file:
#                     candidate = chuan_hoa_cookie(cookie_file.read())
#         except OSError as error:
#             log(f"    chưa nạp được cookie mới: {error}")
#             return False

#         if not candidate or candidate == self.cookie:
#             return False
#         seller_id = seller_id_tu_cookie(candidate)
#         if seller_id != self.seller_id:
#             log("    bỏ qua cookie mới vì seller_id không khớp shop đang crawl")
#             return False
#         self.cookie = candidate
#         return True


# def doc_cookie(path):
#     """Ưu tiên biến môi trường TIKTOK_COOKIE (secret trên GitHub Actions), không có
#     thì đọc file. Nhờ vậy trên Actions chỉ cần thay secret, không phải ghi file."""
#     ck = (os.environ.get(ENV_COOKIE) or "").strip()
#     nguon = f"biến môi trường {ENV_COOKIE}"
#     source_kind = "env"
#     if not ck:
#         if not os.path.exists(path):
#             sys.exit(f"[COOKIE] không có biến môi trường {ENV_COOKIE}, cũng không thấy file "
#                      f"{path}.\nMở Seller Center -> F12 -> Network -> copy giá trị header "
#                      f"cookie của một request bất kỳ, rồi đặt vào {ENV_COOKIE} hoặc {path}.")
#         with open(path, encoding="utf-8") as cookie_file:
#             ck = cookie_file.read()
#         nguon = f"file {path}"
#         source_kind = "file"
#     if not ck:
#         sys.exit(f"[COOKIE] {nguon} rỗng.")
#     ck = chuan_hoa_cookie(ck)
#     log(f"cookie đọc từ {nguon} ({len(ck)} ký tự)")
#     seller_id = seller_id_tu_cookie(ck)
#     if not seller_id:
#         sys.exit("[COOKIE] không đọc được seller_id (oec_seller_id_unified_seller_env) từ cookie.")
#     return CookieState(ck, seller_id, source_kind, path)


# def common_params(seller_id):
#     return {
#         "locale": "vi-VN", "language": "vi-VN",
#         "oec_seller_id": seller_id, "seller_id": seller_id,
#         "aid": "4068", "app_name": "i18n_ecom_shop",
#         "device_platform": "web", "cookie_enabled": "true",
#     }


# # ---------------------------------------------------------------- phân loại lỗi
# class BiChan(RuntimeError):
#     """Tầng 2 — cần đập browser dựng lại tại đúng cursor (CAPTCHA, renderer treo...)."""


# class TrucTracTamThoi(RuntimeError):
#     """Tầng 3 — thử lại tại chỗ, KHÔNG đập browser (code lạ, HTTP 5xx, fetch lỗi)."""


# def co_dau_hieu_chan(text):
#     t = (text or "").lower()
#     return any(dau in t for dau in DAU_HIEU_CHAN)


# FETCH_IN_PAGE = """
# async ({url, body, timeoutMs}) => {
#     const controller = new AbortController();
#     const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
#     try {
#         const response = await fetch(url, {
#             method: "POST",
#             headers: {
#                 "content-type": "application/json",
#                 "accept": "application/json, text/plain, */*",
#                 "x-tt-oec-region": "VN"
#             },
#             body: JSON.stringify(body),
#             credentials: "include",
#             signal: controller.signal
#         });
#         const responseText = await response.text();
#         let data;
#         try {
#             data = JSON.parse(responseText);
#         } catch (error) {
#             return {
#                 status: response.status,
#                 error: `response không phải JSON: ${responseText.slice(0, 500)}`
#             };
#         }
#         return {status: response.status, data};
#     } catch (error) {
#         return {error: error instanceof Error ? error.message : String(error)};
#     } finally {
#         clearTimeout(timeoutId);
#     }
# }
# """


# class TikTokBrowserClient:
#     """Gọi API trong page, phát hiện CAPTCHA và thay toàn bộ Chromium khi bị chặn.

#     Không giải CAPTCHA. Bị chặn thì bỏ browser đó đi, dựng cái sạch, quay lại đúng
#     search_cursor đang dở.
#     """

#     def __init__(self, playwright, cookie_state, browser_restarts=BROWSER_RESTARTS,
#                  browser_restart_wait=BROWSER_RESTART_WAIT_SECONDS,
#                  deadline_phut=DEADLINE_PHUT_MAC_DINH, landing=None, headless=True,
#                  eval_timeout=EVAL_TIMEOUT_S, canary_timeout=CANARY_TIMEOUT_S,
#                  max_restart_tong=MAX_RESTART_TONG):
#         self.playwright = playwright
#         self.cookie_state = cookie_state
#         self.browser_restarts = browser_restarts
#         self.browser_restart_wait = browser_restart_wait
#         self.landing = landing or (BASE + LANDING_MAC_DINH)
#         self.headless = headless
#         self.eval_timeout = eval_timeout
#         self.canary_timeout = canary_timeout
#         self.max_restart_tong = max_restart_tong
#         self.deadline_phut = deadline_phut
#         self.deadline = (time.monotonic() + deadline_phut * 60) if deadline_phut else None

#         self.restart_count = 0      # chuỗi restart LIÊN TIẾP
#         self.tong_restart = 0       # cộng dồn cả run
#         self.so_lan_captcha = 0
#         self.so_lan_treo = 0
#         self.browser_number = 0
#         # Đếm để đo HẠN MỨC CẢ RUN. Hai run ngày 10/09/2026 đều bị chặn ở đúng request
#         # thứ 79 (một run 78 trang LIVE liên tiếp, một run rải qua 3 khối × 3 kênh) sau
#         # ~5,5 phút. Số request và thời gian lẫn nhau vì nhịp đều 4,2s/request, nên cứ
#         # log cả hai — log run sau sẽ tự phân định hạn mức là theo request hay theo giờ.
#         self.so_request_ok = 0
#         self.bat_dau = time.monotonic()

#         self.browser = None
#         self.context = None
#         self.page = None

#     def tom_tat_han_muc(self):
#         giay = time.monotonic() - self.bat_dau
#         return (f"{self.so_request_ok} request thành công trong "
#                 f"{int(giay // 60)}m{int(giay % 60):02d}s")

#     # ------------------------------------------------------------ tiện ích
#     def _kiem_deadline(self):
#         if self.deadline is not None and time.monotonic() > self.deadline:
#             sys.exit(f"[DEADLINE] Quá {self.deadline_phut} phút. DỪNG, KHÔNG đụng BigQuery. "
#                      f"(restart {self.tong_restart} lần, gặp CAPTCHA {self.so_lan_captcha} lần, "
#                      f"renderer treo {self.so_lan_treo} lần.) "
#                      f"Nếu run nào cũng chạm deadline thì giảm --last-days hoặc lấy cookie mới.")

#     async def _han(self, coro, giay, mo_ta):
#         """Bọc timeout Python quanh mọi lời gọi vào renderer. Đây là lớp bản prod thiếu."""
#         try:
#             return await asyncio.wait_for(coro, timeout=giay)
#         except asyncio.TimeoutError:
#             self.so_lan_treo += 1
#             raise BiChan(f"{mo_ta} quá {giay}s không trả về — renderer treo")

#     def _playwright_cookies(self):
#         cookies = []
#         for part in self.cookie_state.cookie.split(";"):
#             part = part.strip()
#             if "=" not in part:
#                 continue
#             name, _, value = part.partition("=")
#             cookies.append({"name": name.strip(), "value": value.strip(), "url": BASE})
#         return cookies

#     # ------------------------------------------------------------ vòng đời browser
#     async def _destroy_browser(self):
#         context, browser = self.context, self.browser
#         self.page = None
#         self.context = None
#         self.browser = None
#         for obj, ten in ((context, "context"), (browser, "browser")):
#             if obj is None:
#                 continue
#             try:
#                 # close() cũng treo được khi renderer đang chiếm main thread.
#                 await asyncio.wait_for(obj.close(), timeout=CLOSE_TIMEOUT_S)
#             except asyncio.TimeoutError:
#                 log(f"    {ten}.close() quá {CLOSE_TIMEOUT_S}s — bỏ lại, dựng cái mới")
#             except Exception:
#                 pass

#     async def _dò_captcha(self):
#         """Phát hiện chặn. KHÔNG thao tác gì lên CAPTCHA — chỉ báo để đi dựng browser mới."""
#         url = (self.page.url or "").lower()
#         if "login" in url or "passport" in url:
#             sys.exit(f"[COOKIE] Browser bị chuyển sang trang đăng nhập: {self.page.url}. "
#                      "Hãy cập nhật cookie rồi chạy lại.")
#         if "verify" in url or "captcha" in url:
#             self.so_lan_captcha += 1
#             raise BiChan(f"URL là trang xác minh: {self.page.url}")

#         # Canary TRƯỚC: nếu renderer đã chết thì query_selector cũng sẽ treo.
#         await self._han(self.page.evaluate("() => 1"), self.canary_timeout, "canary evaluate")

#         el = await self._han(self.page.evaluate(JS_DO_CAPTCHA, CAPTCHA_SELECTORS),
#                              self.canary_timeout, "dò CAPTCHA trong DOM")
#         if el is not None:
#             self.so_lan_captcha += 1
#             raise BiChan(f"overlay CAPTCHA đang HIỆN (không giải, dựng browser sạch) — "
#                          f"<{el['tag']} id={el['id']!r} class={el['cls']!r}> "
#                          f"{el['w']}x{el['h']}")

#     async def _launch_browser(self):
#         self.browser_number += 1
#         log(f"    launch Chromium process #{self.browser_number}")
#         self.browser = await self.playwright.chromium.launch(headless=self.headless)
#         self.context = await self.browser.new_context(
#             viewport={"width": 1440, "height": 960},
#             locale="vi-VN",
#             timezone_id="Asia/Ho_Chi_Minh",
#         )
#         await self.context.add_cookies(self._playwright_cookies())
#         self.page = await self.context.new_page()
#         self.page.set_default_timeout(PAGE_TIMEOUT_MS)
#         await self._han(
#             self.page.goto(self.landing, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS),
#             PAGE_TIMEOUT_MS / 1000 + 15, "page.goto")
#         await asyncio.sleep(BROWSER_SETTLE_MS / 1000)
#         await self._dò_captcha()
#         log(f"    Chromium #{self.browser_number} sẵn sàng: {self.page.url}")

#     async def _restart_browser(self, mo_ta_trang, error):
#         await self._destroy_browser()
#         if self.restart_count >= self.browser_restarts:
#             sys.exit(
#                 f"[BROWSER] Đã đập và dựng lại Chromium {self.browser_restarts} lần liên "
#                 f"tiếp tại {mo_ta_trang} nhưng vẫn lỗi: {error}.\n"
#                 f"  Đã dùng: {self.tom_tat_han_muc()} (CAPTCHA {self.so_lan_captcha} lần, "
#                 f"renderer treo {self.so_lan_treo} lần).\n"
#                 f"  Cursor chưa tăng và BigQuery CHƯA bị thay đổi.\n"
#                 f"  Có hai nguyên nhân, phân biệt bằng con số 'đã dùng' ở trên:\n"
#                 f"  (1) HẠN MỨC CẢ RUN — nếu số request quanh {HAN_MUC_REQUEST_UOC} hoặc "
#                 f"thời gian quanh 5,5 phút. Hai run ngày 10/09/2026 đều dừng ở đúng request "
#                 f"thứ 79 dù cấu trúc khác hẳn nhau. Cookie KHÔNG hỏng; cửa sổ đang kéo chỉ "
#                 f"đơn giản là cần nhiều request hơn mức TikTok cho. Cách sửa là GIẢM SỐ "
#                 f"REQUEST chứ không phải thử lại: tăng --page-size (100 -> 200/500) hoặc "
#                 f"giảm --last-days. Với page-size 100 thì cửa sổ tối đa chỉ khoảng 17 ngày.\n"
#                 f"  (2) COOKIE BỊ RISK-CONTROL — nếu bị chặn ngay từ những request đầu, hoặc "
#                 f"số lần CAPTCHA cao. Lúc đó cần đăng nhập tay lấy cookie mới."
#             )
#         if self.tong_restart >= self.max_restart_tong:
#             sys.exit(
#                 f"[BROWSER] Chạm trần {self.max_restart_tong} lần restart cộng dồn cả run. "
#                 f"DỪNG, không đụng BigQuery. TikTok đang chặn liên tục — lấy cookie mới."
#             )
#         self.restart_count += 1
#         self.tong_restart += 1
#         log(f"  ! browser lỗi tại {mo_ta_trang}: {error}")
#         log(f"    đập toàn bộ browser; giữ checkpoint cursor của page hiện tại "
#             f"({self.restart_count}/{self.browser_restarts}, cộng dồn "
#             f"{self.tong_restart}/{self.max_restart_tong})")
#         if self.browser_restart_wait:
#             await asyncio.sleep(self.browser_restart_wait)
#         if self.cookie_state.reload_if_changed():
#             log("    đã nạp cookie mới trước khi dựng Chromium")

#     async def _ensure_browser(self, mo_ta_trang):
#         while self.page is None:
#             self._kiem_deadline()
#             try:
#                 await self._launch_browser()
#             except SystemExit:
#                 await self._destroy_browser()
#                 raise
#             except Exception as error:
#                 await self._restart_browser(mo_ta_trang, error)

#     async def close(self):
#         await self._destroy_browser()

#     # ------------------------------------------------------------ gọi API
#     async def _goi_mot_lan(self, url, body):
#         result = await self._han(
#             self.page.evaluate(FETCH_IN_PAGE,
#                                {"url": url, "body": body, "timeoutMs": JS_FETCH_TIMEOUT_MS}),
#             self.eval_timeout, "page.evaluate")

#         if result.get("error"):
#             err = str(result["error"])
#             # TikTok trả thẳng trang HTML xác minh thay vì JSON -> đây là bị chặn.
#             if co_dau_hieu_chan(err):
#                 self.so_lan_captcha += 1
#                 raise BiChan(f"phản hồi có dấu hiệu chặn: {err[:200]}")
#             raise TrucTracTamThoi(f"JS fetch lỗi: {err[:200]}")

#         status = result.get("status")
#         if status in (401, 403):
#             sys.exit(f"[COOKIE] Browser nhận HTTP {status} — cookie hết hạn hoặc bị "
#                      "risk-control chặn. Lấy cookie mới rồi chạy lại. "
#                      "(Dựng lại browser cũng không cứu được, nên dừng luôn.)")
#         if status != 200:
#             if isinstance(status, int) and 500 <= status < 600:
#                 raise TrucTracTamThoi(f"HTTP {status}")
#             raise BiChan(f"HTTP status {status}")

#         js = result.get("data") or {}
#         code = str(js.get("code"))
#         if code == "0":
#             return js
#         if code in ("98001002", "98001008"):
#             sys.exit(f"[COOKIE] TikTok trả code {code}: {js.get('message')}. "
#                      "Cookie đã hết hạn — lấy cookie mới rồi chạy lại.")
#         if code == "10000":
#             # TẦNG 3, không phải tầng 2 — dù thông điệp nghe đúng như bị chặn.
#             # Bằng chứng 10/09/2026, trang 79: dựng lại Chromium 3 lần, cả 3 lần bị chặn
#             # LẠI TỨC THÌ (launch 05:30:07 -> lỗi 05:30:07, rồi 05:30:17 -> 05:30:17).
#             # Trạng thái nằm ở phía TikTok, không ở browser, nên đập browser chỉ đốt hết
#             # ngân sách restart trong 20 giây rồi chết. Ngày 31/08 cùng đúng trang 79
#             # nhưng lỗi rơi vào tầng 3 và được THỬ LẠI TẠI CHỖ thì đi qua được.
#             # Vẫn đếm vào so_lan_captcha để thông điệp chẩn đoán cuối run còn ý nghĩa.
#             self.so_lan_captcha += 1
#             raise TrucTracTamThoi(js.get("message") or "TikTok yêu cầu xác minh browser")
#         # Code lạ (vd 21008301 "Lỗi hệ thống") là trục trặc phía TikTok, không phải bị
#         # chặn. Bản prod đập nguyên Chromium ở đây — đắt và đúng lúc dễ dính CAPTCHA nhất.
#         raise TrucTracTamThoi(f"API code={code}: {js.get('message')}")

#     async def post(self, url, body, mo_ta_trang):
#         lan_thu = 0
#         while True:
#             self._kiem_deadline()
#             await self._ensure_browser(mo_ta_trang)
#             try:
#                 js = await self._goi_mot_lan(url, body)
#                 self.so_request_ok += 1
#                 # Ngân sách restart là cho một CHUỖI lỗi liên tiếp, không phải cho cả run:
#                 # backfill vài trăm trang dính 3 lỗi vặt cách xa nhau vẫn phải chạy tiếp.
#                 self.restart_count = 0
#                 return js
#             except SystemExit:
#                 raise
#             except TrucTracTamThoi as error:
#                 lan_thu += 1
#                 if lan_thu > API_RETRY_TAI_CHO:
#                     await self._restart_browser(
#                         mo_ta_trang, f"{API_RETRY_TAI_CHO} lần trục trặc liên tiếp: {error}")
#                     lan_thu = 0
#                     continue
#                 cho = API_RETRY_SLEEP[min(lan_thu - 1, len(API_RETRY_SLEEP) - 1)]
#                 log(f"    {mo_ta_trang}: {error} — thử lại TẠI CHỖ sau {cho}s "
#                     f"({lan_thu}/{API_RETRY_TAI_CHO}, không đập browser)")
#                 await asyncio.sleep(cho)
#             except BiChan as error:
#                 await self._restart_browser(mo_ta_trang, error)
#                 lan_thu = 0
#             except Exception as error:
#                 await self._restart_browser(mo_ta_trang, error)
#                 lan_thu = 0


# # ---------------------------------------------------------------- kéo dữ liệu
# async def keo_mot_kenh(client, seller_id, sale_source, t0, t1, kiem_tra_total=True, nhan=""):
#     """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh.

#     `nhan` chỉ để log: có nhiều khối ngày nên phải biết trang 12 là trang 12 của khối nào.
#     """
#     url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
#     cond = {
#         "order_source": {"value": ["1"]},
#         "time_order_created": {"value": [str(t0), str(t1)]},
#         "sale_source": {"value": [sale_source]},
#     }
#     out, cursor, pages, total = [], "", 0, None
#     so_don, bo_qua, lech_regex, con_tiep = 0, 0, 0, False
#     while pages < MAX_PAGES:
#         body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
#                 "search_cursor": cursor, "search_condition": {"condition_list": cond}}
#         js = await client.post(url, body,
#                                f"{nhan}sale_source={sale_source}, trang {pages + 1}")
#         d = js.get("data") or {}
#         if total is None:                      # lấy ở trang đầu: đơn mới về giữa chừng
#             total = d.get("total_count")       # không được làm mốc kiểm tra nhảy số
#         orders = d.get("main_orders") or []
#         if not orders:
#             break
#         so_don += len(orders)
#         for o in orders:
#             try:
#                 oid = int(o.get("main_order_id"))
#                 t = int((o.get("trade_order_module") or {}).get("create_time"))
#             except (TypeError, ValueError):
#                 bo_qua += 1                    # thiếu field -> bỏ đơn, đừng để crash cả run
#                 continue
#             seen = set()
#             for sku in (o.get("sku_module") or []):
#                 for it in ((sku.get("creator_info_name") or {}).get("items") or []):
#                     msg = it.get("message_content") or ""
#                     m = RE_NGUOI_NHAN.search(msg)
#                     if m:
#                         seen.add(m.group(1))
#                     elif RE_LONG.search(msg):
#                         lech_regex += 1
#             for c in seen:
#                 out.append((oid, t, c))
#         cursor = d.get("search_next_cursor") or ""
#         pages += 1
#         # Bản prod im lặng suốt lúc crawl nên nhìn log không phân biệt được "treo" với
#         # "chậm". Heartbeat để lần sau đọc log là biết ngay.
#         if pages % HEARTBEAT_MOI == 0:
#             # In luôn hạn mức đã dùng: đây là con số cần để biết còn bao xa tới tường.
#             log(f"    ... {nhan}sale_source={sale_source}: {pages} trang, {so_don} đơn "
#                 f"| cả run: {client.tom_tat_han_muc()}")
#         con_tiep = bool(d.get("search_next_has_more")) and bool(cursor)
#         if not con_tiep:
#             break

#     log(f"  {nhan}sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
#         f"total_count={total}, {pages} trang, {so_don} đơn, {len(out)} cặp đơn-creator")
#     if bo_qua:
#         log(f"    ! bỏ {bo_qua} đơn thiếu main_order_id hoặc create_time")
#     if lech_regex:
#         log(f"    ({lech_regex} item có 'hoa hồng:' nhưng không phải 'Người nhận hoa hồng:' "
#             f"— đã bỏ qua)")

#     # Hai guard dưới đây phải là sys.exit chứ không phải log: nap_bq sẽ DELETE nguyên
#     # khoảng ngày rồi INSERT lại, nên kéo thiếu mà chạy tiếp là mất dữ liệu thật.
#     if con_tiep:
#         sys.exit(f"[PHÂN TRANG] {nhan}sale_source={sale_source} chạm trần "
#                  f"MAX_PAGES={MAX_PAGES} mà TikTok vẫn báo còn dữ liệu. DỪNG, không đụng "
#                  f"BigQuery. Hãy giảm --chunk-ngay (đang cắt theo khối ngày) chứ đừng "
#                  f"tăng MAX_PAGES — cursor TikTok vỡ quanh trang 79.")
#     try:
#         total_int = int(total)
#     except (TypeError, ValueError):
#         total_int = None
#     if kiem_tra_total and total_int is not None and so_don < total_int:
#         sys.exit(f"[PHÂN TRANG] {nhan}sale_source={sale_source}: TikTok báo "
#                  f"total_count={total_int} nhưng chỉ lấy được {so_don} đơn. DỪNG, không "
#                  f"đụng BigQuery. Nếu chắc chắn total_count của TikTok sai thì chạy lại "
#                  f"với --skip-total-check.")
#     return out


# def chia_khoi_ngay(d0, d1, so_ngay):
#     """Cắt [d0, d1] thành các khối liền nhau, mỗi khối tối đa `so_ngay` ngày.

#     `so_ngay <= 0` nghĩa là KHÔNG cắt — trả về một khối duy nhất. Đây là mặc định; lý do
#     xem CHUNK_NGAY_MAC_DINH.

#     Các khối rời nhau và phủ kín, nên gộp lại đúng bằng khoảng gốc — không hở ngày nào,
#     không trùng ngày nào.
#     """
#     if so_ngay <= 0:
#         return [(d0, d1)]
#     khoi, dau = [], d0
#     while dau <= d1:
#         cuoi = min(dau + datetime.timedelta(days=so_ngay - 1), d1)
#         khoi.append((dau, cuoi))
#         dau = cuoi + datetime.timedelta(days=1)
#     return khoi


# async def keo(client, seller_id, khoi_ngay, kiem_tra_total=True):
#     """Kéo lần lượt từng khối ngày × từng kênh, gộp và khử trùng vào một dict.

#     Khoá khử trùng là (main_order_id, creator_username) nên dù các khối có chồng nhau
#     hay một đơn về ở hai kênh thì cũng không nhân đôi dòng.
#     """
#     rows, la = {}, set()
#     tong_khoi = len(khoi_ngay)
#     for i, (kd0, kd1) in enumerate(khoi_ngay, 1):
#         t0 = int(datetime.datetime.combine(kd0, datetime.time(0, 0, 0), TZ).timestamp())
#         t1 = int(datetime.datetime.combine(kd1, datetime.time(23, 59, 59), TZ).timestamp())
#         nhan = f"[khối {i}/{tong_khoi} {kd0}..{kd1}] "
#         log(f"khối {i}/{tong_khoi}: {kd0} -> {kd1} (giờ VN)")
#         for ss in ("1", "2", "3"):
#             ket = await keo_mot_kenh(client, seller_id, ss, t0, t1, kiem_tra_total, nhan)
#             for oid, t, cu in ket:
#                 if cu not in CREATORS:
#                     la.add(cu)
#                     continue
#                 key = (oid, cu)
#                 pos = SALE_SOURCE_TO_POSITION[ss]
#                 if key in rows:
#                     if rows[key]["promotion_position_type"] != pos:
#                         log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
#                             f"({rows[key]['promotion_position_type']} và {pos}) "
#                             f"— giữ kênh đầu")
#                     continue
#                 rows[key] = {
#                     "main_order_id": oid,
#                     "creator_nickname": CREATORS[cu],
#                     "creator_username": cu,
#                     "promotion_position_type": pos,
#                     "create_time": datetime.datetime.fromtimestamp(
#                         t, datetime.timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
#                     "cos_ratio": None,
#                     "estimated_cos_fee": None,
#                     "shop_ads_commission_ratio": None,
#                     "estimated_shop_ads_commission": None,
#                     "id_shop": seller_id,
#                 }
#     if la:
#         log(f"  (bỏ qua {len(la)} creator ngoài danh sách CREATORS, vd: {sorted(la)[:5]})")
#     return sorted(rows.values(), key=lambda r: r["create_time"])


# # ---------------------------------------------------------------- BigQuery
# def bq_client():
#     """Giống các job khác trong repo: ưu tiên key file, rồi JSON inline trong biến
#     môi trường (secret GOOGLE_SERVICE_ACCOUNT_JSON), cuối cùng mới dùng ADC."""
#     from google.cloud import bigquery
#     key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
#     key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

#     if key_path and os.path.exists(key_path):
#         from google.oauth2 import service_account
#         cred = service_account.Credentials.from_service_account_file(key_path)
#         log(f"  BigQuery: key file {key_path}")
#         return bigquery.Client(project=BQ_PROJECT, credentials=cred)
#     if key_json:
#         from google.oauth2 import service_account
#         try:
#             info = json.loads(key_json)
#         except json.JSONDecodeError as e:
#             sys.exit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
#         cred = service_account.Credentials.from_service_account_info(info)
#         log(f"  BigQuery: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email','?')})")
#         return bigquery.Client(project=BQ_PROJECT, credentials=cred)
#     log("  BigQuery: credentials mặc định của môi trường (ADC)")
#     return bigquery.Client(project=BQ_PROJECT)


# def _dml_theo_cau_lenh(client, script_job):
#     """Số dòng DELETE/INSERT của từng câu trong script. Thiếu quyền jobs.list thì thôi."""
#     try:
#         ra = {}
#         for child in client.list_jobs(parent_job=script_job.job_id):
#             if child.num_dml_affected_rows is not None:
#                 ra[child.statement_type] = child.num_dml_affected_rows
#         return ra
#     except Exception:
#         return {}


# def nap_bq(rows, seller_id, utc0, utc1):
#     """Nạp qua bảng staging rồi DELETE + INSERT trong MỘT transaction."""
#     from google.cloud import bigquery
#     client = bq_client()
#     table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
#     bang_dich = client.get_table(table_id)

#     cot_bang = {f.name for f in bang_dich.schema}
#     thieu = [c for c in rows[0] if c not in cot_bang]
#     if thieu:
#         sys.exit(f"[BQ] các cột {thieu} không tồn tại trong {table_id}.")
#     cot = ", ".join(f"`{c}`" for c in rows[0])

#     stg_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}__stg_{uuid.uuid4().hex[:12]}"
#     job = client.load_table_from_json(
#         rows, stg_id,
#         job_config=bigquery.LoadJobConfig(
#             schema=bang_dich.schema, write_disposition="WRITE_TRUNCATE"))
#     job.result()
#     log(f"  staging {stg_id.split('.')[-1]}: {len(rows)} dòng")

#     cfg = bigquery.QueryJobConfig(query_parameters=[
#         bigquery.ScalarQueryParameter("shop", "STRING", seller_id),
#         bigquery.ArrayQueryParameter("creators", "STRING", list(CREATORS)),
#         bigquery.ScalarQueryParameter("t0", "DATETIME", utc0),
#         bigquery.ScalarQueryParameter("t1", "DATETIME", utc1),
#     ])
#     sql = f"""
#         BEGIN TRANSACTION;
#         DELETE FROM `{table_id}`
#         WHERE id_shop = @shop
#           AND creator_username IN UNNEST(@creators)
#           AND create_time >= @t0 AND create_time <= @t1;
#         INSERT INTO `{table_id}` ({cot}) SELECT {cot} FROM `{stg_id}`;
#         COMMIT TRANSACTION;
#     """
#     try:
#         job = client.query(sql, job_config=cfg)
#         job.result()
#     finally:
#         client.delete_table(stg_id, not_found_ok=True)
#     dml = _dml_theo_cau_lenh(client, job)
#     log(f"  DELETE {dml.get('DELETE', '?')} dòng cũ + INSERT {dml.get('INSERT', len(rows))} "
#         f"dòng mới, cùng 1 transaction -> {table_id}")


# # ---------------------------------------------------------------- main
# def im_lang_loi_thua(loop):
#     """Sau khi _han() timeout, task page.evaluate bị huỷ nhưng future nội bộ của
#     Playwright vẫn kết thúc bằng TargetClosedError, và asyncio in nguyên một traceback
#     vô nghĩa ra log. Chính log rác kiểu này làm sự cố treo khó chẩn đoán, nên chặn."""
#     goc = loop.get_exception_handler()

#     def handler(lp, context):
#         exc = context.get("exception")
#         if exc is not None and type(exc).__name__ in ("TargetClosedError", "CancelledError"):
#             return
#         if goc is not None:
#             goc(lp, context)
#         else:
#             lp.default_exception_handler(context)

#     loop.set_exception_handler(handler)


# async def chay_crawl(cookie_state, seller_id, khoi_ngay, a):
#     from playwright.async_api import async_playwright
#     im_lang_loi_thua(asyncio.get_running_loop())
#     async with async_playwright() as playwright:
#         client = TikTokBrowserClient(
#             playwright,
#             cookie_state,
#             browser_restarts=a.browser_restarts,
#             browser_restart_wait=a.browser_restart_wait,
#             deadline_phut=a.deadline_phut,
#             landing=(BASE + a.landing),
#             headless=not a.headful,
#         )
#         try:
#             rows = await keo(client, seller_id, khoi_ngay, not a.skip_total_check)
#             log(f"thống kê chặn: CAPTCHA {client.so_lan_captcha} lần, renderer treo "
#                 f"{client.so_lan_treo} lần, restart browser {client.tong_restart} lần")
#             return rows
#         finally:
#             await client.close()


# def main():
#     # keo_mot_kenh đọc PAGE_SIZE như biến module (giống cách test_offline.py vá các hằng
#     # số khác), nên --page-size ghi thẳng vào đây. Khai báo phải nằm trước mọi lần đọc
#     # tên này trong hàm, kể cả trong default= của argparse.
#     global PAGE_SIZE
#     ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery (BẢN LAB)")
#     g = ap.add_mutually_exclusive_group()
#     g.add_argument("--date", help="một ngày YYYY-MM-DD (giờ VN)")
#     g.add_argument("--last-days", type=int, default=None,
#                    help=f"N ngày gần nhất tính cả hôm nay (mặc định {LAST_DAYS_MAC_DINH})")
#     ap.add_argument("--from", dest="d_from", help="từ ngày YYYY-MM-DD")
#     ap.add_argument("--to", dest="d_to", help="đến ngày YYYY-MM-DD")
#     ap.add_argument("--cookie", default=COOKIE,
#                     help=f"file cookie (mặc định {COOKIE}); bị bỏ qua nếu đã có "
#                          f"biến môi trường {ENV_COOKIE}")
#     ap.add_argument("--csv", help="ghi thêm ra file CSV để kiểm tra")
#     ap.add_argument("--no-bq", action="store_true", help="không đụng BigQuery")
#     ap.add_argument("--min-rows", type=int, default=1,
#                     help="ít hơn số này thì DỪNG, không xoá gì (chống wipe)")
#     ap.add_argument("--skip-total-check", action="store_true",
#                     help="bỏ đối chiếu số đơn lấy được với total_count của TikTok")
#     ap.add_argument("--browser-restarts", type=int, default=BROWSER_RESTARTS,
#                     help=f"số lần đập/dựng lại Chromium liên tiếp (mặc định {BROWSER_RESTARTS})")
#     ap.add_argument("--browser-restart-wait", type=int, default=BROWSER_RESTART_WAIT_SECONDS,
#                     help=f"số giây chờ trước khi launch Chromium mới "
#                          f"(mặc định {BROWSER_RESTART_WAIT_SECONDS})")
#     ap.add_argument("--deadline-phut", type=int, default=DEADLINE_PHUT_MAC_DINH,
#                     help=f"trần thời gian cả run, quá thì dừng và KHÔNG đụng BigQuery "
#                          f"(mặc định {DEADLINE_PHUT_MAC_DINH}; 0 = tắt)")
#     ap.add_argument("--landing", default=LANDING_MAC_DINH,
#                     help=f"đường dẫn trang landing để inject cookie (mặc định "
#                          f"{LANDING_MAC_DINH}). Chỉ cần cùng origin là cookie vẫn gửi. "
#                          f"Đổi sang '/order' là mở lại SPA — chỉ dùng khi cần debug.")
#     ap.add_argument("--headful", action="store_true",
#                     help="hiện cửa sổ Chromium (xem tận mắt CAPTCHA lúc debug ở máy)")
#     ap.add_argument("--chunk-ngay", type=int, default=CHUNK_NGAY_MAC_DINH,
#                     help=f"cắt khoảng ngày thành từng khối tối đa N ngày (mặc định "
#                          f"{CHUNK_NGAY_MAC_DINH} = không cắt). Bật lên là TỐN THÊM request "
#                          f"vì mỗi khối phải gọi lại trang đầu cho cả 3 kênh.")
#     ap.add_argument("--page-size", type=int, default=PAGE_SIZE,
#                     help=f"số đơn mỗi request (mặc định {PAGE_SIZE}). ĐÂY LÀ CỜ QUAN TRỌNG "
#                          f"NHẤT khi gặp [HẠN MỨC]: run bị chặn sau ~78 request, nên tăng "
#                          f"page-size là cách duy nhất kéo được cửa sổ rộng. 30 ngày cần "
#                          f"~135 request ở 100, nhưng chỉ ~69 ở 200 và ~30 ở 500. "
#                          f"CHƯA KIỂM CHỨNG TikTok có nhận >100 hay không — thử bằng "
#                          f"--date <1 ngày> --no-bq rồi xem số trang trong log.")
#     a = ap.parse_args()

#     if a.browser_restarts < 0 or a.browser_restart_wait < 0 or a.deadline_phut < 0:
#         sys.exit("--browser-restarts, --browser-restart-wait, --deadline-phut không được âm.")
#     if a.chunk_ngay < 0:
#         sys.exit("--chunk-ngay không được âm (0 = không cắt).")
#     if a.page_size < 1:
#         sys.exit("--page-size phải >= 1.")
#     PAGE_SIZE = a.page_size

#     if a.d_from or a.d_to:
#         if not (a.d_from and a.d_to):
#             sys.exit("--from và --to phải đi cùng nhau.")
#         if a.date or a.last_days is not None:
#             sys.exit("--from/--to không dùng chung với --date hoặc --last-days.")
#         d0 = datetime.date.fromisoformat(a.d_from)
#         d1 = datetime.date.fromisoformat(a.d_to)
#     elif a.date:
#         d0 = d1 = datetime.date.fromisoformat(a.date)
#     else:
#         d1 = datetime.datetime.now(TZ).date()
#         so_ngay = LAST_DAYS_MAC_DINH if a.last_days is None else a.last_days
#         d0 = d1 - datetime.timedelta(days=so_ngay - 1)
#     if d0 > d1:
#         sys.exit("Khoảng ngày không hợp lệ.")

#     cookie_state = doc_cookie(a.cookie)
#     seller_id = cookie_state.seller_id
#     start = datetime.datetime.combine(d0, datetime.time(0, 0, 0), TZ)
#     end = datetime.datetime.combine(d1, datetime.time(23, 59, 59), TZ)
#     # Khoảng XOÁ vẫn là trọn cửa sổ, KHÔNG cắt theo khối — chia khối chỉ để gọi API cho
#     # cursor đỡ sâu, còn DELETE+INSERT vẫn làm một lần cho cả khoảng như trước.
#     utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
#     utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

#     khoi_ngay = chia_khoi_ngay(d0, d1, a.chunk_ngay)

#     log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
#     log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")
#     so_ngay_keo = (d1 - d0).days + 1
#     so_don_uoc = DON_MOI_NGAY_UOC * so_ngay_keo
#     uoc_request = uoc_so_request(so_ngay_keo, PAGE_SIZE)
#     mo_ta_khoi = "" if a.chunk_ngay <= 0 else f" (<= {a.chunk_ngay} ngày mỗi khối)"
#     log(f"page_size={PAGE_SIZE} | {len(khoi_ngay)} khối{mo_ta_khoi} | "
#         f"ước ~{uoc_request} request cho ~{so_don_uoc:,} đơn")
#     if uoc_request > HAN_MUC_REQUEST_UOC:
#         log(f"  ! CẢNH BÁO: ước {uoc_request} request, vượt hạn mức "
#             f"~{HAN_MUC_REQUEST_UOC} quan sát được ngày 10/09/2026. Run này khả năng cao "
#             f"bị chặn giữa đường và KHÔNG ghi được gì. Tăng --page-size (nếu TikTok nhận) "
#             f"hoặc giảm --last-days.")

#     try:
#         import playwright  # noqa: F401
#     except ImportError:
#         sys.exit("[PLAYWRIGHT] Chưa cài Playwright. Chạy: pip install playwright && "
#                  "python -m playwright install chromium")

#     rows = asyncio.run(chay_crawl(cookie_state, seller_id, khoi_ngay, a))

#     log(f"TỔNG: {len(rows)} dòng, {len({r['main_order_id'] for r in rows})} đơn")
#     for cu in CREATORS:
#         sub = [r for r in rows if r["creator_username"] == cu]
#         if sub:
#             b = {}
#             for r in sub:
#                 b[r["promotion_position_type"]] = b.get(r["promotion_position_type"], 0) + 1
#             log(f"  {cu:<20} {len(sub):>5}  (1 Thẻ SP={b.get(1,0)}, 2 Video={b.get(2,0)}, 3 LIVE={b.get(3,0)})")

#     if a.csv:
#         import csv as _csv
#         with open(a.csv, "w", newline="", encoding="utf-8-sig") as f:
#             w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
#             if rows:
#                 w.writeheader()
#                 w.writerows(rows)
#         log(f"đã ghi {a.csv}")

#     if a.no_bq:
#         log("--no-bq: dừng, không đụng BigQuery.")
#         return
#     if len(rows) < a.min_rows:
#         sys.exit(f"[GUARD] chỉ có {len(rows)} dòng (< --min-rows {a.min_rows}). "
#                  f"DỪNG, không xoá gì. Nghi cookie hỏng hoặc TikTok đổi API.")
#     nap_bq(rows, seller_id, utc0, utc1)
#     log("XONG.")


# if __name__ == "__main__":
#     main()














#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TikTok Seller — NGƯỜI NHẬN HOA HỒNG theo đơn, nạp BigQuery fact_creator_tiktok.

Vì sao có script này: job affiliate cũ ngừng ghi nhận 3 tài khoản nhà (rhysman.com,
rhysman.shopping, rhysman_channel) từ 29/06/2026, trong khi TikTok VẪN trả dữ liệu.
Script này lấy thẳng từ API danh sách đơn của Seller Center.

NGUỒN DỮ LIỆU (quan trọng, đừng đổi sang file export):
  Người nhận hoa hồng nằm ở  main_orders[].sku_module[].creator_info_name.items[]
  phần tử position=3 -> "Người nhận hoa hồng: <handle>".
  Cột "Creator Handle" trong file export .xlsx KHÔNG phải trường này — nó để trống
  cho toàn bộ đơn Thẻ sản phẩm, thiếu ~27% dữ liệu. Đã kiểm chứng 06/08/2026.

  promotion_position_type lấy bằng cách lọc sale_source (mỗi đơn thuộc đúng 1 kênh,
  đã đối chiếu 501/501 đơn ngày 05/08/2026 khớp với cột Order Channel của file export):
      sale_source 1 = LIVE          -> promotion_position_type 3
      sale_source 2 = Video         -> promotion_position_type 2
      sale_source 3 = Thẻ sản phẩm  -> promotion_position_type 1

XỬ LÝ LỖI (bản sửa 10/09/2026):
  - code 10000, CAPTCHA/verify: DỪNG để người dùng xác minh; không restart để né chặn.
  - HTTP 401/403, redirect login/passport, code 98001002/98001008: dừng kiểm tra phiên.
  - HTTP 429/5xx, lỗi mạng/code khác: retry có giới hạn, tôn trọng Retry-After.
    Hết retry thì dừng; không khuếch đại lỗi API thành restart Chromium.
  - Chỉ renderer/context lỗi kỹ thuật mới được restart tại cursor đang giữ TRONG RAM.
    Script chưa có checkpoint trên đĩa; dừng process là chạy lại từ đầu.
  - Mọi lỗi crawl/kiểm dữ liệu đều xảy ra trước nạp BigQuery.

fetch vẫn chạy ở /robots.txt cùng origin. Cookie cùng origin không chứng minh phiên
đã được TikTok xác minh; "Chromium sẵn sàng" chỉ chứng minh trang đã mở và canary chạy.
Giữ timeout Python quanh evaluate vì API này không có tham số timeout riêng.
--deadline-phut giới hạn phần crawl; dọn browser có timeout riêng, BigQuery chạy sau đó.

VÍ DỤ:
  py -X utf8 keo_creator_IH.py                      # 30 ngày gần nhất
  py -X utf8 keo_creator_IH.py --date 2026-08-05 --no-bq --csv out.csv
  py -X utf8 keo_creator_IH.py --from 2026-06-29 --to 2026-07-31   # backfill
  py -X utf8 keo_creator_IH.py --last-days 3 --cookie cookie_shop2.txt

Idempotent: load vào bảng staging, rồi DELETE (id_shop x creator_username x khoảng
create_time) + INSERT trong MỘT transaction. Chạy lại bao nhiêu lần cũng không nhân đôi,
và load hỏng thì chưa chạy DELETE. Service account cần quyền tạo/xoá/ghi bảng trong
dataset và quyền tạo BigQuery jobs trên project thực thi; xác nhận ở staging.

create_time ghi xuống BigQuery là UTC (không tzinfo) — đúng quy ước sẵn có của bảng,
đã đối chiếu phân bố giờ của dữ liệu cũ ngày 06/08/2026. Đừng đổi sang giờ VN.

Kéo thiếu là hỏng dữ liệu chứ không phải chạy chậm: vì nap_bq xoá cả khoảng ngày rồi ghi
lại, script sẽ DỪNG HẲN (không đụng BigQuery) nếu chạm trần MAX_PAGES mà TikTok còn báo
dữ liệu, hoặc nếu số đơn lấy được ít hơn total_count TikTok tự báo. Chỉ dùng
--skip-total-check khi đã kiểm tay và biết chắc total_count của TikTok sai.

BỘ TEST đi kèm: python -X utf8 -m unittest discover -s . -p "test_*.py" -v
Chạy trong thư mục bản sửa; cài dependencies theo requirements-test.txt.
Test offline không chứng minh phiên TikTok thật hoặc quyền/transaction BigQuery thật.

"""
import argparse, asyncio, datetime, json, os, re, sys, time, urllib.parse, uuid
from email.utils import parsedate_to_datetime

# ---------------------------------------------------------------- cấu hình
# Hằng số cứng, KHÔNG đọc từ biến môi trường. Test offline đổi host bằng cách gán thẳng
# lab.BASE / lab.API (xem test_offline.py), nên không cần cửa hậu qua env — mà cửa hậu đó
# trên Actions chỉ là một đường để set nhầm rồi trỏ script sang host lạ.
BASE = "https://seller-vn.tiktok.com"
API = BASE + "/api/fulfillment/order/list"
COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
TZ = datetime.timezone(datetime.timedelta(hours=7))

BQ_PROJECT = "rhysman-data-warehouse-488306"
BQ_DATASET = "rhysman"
BQ_TABLE = "fact_creator_tiktok"

# Số ngày kéo khi không truyền --date/--from/--to. Đổi ở ĐÂY và ở LAST_DAYS trong
# workflow (cả hai phải khớp, nếu không chạy tay và chạy tự động sẽ ra khác nhau).
# Mỗi run XOÁ rồi GHI LẠI trọn cửa sổ này, nên tăng số là tăng cả thời gian chạy, số
# partition BigQuery bị viết lại, lẫn thời gian phơi nhiễm với lỗi treo.
LAST_DAYS_MAC_DINH = 30

CREATORS = {
    "rhysman.com": "Rhys Man Chính Hãng",
    "rhysman.shopping": "Rhys Man",
    "rhysman_channel": "Rhys Man Chăm Sóc Cơ Thể",
}

SALE_SOURCE_TO_POSITION = {"1": 3, "2": 2, "3": 1}
SALE_SOURCE_TEN = {"1": "LIVE", "2": "Video", "3": "Thẻ sản phẩm"}

RE_NGUOI_NHAN = re.compile(r"người nhận hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
RE_LONG = re.compile(r"hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
PAGE_SIZE = 100
MAX_PAGES = 600

# --- thời gian ---------------------------------------------------------------
# Thứ tự phải là: JS abort < timeout Python < deadline tổng. JS abort bắn trước cho ra
# lỗi sạch (thử lại tại chỗ được); timeout Python là lưới cuối khi renderer chết hẳn và
# JS abort không chạy nổi.
PAGE_TIMEOUT_MS = 90_000
JS_FETCH_TIMEOUT_MS = 60_000
EVAL_TIMEOUT_S = 90.0          # timeout Python độc lập với timer trong JavaScript
CANARY_TIMEOUT_S = 15.0        # renderer còn sống không, hỏi ngay sau khi launch
CLOSE_TIMEOUT_S = 20.0         # browser.close() cũng treo được khi renderer đang spin
BROWSER_SETTLE_MS = 4_000
DEADLINE_PHUT_MAC_DINH = 45

# Trang nhẹ cùng origin; không đảm bảo bootstrap đầy đủ SDK của Seller Center.
# Giữ lựa chọn gốc để không đổi đồng thời luồng bootstrap chưa được test trên TikTok thật.
LANDING_MAC_DINH = "/robots.txt"

# --- ngân sách thử lại -------------------------------------------------------
BROWSER_RESTARTS = 3           # số restart LIÊN TIẾP cho phép (reset sau mỗi lần thành công)
MAX_RESTART_TONG = 12          # trần cộng dồn cả run, chống flap vô tận
BROWSER_RESTART_WAIT_SECONDS = 5
API_RETRY_TAI_CHO = 5          # retry sau lần gọi đầu; hết ngân sách thì dừng
# Chỉ dùng backoff cho lỗi tạm thời; yêu cầu xác minh dừng ngay.
API_RETRY_SLEEP = (3, 10, 30, 60, 120)
HEARTBEAT_MOI = 20             # log tiến độ mỗi N trang

# Chia khối thay đổi phạm vi truy vấn, không làm reset trạng thái xác minh phía server.
# Không suy ra một hạn mức cố định từ số request trong ảnh/log.
CHUNK_NGAY_MAC_DINH = 0

# Overlay CAPTCHA của Seller Center nằm TRÊN CÙNG url /order, không đổi URL — nên bản
# prod (chỉ dò chuỗi trong URL) không bao giờ thấy nó.
CAPTCHA_SELECTORS = ", ".join([
    "#captcha_container",
    "#captcha-verify-page",
    "[class*='captcha']",
    "[id*='captcha']",
    "[class*='secsdk']",
])

# CHỈ khớp selector là KHÔNG ĐỦ. Seller Center LUÔN nhúng sẵn 2 thẻ script
#   <script id="oec-ttweb-captcha-config">  và  <script id="lucifer-captcha-loader-js">
# nên bản dò đầu tiên báo "có CAPTCHA" ở mọi lần mở trang, trong khi API vẫn trả code=0
# (đã kiểm chứng 31/08/2026: total_count=192, không hề bị chặn). Vì vậy phải loại thẻ
# không phải giao diện VÀ bắt buộc element thật sự hiện ra màn hình.
JS_DO_CAPTCHA = """
(sel) => {
    const bo_qua = new Set(['script', 'style', 'link', 'meta', 'template', 'noscript', 'head']);
    for (const el of document.querySelectorAll(sel)) {
        const tag = el.tagName.toLowerCase();
        if (bo_qua.has(tag)) continue;
        const r = el.getBoundingClientRect();
        if (r.width <= 0 || r.height <= 0) continue;
        const st = window.getComputedStyle(el);
        if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') continue;
        return {
            tag: tag,
            id: el.id || '',
            cls: (el.className || '').toString().slice(0, 100),
            w: Math.round(r.width), h: Math.round(r.height)
        };
    }
    return null;
}
"""

# Dấu hiệu bị chặn khi phản hồi không phải JSON (TikTok trả thẳng trang HTML xác minh).
DAU_HIEU_CHAN = ("captcha", "secsdk", "verify", "/passport", "risk_control")


def log(*a):
    print(datetime.datetime.now(TZ).strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- cookie
def chuan_hoa_cookie(cookie):
    """Cookie phải nằm trên một dòng để dùng làm HTTP header."""
    return (cookie or "").strip().replace("\r", "").replace("\n", "")


def seller_id_tu_cookie(cookie):
    m = re.search(r"oec_seller_id_unified_seller_env=(\d+)", cookie)
    if not m:
        m = re.search(r"(?:^|;\s*)SHOP_ID=(\d+)", cookie)
    return m.group(1) if m else None


class CookieState:
    """Giữ cookie gốc để inject lại mỗi lần dựng browser mới."""

    def __init__(self, cookie, seller_id, source_kind, path=None):
        self.cookie = cookie
        self.seller_id = seller_id
        self.source_kind = source_kind
        self.path = path

    def reload_if_changed(self):
        try:
            if self.source_kind == "env":
                candidate = chuan_hoa_cookie(os.environ.get(ENV_COOKIE))
            else:
                with open(self.path, encoding="utf-8") as cookie_file:
                    candidate = chuan_hoa_cookie(cookie_file.read())
        except OSError as error:
            log(f"    chưa nạp được cookie mới: {error}")
            return False

        if not candidate or candidate == self.cookie:
            return False
        seller_id = seller_id_tu_cookie(candidate)
        if seller_id != self.seller_id:
            log("    bỏ qua cookie mới vì seller_id không khớp shop đang crawl")
            return False
        self.cookie = candidate
        return True


def doc_cookie(path):
    """Ưu tiên biến môi trường TIKTOK_COOKIE (secret trên GitHub Actions), không có
    thì đọc file. Nhờ vậy trên Actions chỉ cần thay secret, không phải ghi file."""
    ck = (os.environ.get(ENV_COOKIE) or "").strip()
    nguon = f"biến môi trường {ENV_COOKIE}"
    source_kind = "env"
    if not ck:
        if not os.path.exists(path):
            sys.exit(f"[COOKIE] không có biến môi trường {ENV_COOKIE}, cũng không thấy file "
                     f"{path}.\nMở Seller Center -> F12 -> Network -> copy giá trị header "
                     f"cookie của một request bất kỳ, rồi đặt vào {ENV_COOKIE} hoặc {path}.")
        with open(path, encoding="utf-8") as cookie_file:
            ck = cookie_file.read()
        nguon = f"file {path}"
        source_kind = "file"
    if not ck:
        sys.exit(f"[COOKIE] {nguon} rỗng.")
    ck = chuan_hoa_cookie(ck)
    log(f"cookie đọc từ {nguon} ({len(ck)} ký tự)")
    seller_id = seller_id_tu_cookie(ck)
    if not seller_id:
        sys.exit("[COOKIE] không đọc được seller_id (oec_seller_id_unified_seller_env) từ cookie.")
    return CookieState(ck, seller_id, source_kind, path)


def common_params(seller_id):
    return {
        "locale": "vi-VN", "language": "vi-VN",
        "oec_seller_id": seller_id, "seller_id": seller_id,
        "aid": "4068", "app_name": "i18n_ecom_shop",
        "device_platform": "web", "cookie_enabled": "true",
    }


# ---------------------------------------------------------------- phân loại lỗi
class BiChan(RuntimeError):
    """Renderer/context hỏng; có thể dựng lại browser tại cursor hiện tại."""


class TrucTracTamThoi(RuntimeError):
    """Lỗi API/mạng tạm thời; hết retry thì dừng, không dựng lại browser."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def doc_retry_after(value):
    """Retry-After là số giây nguyên hoặc HTTP-date; sai định dạng thì dùng backoff."""
    value = str(value or "").strip()
    if re.fullmatch(r"[0-9]+", value):
        return int(value)
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=datetime.timezone.utc)
        return max(0.0, (date - datetime.datetime.now(datetime.timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def la_loi_browser(error):
    if isinstance(error, BiChan):
        return True
    from playwright.async_api import Error
    return isinstance(error, Error)


def co_dau_hieu_chan(text):
    t = (text or "").lower()
    return any(dau in t for dau in DAU_HIEU_CHAN)


FETCH_IN_PAGE = """
async ({url, body, timeoutMs}) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "accept": "application/json, text/plain, */*",
                "x-tt-oec-region": "VN"
            },
            body: JSON.stringify(body),
            credentials: "include",
            signal: controller.signal
        });
        const responseText = await response.text();
        const meta = {
            status: response.status,
            response_url: response.url,
            retry_after: response.headers.get('retry-after')
        };
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (error) {
            return {
                ...meta,
                error: `response không phải JSON: ${responseText.slice(0, 500)}`
            };
        }
        return {...meta, data};
    } catch (error) {
        return {error: error instanceof Error ? error.message : String(error)};
    } finally {
        clearTimeout(timeoutId);
    }
}
"""


class TikTokBrowserClient:
    """Dừng khi cần xác minh; chỉ restart khi renderer/context gặp lỗi kỹ thuật."""

    def __init__(self, playwright, cookie_state, browser_restarts=BROWSER_RESTARTS,
                 browser_restart_wait=BROWSER_RESTART_WAIT_SECONDS,
                 deadline_phut=DEADLINE_PHUT_MAC_DINH, landing=None, headless=True,
                 eval_timeout=EVAL_TIMEOUT_S, canary_timeout=CANARY_TIMEOUT_S,
                 max_restart_tong=MAX_RESTART_TONG):
        self.playwright = playwright
        self.cookie_state = cookie_state
        self.browser_restarts = browser_restarts
        self.browser_restart_wait = browser_restart_wait
        self.landing = landing or (BASE + LANDING_MAC_DINH)
        self.headless = headless
        self.eval_timeout = eval_timeout
        self.canary_timeout = canary_timeout
        self.max_restart_tong = max_restart_tong
        self.deadline_phut = deadline_phut
        self.deadline = (time.monotonic() + deadline_phut * 60) if deadline_phut else None

        self.restart_count = 0      # chuỗi restart LIÊN TIẾP
        self.tong_restart = 0       # cộng dồn cả run
        self.so_lan_captcha = 0
        self.so_lan_treo = 0
        self.browser_number = 0
        # Đếm request thành công để chẩn đoán, không dùng làm ngưỡng quota.
        self.so_request_ok = 0
        self.bat_dau = time.monotonic()

        self.browser = None
        self.context = None
        self.page = None

    def tom_tat_han_muc(self):
        giay = time.monotonic() - self.bat_dau
        return (f"{self.so_request_ok} request thành công trong "
                f"{int(giay // 60)}m{int(giay % 60):02d}s")

    # ------------------------------------------------------------ tiện ích
    def _kiem_deadline(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            sys.exit(f"[DEADLINE] Quá {self.deadline_phut} phút. DỪNG, KHÔNG đụng BigQuery. "
                     f"(restart {self.tong_restart} lần, gặp CAPTCHA {self.so_lan_captcha} lần, "
                     f"renderer treo {self.so_lan_treo} lần.) "
                     f"Nếu run nào cũng chạm deadline thì giảm --last-days hoặc lấy cookie mới.")

    async def _han(self, coro, giay, mo_ta):
        """Timeout từng thao tác không được vượt ngân sách crawl còn lại."""
        if self.deadline is not None:
            con_lai = self.deadline - time.monotonic()
            if con_lai <= 0:
                coro.close()
                self._kiem_deadline()
            giay = min(giay, con_lai)
        try:
            result = await asyncio.wait_for(coro, timeout=giay)
            self._kiem_deadline()
            return result
        except asyncio.TimeoutError:
            self._kiem_deadline()
            self.so_lan_treo += 1
            raise BiChan(f"{mo_ta} quá {giay:g}s không trả về — renderer treo") from None

    async def _cho(self, giay):
        self._kiem_deadline()
        if self.deadline is not None and giay >= self.deadline - time.monotonic():
            sys.exit("[DEADLINE] Không đủ thời gian cho lần chờ/retry tiếp theo. "
                     "DỪNG, KHÔNG đụng BigQuery.")
        await asyncio.sleep(giay)
        self._kiem_deadline()

    def _dung_xac_minh(self, ly_do):
        self.so_lan_captcha += 1
        sys.exit(f"[XÁC MINH] {ly_do}. Đã nhận {self.tom_tat_han_muc()}. "
                 "DỪNG, KHÔNG đụng BigQuery; không tự retry hoặc đổi browser. "
                 "Mở Seller Center bằng trình duyệt của anh, hoàn tất yêu cầu xác minh, "
                 "cập nhật phiên/cookie qua nơi lưu secret rồi chạy --no-bq để kiểm tra. "
                 "Log này không xác định được hạn mức request hay nguyên nhân risk-control.")

    def _kiem_url_auth(self, url):
        # Chỉ dùng host/path để phân loại; không in query có thể chứa token đăng nhập.
        parts = urllib.parse.urlsplit(url or "")
        target = (parts.netloc + parts.path).lower()
        if "login" in target or "passport" in target:
            sys.exit("[COOKIE] Bị chuyển sang trang đăng nhập. Cập nhật cookie rồi chạy lại.")
        if "verify" in target or "captcha" in target:
            self._dung_xac_minh("Bị chuyển sang trang xác minh")

    def _playwright_cookies(self):
        cookies = []
        for part in self.cookie_state.cookie.split(";"):
            part = part.strip()
            if "=" not in part:
                continue
            name, _, value = part.partition("=")
            cookies.append({"name": name.strip(), "value": value.strip(), "url": BASE})
        return cookies

    # ------------------------------------------------------------ vòng đời browser
    async def _destroy_browser(self):
        context, browser = self.context, self.browser
        self.page = None
        self.context = None
        self.browser = None
        for obj, ten in ((context, "context"), (browser, "browser")):
            if obj is None:
                continue
            try:
                # close() cũng treo được khi renderer đang chiếm main thread.
                await asyncio.wait_for(obj.close(), timeout=CLOSE_TIMEOUT_S)
            except asyncio.TimeoutError:
                log(f"    {ten}.close() quá {CLOSE_TIMEOUT_S}s — bỏ lại, dựng cái mới")
            except Exception:
                pass

    async def _dò_captcha(self):
        """Phát hiện giao diện CAPTCHA đang hiện; dừng để người dùng xác minh."""
        self._kiem_url_auth(self.page.url)

        # Canary TRƯỚC: nếu renderer đã chết thì query_selector cũng sẽ treo.
        await self._han(self.page.evaluate("() => 1"), self.canary_timeout, "canary evaluate")

        el = await self._han(self.page.evaluate(JS_DO_CAPTCHA, CAPTCHA_SELECTORS),
                             self.canary_timeout, "dò CAPTCHA trong DOM")
        if el is not None:
            self._dung_xac_minh("Overlay CAPTCHA đang hiển thị")

    async def _launch_browser(self):
        self.browser_number += 1
        log(f"    launch Chromium process #{self.browser_number}")
        self.browser = await self._han(
            self.playwright.chromium.launch(headless=self.headless),
            PAGE_TIMEOUT_MS / 1000, "chromium.launch")
        self.context = await self._han(self.browser.new_context(
            viewport={"width": 1440, "height": 960},
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
        ), self.canary_timeout, "browser.new_context")
        await self._han(self.context.add_cookies(self._playwright_cookies()),
                        self.canary_timeout, "context.add_cookies")
        self.page = await self._han(self.context.new_page(),
                                    self.canary_timeout, "context.new_page")
        self.page.set_default_timeout(PAGE_TIMEOUT_MS)
        await self._han(
            self.page.goto(self.landing, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS),
            PAGE_TIMEOUT_MS / 1000 + 15, "page.goto")
        await self._cho(BROWSER_SETTLE_MS / 1000)
        await self._dò_captcha()
        log(f"    Chromium #{self.browser_number} sẵn sàng")

    async def _restart_browser(self, mo_ta_trang, error):
        await self._destroy_browser()
        self._kiem_deadline()
        if self.restart_count >= self.browser_restarts:
            sys.exit(f"[BROWSER] Hết {self.browser_restarts} lần restart liên tiếp tại "
                     f"{mo_ta_trang}. DỪNG, KHÔNG đụng BigQuery. "
                     f"Đã nhận {self.tom_tat_han_muc()}.")
        if self.tong_restart >= self.max_restart_tong:
            sys.exit(f"[BROWSER] Chạm trần {self.max_restart_tong} lần restart cả lượt. "
                     "DỪNG, KHÔNG đụng BigQuery.")
        self.restart_count += 1
        self.tong_restart += 1
        log(f"  ! lỗi kỹ thuật {type(error).__name__} tại {mo_ta_trang}; "
            f"restart {self.restart_count}/{self.browser_restarts}, "
            f"cộng dồn {self.tong_restart}/{self.max_restart_tong}; giữ cursor trong RAM")
        if self.browser_restart_wait:
            await self._cho(self.browser_restart_wait)
        if self.cookie_state.reload_if_changed():
            log("    đã nạp cookie mới trước khi dựng Chromium")

    async def _ensure_browser(self, mo_ta_trang):
        while self.page is None:
            self._kiem_deadline()
            try:
                await self._launch_browser()
            except SystemExit:
                await self._destroy_browser()
                raise
            except Exception as error:
                if not la_loi_browser(error):
                    raise
                await self._restart_browser(mo_ta_trang, error)

    async def close(self):
        await self._destroy_browser()

    # ------------------------------------------------------------ gọi API
    async def _goi_mot_lan(self, url, body):
        result = await self._han(
            self.page.evaluate(FETCH_IN_PAGE,
                               {"url": url, "body": body, "timeoutMs": JS_FETCH_TIMEOUT_MS}),
            self.eval_timeout, "page.evaluate")

        if not isinstance(result, dict):
            sys.exit("[API] Kết quả fetch sai cấu trúc. DỪNG, KHÔNG đụng BigQuery.")
        # HTTP/auth phải được kiểm tra TRƯỚC lỗi JSON: 401/403 thường trả HTML.
        status = result.get("status")
        if status is None and not result.get("error"):
            sys.exit("[API] Fetch thiếu HTTP status. DỪNG, KHÔNG đụng BigQuery.")
        if status in (401, 403):
            sys.exit(f"[COOKIE] HTTP {status}: phiên hết hạn, thiếu quyền hoặc bị chặn. "
                     "DỪNG, KHÔNG đụng BigQuery; kiểm tra đăng nhập/xác minh ở Seller Center.")
        self._kiem_url_auth(result.get("response_url"))
        if status == 429 or (isinstance(status, int) and 500 <= status < 600):
            raise TrucTracTamThoi(f"HTTP {status}", doc_retry_after(result.get("retry_after")))
        if status is not None and status != 200:
            sys.exit(f"[API] HTTP {status}; kiểm tra request/quyền truy cập. "
                     "DỪNG, KHÔNG đụng BigQuery.")
        if result.get("error"):
            err = str(result["error"])
            if "/passport" in err.lower() or re.search(r"/login(?:[/?\s\"'])", err, re.I):
                sys.exit("[COOKIE] Phản hồi là trang đăng nhập. Cập nhật cookie rồi chạy lại.")
            if co_dau_hieu_chan(err):
                self._dung_xac_minh("Phản hồi chứa dấu hiệu trang xác minh")
            # Không log response HTML hay message thô: có thể chứa token hoặc dữ liệu đơn.
            raise TrucTracTamThoi("JS fetch lỗi mạng hoặc phản hồi không phải JSON")
        js = result.get("data")
        if not isinstance(js, dict) or "code" not in js:
            sys.exit("[API] JSON thiếu code hoặc sai cấu trúc. DỪNG, KHÔNG đụng BigQuery.")
        code = str(js["code"])
        if code == "0":
            if not isinstance(js.get("data"), dict):
                sys.exit("[API] code=0 nhưng thiếu object data. DỪNG, KHÔNG đụng BigQuery.")
            return js
        if code in ("98001002", "98001008"):
            sys.exit(f"[COOKIE] TikTok trả code {code}. Kiểm tra đăng nhập/cookie. "
                     "DỪNG, KHÔNG đụng BigQuery.")
        if code == "10000":
            self._dung_xac_minh("TikTok trả code=10000 (yêu cầu xác minh trong log đã gửi)")
        raise TrucTracTamThoi(f"API code={code}")

    async def post(self, url, body, mo_ta_trang):
        lan_thu = 0
        while True:
            self._kiem_deadline()
            await self._ensure_browser(mo_ta_trang)
            try:
                js = await self._goi_mot_lan(url, body)
                self.so_request_ok += 1
                # Ngân sách restart là cho một CHUỖI lỗi liên tiếp, không phải cho cả run:
                # backfill vài trăm trang dính 3 lỗi vặt cách xa nhau vẫn phải chạy tiếp.
                self.restart_count = 0
                return js
            except SystemExit:
                raise
            except TrucTracTamThoi as error:
                lan_thu += 1
                if lan_thu > API_RETRY_TAI_CHO:
                    sys.exit(f"[API] {mo_ta_trang}: hết {API_RETRY_TAI_CHO} lần retry: "
                             f"{error}. DỪNG, KHÔNG đụng BigQuery; không restart browser.")
                cho = API_RETRY_SLEEP[min(lan_thu - 1, len(API_RETRY_SLEEP) - 1)]
                if error.retry_after is not None:
                    cho = max(cho, error.retry_after)
                if cho > 300:
                    sys.exit(f"[API] Server yêu cầu chờ {cho:g}s, vượt ngân sách 300s "
                             "mỗi lần chờ; dừng, không retry sớm và KHÔNG đụng BigQuery.")
                log(f"    {mo_ta_trang}: {error} — thử lại TẠI CHỖ sau {cho:g}s "
                    f"({lan_thu}/{API_RETRY_TAI_CHO}, không đập browser)")
                await self._cho(cho)
            except BiChan as error:
                await self._restart_browser(mo_ta_trang, error)
                lan_thu = 0
            except Exception as error:
                if not la_loi_browser(error):
                    raise
                await self._restart_browser(mo_ta_trang, error)
                lan_thu = 0


# ---------------------------------------------------------------- kéo dữ liệu
def du_lieu_loi(message):
    sys.exit(f"[DỮ LIỆU] {message}. DỪNG, KHÔNG đụng BigQuery.")


def so_nguyen(value, name):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        du_lieu_loi(f"{name} không phải số nguyên không âm")
    return int(value)


def co_trang_tiep(value):
    if value is True or type(value) is int and value == 1 or value in ("1", "true"):
        return True
    if value is False or type(value) is int and value == 0 or value in ("0", "false"):
        return False
    du_lieu_loi("search_next_has_more thiếu hoặc sai kiểu")


def doc_nguoi_nhan(order):
    skus = order.get("sku_module")
    if not isinstance(skus, list):
        du_lieu_loi("sku_module thiếu hoặc không phải list")
    seen, lech_regex = set(), 0
    for sku in skus:
        if not isinstance(sku, dict):
            du_lieu_loi("phần tử sku_module không phải object")
        info = sku.get("creator_info_name")
        if info is None:
            continue
        if not isinstance(info, dict):
            du_lieu_loi("creator_info_name không phải object")
        items = info.get("items", [])
        if not isinstance(items, list):
            du_lieu_loi("creator_info_name.items không phải list")
        for item in items:
            if not isinstance(item, dict):
                du_lieu_loi("creator_info_name.items chứa phần tử sai kiểu")
            msg = item.get("message_content")
            if msg is None:
                continue
            if not isinstance(msg, str):
                du_lieu_loi("message_content không phải chuỗi")
            match = RE_NGUOI_NHAN.search(msg)
            if match:
                seen.add(match.group(1))
            elif RE_LONG.search(msg):
                lech_regex += 1
    return seen, lech_regex


async def keo_mot_kenh(client, seller_id, sale_source, t0, t1, kiem_tra_total=True, nhan=""):
    """Đếm ID đơn duy nhất; chỉ trả kết quả khi phân trang kết thúc hợp lệ."""
    url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
    cond = {
        "order_source": {"value": ["1"]},
        "time_order_created": {"value": [str(t0), str(t1)]},
        "sale_source": {"value": [sale_source]},
    }
    out, cursor, pages, total = [], "", 0, None
    da_doc, cursor_da_dung = {}, set()
    so_don, lech_regex, con_tiep = 0, 0, False
    while pages < MAX_PAGES:
        if cursor in cursor_da_dung:
            du_lieu_loi(f"{nhan}sale_source={sale_source}: cursor bị lặp")
        cursor_da_dung.add(cursor)
        body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
                "search_cursor": cursor, "search_condition": {"condition_list": cond}}
        js = await client.post(url, body,
                               f"{nhan}sale_source={sale_source}, trang {pages + 1}")
        d = js.get("data")
        if not isinstance(d, dict):
            du_lieu_loi("data thiếu hoặc không phải object")
        if pages == 0:
            if kiem_tra_total:
                total = so_nguyen(d.get("total_count"), "total_count trang đầu")
            else:
                total = d.get("total_count")
        orders = d.get("main_orders")
        # Một số response rỗng dùng null/missing thay cho []; chỉ chấp nhận khi
        # server đồng thời công bố total_count=0 và không có trang tiếp.
        if orders is None and str(d.get("total_count")) == "0":
            orders = []
        if not isinstance(orders, list):
            du_lieu_loi("main_orders thiếu hoặc không phải list")
        more = d.get("search_next_has_more")
        if more is None and not orders and str(d.get("total_count")) == "0":
            more = False
        con_tiep = co_trang_tiep(more)
        next_cursor = d.get("search_next_cursor")
        if next_cursor is None:
            next_cursor = ""
        if not isinstance(next_cursor, str):
            du_lieu_loi("search_next_cursor không phải chuỗi")
        if con_tiep and (not next_cursor or next_cursor in cursor_da_dung):
            du_lieu_loi(f"{nhan}sale_source={sale_source}: còn trang nhưng cursor thiếu/lặp")
        if not orders and con_tiep:
            du_lieu_loi(f"{nhan}sale_source={sale_source}: trang rỗng nhưng còn dữ liệu")
        so_don += len(orders)
        for order in orders:
            if not isinstance(order, dict):
                du_lieu_loi("main_orders chứa phần tử không phải object")
            oid = so_nguyen(order.get("main_order_id"), "main_order_id")
            trade = order.get("trade_order_module")
            if not isinstance(trade, dict):
                du_lieu_loi("trade_order_module thiếu hoặc không phải object")
            created = so_nguyen(trade.get("create_time"), "create_time")
            if oid <= 0 or not t0 <= created <= t1:
                du_lieu_loi("ID đơn không hợp lệ hoặc create_time nằm ngoài khoảng đang kéo")
            creators, lech = doc_nguoi_nhan(order)
            lech_regex += lech
            record = (created, frozenset(creators))
            if oid in da_doc:
                if da_doc[oid] != record:
                    du_lieu_loi("một ID đơn có dữ liệu khác nhau giữa các trang cùng kênh")
                continue
            da_doc[oid] = record
            out.extend((oid, created, creator) for creator in sorted(creators))
        pages += 1
        if pages % HEARTBEAT_MOI == 0:
            log(f"    ... {nhan}sale_source={sale_source}: {pages} trang, "
                f"{len(da_doc)} đơn duy nhất | cả run: {client.tom_tat_han_muc()}")
        cursor = next_cursor
        if not con_tiep:
            break
    log(f"  {nhan}sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
        f"total_count={total}, {pages} trang, {so_don} lượt đơn, "
        f"{len(da_doc)} đơn duy nhất, {len(out)} cặp đơn-creator")
    if lech_regex:
        log(f"    {lech_regex} item chứa 'hoa hồng:' nhưng không khớp 'Người nhận hoa hồng:'")
    if con_tiep:
        du_lieu_loi(f"{nhan}sale_source={sale_source}: chạm MAX_PAGES={MAX_PAGES} "
                    "mà vẫn còn dữ liệu; cần kiểm tra phân trang/khoảng ngày")
    if kiem_tra_total and len(da_doc) < total:
        du_lieu_loi(f"{nhan}sale_source={sale_source}: total_count={total} "
                    f"nhưng chỉ lấy được {len(da_doc)} ID đơn duy nhất")
    return out


def chia_khoi_ngay(d0, d1, so_ngay):
    """Cắt [d0, d1] thành các khối liền nhau, mỗi khối tối đa `so_ngay` ngày.

    `so_ngay <= 0` nghĩa là KHÔNG cắt — trả về một khối duy nhất. Đây là mặc định; lý do
    xem CHUNK_NGAY_MAC_DINH.

    Các khối rời nhau và phủ kín, nên gộp lại đúng bằng khoảng gốc — không hở ngày nào,
    không trùng ngày nào.
    """
    if so_ngay <= 0:
        return [(d0, d1)]
    khoi, dau = [], d0
    while dau <= d1:
        cuoi = min(dau + datetime.timedelta(days=so_ngay - 1), d1)
        khoi.append((dau, cuoi))
        dau = cuoi + datetime.timedelta(days=1)
    return khoi


async def keo(client, seller_id, khoi_ngay, kiem_tra_total=True):
    """Kéo lần lượt từng khối ngày × từng kênh, gộp và khử trùng vào một dict.

    Khoá khử trùng là (main_order_id, creator_username) nên dù các khối có chồng nhau
    hay một đơn về ở hai kênh thì cũng không nhân đôi dòng.
    """
    rows, la = {}, set()
    tong_khoi = len(khoi_ngay)
    for i, (kd0, kd1) in enumerate(khoi_ngay, 1):
        t0 = int(datetime.datetime.combine(kd0, datetime.time(0, 0, 0), TZ).timestamp())
        t1 = int(datetime.datetime.combine(kd1, datetime.time(23, 59, 59), TZ).timestamp())
        nhan = f"[khối {i}/{tong_khoi} {kd0}..{kd1}] "
        log(f"khối {i}/{tong_khoi}: {kd0} -> {kd1} (giờ VN)")
        for ss in ("1", "2", "3"):
            ket = await keo_mot_kenh(client, seller_id, ss, t0, t1, kiem_tra_total, nhan)
            for oid, t, cu in ket:
                if cu not in CREATORS:
                    la.add(cu)
                    continue
                key = (oid, cu)
                pos = SALE_SOURCE_TO_POSITION[ss]
                if key in rows:
                    if rows[key]["promotion_position_type"] != pos:
                        log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
                            f"({rows[key]['promotion_position_type']} và {pos}) "
                            f"— giữ kênh đầu")
                    continue
                rows[key] = {
                    "main_order_id": oid,
                    "creator_nickname": CREATORS[cu],
                    "creator_username": cu,
                    "promotion_position_type": pos,
                    "create_time": datetime.datetime.fromtimestamp(
                        t, datetime.timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
                    "cos_ratio": None,
                    "estimated_cos_fee": None,
                    "shop_ads_commission_ratio": None,
                    "estimated_shop_ads_commission": None,
                    "id_shop": seller_id,
                }
    if la:
        log(f"  (bỏ qua {len(la)} creator ngoài danh sách CREATORS, vd: {sorted(la)[:5]})")
    return sorted(rows.values(), key=lambda r: r["create_time"])


# ---------------------------------------------------------------- BigQuery
def bq_client():
    """Giống các job khác trong repo: ưu tiên key file, rồi JSON inline trong biến
    môi trường (secret GOOGLE_SERVICE_ACCOUNT_JSON), cuối cùng mới dùng ADC."""
    from google.cloud import bigquery
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

    if key_path and os.path.exists(key_path):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(key_path)
        log(f"  BigQuery: key file {key_path}")
        return bigquery.Client(project=BQ_PROJECT, credentials=cred)
    if key_json:
        from google.oauth2 import service_account
        try:
            info = json.loads(key_json)
        except json.JSONDecodeError as e:
            sys.exit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
        cred = service_account.Credentials.from_service_account_info(info)
        log(f"  BigQuery: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email','?')})")
        return bigquery.Client(project=BQ_PROJECT, credentials=cred)
    log("  BigQuery: credentials mặc định của môi trường (ADC)")
    return bigquery.Client(project=BQ_PROJECT)


def _dml_theo_cau_lenh(client, script_job):
    """Số dòng DELETE/INSERT của từng câu trong script. Thiếu quyền jobs.list thì thôi."""
    try:
        ra = {}
        for child in client.list_jobs(parent_job=script_job.job_id):
            if child.num_dml_affected_rows is not None:
                ra[child.statement_type] = child.num_dml_affected_rows
        return ra
    except Exception:
        return {}


def nap_bq(rows, seller_id, utc0, utc1):
    """Nạp qua bảng staging rồi DELETE + INSERT trong MỘT transaction."""
    if not rows:
        du_lieu_loi("không nạp tập rỗng vào BigQuery")
    keys, ids = set(rows[0]), set()
    for row in rows:
        if set(row) != keys or row.get("id_shop") != seller_id:
            du_lieu_loi("schema các dòng không đồng nhất hoặc sai shop")
        if row.get("creator_username") not in CREATORS:
            du_lieu_loi("creator ngoài phạm vi ghi đè")
        if row.get("promotion_position_type") not in (1, 2, 3):
            du_lieu_loi("promotion_position_type không hợp lệ")
        oid = so_nguyen(row.get("main_order_id"), "main_order_id")
        key = (oid, row["creator_username"])
        if not oid or key in ids:
            du_lieu_loi("ID đơn không hợp lệ hoặc trùng cặp đơn-creator khi nạp")
        ids.add(key)
        try:
            created = datetime.datetime.fromisoformat(row["create_time"])
        except (KeyError, TypeError, ValueError):
            du_lieu_loi("create_time không phải DATETIME hợp lệ")
        if created.tzinfo is not None or not utc0 <= created <= utc1:
            du_lieu_loi("create_time ngoài khoảng UTC ghi đè hoặc chứa timezone")
    from google.cloud import bigquery
    client = bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    bang_dich = client.get_table(table_id)

    cot_bang = {f.name for f in bang_dich.schema}
    thieu = [c for c in rows[0] if c not in cot_bang]
    if thieu:
        sys.exit(f"[BQ] các cột {thieu} không tồn tại trong {table_id}.")
    cot = ", ".join(f"`{c}`" for c in rows[0])

    stg_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}__stg_{uuid.uuid4().hex[:12]}"
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("shop", "STRING", seller_id),
        bigquery.ArrayQueryParameter("creators", "STRING", list(CREATORS)),
        bigquery.ScalarQueryParameter("t0", "DATETIME", utc0),
        bigquery.ScalarQueryParameter("t1", "DATETIME", utc1),
    ])
    sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{table_id}`
        WHERE id_shop = @shop
          AND creator_username IN UNNEST(@creators)
          AND create_time >= @t0 AND create_time <= @t1;
        INSERT INTO `{table_id}` ({cot}) SELECT {cot} FROM `{stg_id}`;
        COMMIT TRANSACTION;
    """
    try:
        load_job = client.load_table_from_json(
            rows, stg_id,
            job_config=bigquery.LoadJobConfig(
                schema=bang_dich.schema, write_disposition="WRITE_TRUNCATE"))
        load_job.result()
        if load_job.output_rows != len(rows):
            du_lieu_loi(f"staging có {load_job.output_rows} dòng, kỳ vọng {len(rows)}")
        log(f"  staging {stg_id.split('.')[-1]}: {len(rows)} dòng")
        job = client.query(sql, job_config=cfg)
        job.result()
    finally:
        try:
            client.delete_table(stg_id, not_found_ok=True)
        except Exception as error:
            # Lỗi dọn staging không được che mất lỗi load/query hoặc báo sai kết quả COMMIT.
            log(f"  ! Chưa dọn được staging {stg_id}: {type(error).__name__}; cần dọn sau.")
    dml = _dml_theo_cau_lenh(client, job)
    log(f"  DELETE {dml.get('DELETE', '?')} dòng cũ + INSERT {dml.get('INSERT', len(rows))} "
        f"dòng mới, cùng 1 transaction -> {table_id}")


# ---------------------------------------------------------------- main
def im_lang_loi_thua(loop):
    """Sau khi _han() timeout, task page.evaluate bị huỷ nhưng future nội bộ của
    Playwright vẫn kết thúc bằng TargetClosedError, và asyncio in nguyên một traceback
    vô nghĩa ra log. Chính log rác kiểu này làm sự cố treo khó chẩn đoán, nên chặn."""
    goc = loop.get_exception_handler()

    def handler(lp, context):
        exc = context.get("exception")
        if exc is not None and type(exc).__name__ in ("TargetClosedError", "CancelledError"):
            return
        if goc is not None:
            goc(lp, context)
        else:
            lp.default_exception_handler(context)

    loop.set_exception_handler(handler)


async def chay_crawl(cookie_state, seller_id, khoi_ngay, a):
    from playwright.async_api import async_playwright
    im_lang_loi_thua(asyncio.get_running_loop())
    async with async_playwright() as playwright:
        client = TikTokBrowserClient(
            playwright,
            cookie_state,
            browser_restarts=a.browser_restarts,
            browser_restart_wait=a.browser_restart_wait,
            deadline_phut=a.deadline_phut,
            landing=(BASE + a.landing),
            headless=not a.headful,
        )
        try:
            rows = await keo(client, seller_id, khoi_ngay, not a.skip_total_check)
            log(f"thống kê chặn: CAPTCHA {client.so_lan_captcha} lần, renderer treo "
                f"{client.so_lan_treo} lần, restart browser {client.tong_restart} lần")
            return rows
        finally:
            await client.close()


def main():
    # keo_mot_kenh đọc PAGE_SIZE như biến module (giống cách test_offline.py vá các hằng
    # số khác), nên --page-size ghi thẳng vào đây. Khai báo phải nằm trước mọi lần đọc
    # tên này trong hàm, kể cả trong default= của argparse.
    global PAGE_SIZE, BQ_PROJECT, BQ_DATASET, BQ_TABLE
    ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery (bản sửa 2026-09-10)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--date", help="một ngày YYYY-MM-DD (giờ VN)")
    g.add_argument("--last-days", type=int, default=None,
                   help=f"N ngày gần nhất tính cả hôm nay (mặc định {LAST_DAYS_MAC_DINH})")
    ap.add_argument("--from", dest="d_from", help="từ ngày YYYY-MM-DD")
    ap.add_argument("--to", dest="d_to", help="đến ngày YYYY-MM-DD")
    ap.add_argument("--cookie", default=COOKIE,
                    help=f"file cookie (mặc định {COOKIE}); bị bỏ qua nếu đã có "
                         f"biến môi trường {ENV_COOKIE}")
    ap.add_argument("--csv", help="ghi thêm ra file CSV để kiểm tra")
    ap.add_argument("--no-bq", action="store_true", help="không đụng BigQuery")
    ap.add_argument("--min-rows", type=int, default=1,
                    help="ít hơn số này thì DỪNG, không xoá gì (chống wipe)")
    ap.add_argument("--skip-total-check", action="store_true",
                    help="bỏ đối chiếu số đơn lấy được với total_count của TikTok")
    ap.add_argument("--browser-restarts", type=int, default=BROWSER_RESTARTS,
                    help=f"số lần đập/dựng lại Chromium liên tiếp (mặc định {BROWSER_RESTARTS})")
    ap.add_argument("--browser-restart-wait", type=int, default=BROWSER_RESTART_WAIT_SECONDS,
                    help=f"số giây chờ trước khi launch Chromium mới "
                         f"(mặc định {BROWSER_RESTART_WAIT_SECONDS})")
    ap.add_argument("--deadline-phut", type=int, default=DEADLINE_PHUT_MAC_DINH,
                    help=f"trần thời gian crawl, quá thì dừng và KHÔNG đụng BigQuery "
                         f"(mặc định {DEADLINE_PHUT_MAC_DINH}; 0 = tắt)")
    ap.add_argument("--landing", default=LANDING_MAC_DINH,
                    help=f"đường dẫn trang landing để inject cookie (mặc định "
                         f"{LANDING_MAC_DINH}). Chỉ cần cùng origin là cookie vẫn gửi. "
                         f"Đổi sang '/order' là mở lại SPA — chỉ dùng khi cần debug.")
    ap.add_argument("--headful", action="store_true",
                    help="hiện cửa sổ Chromium (xem tận mắt CAPTCHA lúc debug ở máy)")
    ap.add_argument("--chunk-ngay", type=int, default=CHUNK_NGAY_MAC_DINH,
                    help="số ngày mỗi khối (0 = không cắt); không reset yêu cầu xác minh")
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE,
                    help=f"số đơn mỗi request (mặc định {PAGE_SIZE}); giá trị >100 chỉ "
                         "được thử với --no-bq vì chưa có bằng chứng API hỗ trợ")
    ap.add_argument("--bq-project", default=BQ_PROJECT, help="project đích; dùng staging khi test")
    ap.add_argument("--bq-dataset", default=BQ_DATASET, help="dataset đích")
    ap.add_argument("--bq-table", default=BQ_TABLE, help="bảng đích đã tồn tại, cùng schema")
    a = ap.parse_args()

    if a.browser_restarts < 0 or a.browser_restart_wait < 0 or a.deadline_phut < 0:
        sys.exit("--browser-restarts, --browser-restart-wait, --deadline-phut không được âm.")
    if a.chunk_ngay < 0:
        sys.exit("--chunk-ngay không được âm (0 = không cắt).")
    if a.page_size < 1:
        sys.exit("--page-size phải >= 1.")
    if a.min_rows < 1:
        sys.exit("--min-rows phải >= 1; không cho phép nạp tập rỗng.")
    if a.last_days is not None and a.last_days < 1:
        sys.exit("--last-days phải >= 1.")
    if a.page_size > 100 and not a.no_bq:
        sys.exit("--page-size >100 chưa được xác nhận trên TikTok; chỉ thử với --no-bq.")
    if not a.landing.startswith("/") or a.landing.startswith("//"):
        sys.exit("--landing phải là đường dẫn cùng origin, ví dụ /robots.txt.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", a.bq_project):
        sys.exit("--bq-project không hợp lệ.")
    for name, value in (("dataset", a.bq_dataset), ("table", a.bq_table)):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            sys.exit(f"--bq-{name} không hợp lệ.")
    BQ_PROJECT, BQ_DATASET, BQ_TABLE = a.bq_project, a.bq_dataset, a.bq_table
    PAGE_SIZE = a.page_size

    if a.d_from or a.d_to:
        if not (a.d_from and a.d_to):
            sys.exit("--from và --to phải đi cùng nhau.")
        if a.date or a.last_days is not None:
            sys.exit("--from/--to không dùng chung với --date hoặc --last-days.")
        d0 = datetime.date.fromisoformat(a.d_from)
        d1 = datetime.date.fromisoformat(a.d_to)
    elif a.date:
        d0 = d1 = datetime.date.fromisoformat(a.date)
    else:
        d1 = datetime.datetime.now(TZ).date()
        so_ngay = LAST_DAYS_MAC_DINH if a.last_days is None else a.last_days
        d0 = d1 - datetime.timedelta(days=so_ngay - 1)
    if d0 > d1:
        sys.exit("Khoảng ngày không hợp lệ.")

    cookie_state = doc_cookie(a.cookie)
    seller_id = cookie_state.seller_id
    start = datetime.datetime.combine(d0, datetime.time(0, 0, 0), TZ)
    end = datetime.datetime.combine(d1, datetime.time(23, 59, 59), TZ)
    # Khoảng XOÁ vẫn là trọn cửa sổ, KHÔNG cắt theo khối — chia khối chỉ để gọi API cho
    # cursor đỡ sâu, còn DELETE+INSERT vẫn làm một lần cho cả khoảng như trước.
    utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    khoi_ngay = chia_khoi_ngay(d0, d1, a.chunk_ngay)

    log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
    log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")
    log(f"page_size={PAGE_SIZE} | {len(khoi_ngay)} khối; "
        "số request không phải bằng chứng về hạn mức TikTok")
    if not a.no_bq:
        log(f"BigQuery đích: {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

    try:
        import playwright  # noqa: F401
    except ImportError:
        sys.exit("[PLAYWRIGHT] Chưa cài Playwright. Chạy: pip install playwright && "
                 "python -m playwright install chromium")

    rows = asyncio.run(chay_crawl(cookie_state, seller_id, khoi_ngay, a))

    log(f"TỔNG: {len(rows)} dòng, {len({r['main_order_id'] for r in rows})} đơn")
    for cu in CREATORS:
        sub = [r for r in rows if r["creator_username"] == cu]
        if sub:
            b = {}
            for r in sub:
                b[r["promotion_position_type"]] = b.get(r["promotion_position_type"], 0) + 1
            log(f"  {cu:<20} {len(sub):>5}  (1 Thẻ SP={b.get(1,0)}, 2 Video={b.get(2,0)}, 3 LIVE={b.get(3,0)})")

    if a.csv:
        import csv as _csv
        with open(a.csv, "w", newline="", encoding="utf-8-sig") as f:
            w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
            if rows:
                w.writeheader()
                w.writerows(rows)
        log(f"đã ghi {a.csv}")

    if a.no_bq:
        log("--no-bq: dừng, không đụng BigQuery.")
        return
    if len(rows) < a.min_rows:
        sys.exit(f"[GUARD] chỉ có {len(rows)} dòng (< --min-rows {a.min_rows}). "
                 f"DỪNG, không xoá gì. Nghi cookie hỏng hoặc TikTok đổi API.")
    nap_bq(rows, seller_id, utc0, utc1)
    log("XONG.")


if __name__ == "__main__":
    main()

# if __name__ == "__main__":
#     main()

