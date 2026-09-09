
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

# VÍ DỤ:
#   py -X utf8 keo_creator_daily.py                     # 41 ngày gần nhất (LAST_DAYS_MAC_DINH)
#   py -X utf8 keo_creator_daily.py --date 2026-08-05 --no-bq --csv out.csv
#   py -X utf8 keo_creator_daily.py --from 2026-06-29 --to 2026-07-31   # backfill
#   py -X utf8 keo_creator_daily.py --last-days 2 --cookie cookie_shop2.txt

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
# """
# import argparse, datetime, json, os, re, sys, time, urllib.parse, uuid

# # ---------------------------------------------------------------- cấu hình
# BASE = "https://seller-vn.tiktok.com"
# API = BASE + "/api/fulfillment/order/list"
# COOKIE = "cookie.txt"          # file cookie mặc định khi chạy ở máy
# ENV_COOKIE = "TIKTOK_COOKIE"   # biến môi trường, ưu tiên hơn file (dùng cho GitHub Actions)
# TZ = datetime.timezone(datetime.timedelta(hours=7))

# BQ_PROJECT = "rhysman-data-warehouse-488306"
# BQ_DATASET = "rhysman"
# BQ_TABLE = "fact_creator_tiktok"

# # Số ngày kéo khi không truyền --date/--from/--to. Đổi số ngày mặc định thì sửa ĐÚNG ở
# # đây (và ${LAST_DAYS:-41} trong .github/workflows/creator-daily.yml cho lịch chạy tự
# # động). Mỗi run xoá và ghi lại trọn cửa sổ này, nên tăng lên là tăng cả thời gian chạy
# # lẫn số partition BigQuery bị viết lại mỗi lần.
# LAST_DAYS_MAC_DINH = 41

# # Chỉ nạp các creator biết chắc nickname. API chỉ trả username, không trả nickname —
# # thêm tên mới thì phải bổ sung ở đây, nếu không sẽ bị bỏ qua (có cảnh báo).
# CREATORS = {
#     "rhysman.com": "Rhys Man Chính Hãng",
#     "rhysman.shopping": "Rhys Man",
#     "rhysman_channel": "Rhys Man Chăm Sóc Cơ Thể",
# }

# # sale_source (API) -> promotion_position_type (BigQuery)
# SALE_SOURCE_TO_POSITION = {"1": 3, "2": 2, "3": 1}
# SALE_SOURCE_TEN = {"1": "LIVE", "2": "Video", "3": "Thẻ sản phẩm"}

# # Chỉ nhận đúng item "Người nhận hoa hồng: <handle>". Regex lỏng r"hoa hồng:" nuốt luôn
# # "Tỷ lệ hoa hồng: 20%" -> rác lọt vào cảnh báo "creator ngoài danh sách", làm hỏng đúng
# # cái cảnh báo dùng để biết khi nào phải thêm tên vào CREATORS. Giữ RE_LONG để đếm và
# # log số item bị loại, phòng khi TikTok đổi câu chữ thì thấy ngay.
# RE_NGUOI_NHAN = re.compile(r"người nhận hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
# RE_LONG = re.compile(r"hoa hồng\s*:\s*(\S+)", re.IGNORECASE)
# PAGE_SIZE = 100
# MAX_PAGES = 800
# BROWSER_RESTARTS = 3
# BROWSER_RESTART_WAIT_SECONDS = 5
# PAGE_TIMEOUT_MS = 90_000
# JS_FETCH_TIMEOUT_MS = 90_000
# BROWSER_SETTLE_MS = 4_000


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


# class BrowserRestartRequired(RuntimeError):
#     pass


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
#     """Gọi API trong page và thay toàn bộ Chromium khi browser/context hỏng."""

#     def __init__(self, playwright, cookie_state, browser_restarts=BROWSER_RESTARTS,
#                  browser_restart_wait=BROWSER_RESTART_WAIT_SECONDS):
#         self.playwright = playwright
#         self.cookie_state = cookie_state
#         self.browser_restarts = browser_restarts
#         self.browser_restart_wait = browser_restart_wait
#         self.restart_count = 0
#         self.browser_number = 0
#         self.browser = None
#         self.context = None
#         self.page = None

#     def _playwright_cookies(self):
#         cookies = []
#         for part in self.cookie_state.cookie.split(";"):
#             part = part.strip()
#             if "=" not in part:
#                 continue
#             name, _, value = part.partition("=")
#             cookies.append({"name": name.strip(), "value": value.strip(), "url": BASE})
#         return cookies

#     def _destroy_browser(self):
#         context, browser = self.context, self.browser
#         self.page = None
#         self.context = None
#         self.browser = None
#         if context is not None:
#             try:
#                 context.close()
#             except Exception:
#                 pass
#         if browser is not None:
#             try:
#                 browser.close()
#             except Exception:
#                 pass

#     def _launch_browser(self):
#         self.browser_number += 1
#         log(f"    launch Chromium process #{self.browser_number}")
#         self.browser = self.playwright.chromium.launch(headless=True)
#         self.context = self.browser.new_context(
#             viewport={"width": 1440, "height": 960},
#             locale="vi-VN",
#             timezone_id="Asia/Ho_Chi_Minh",
#         )
#         cookies = self._playwright_cookies()
#         self.context.add_cookies(cookies)
#         self.page = self.context.new_page()
#         self.page.set_default_timeout(PAGE_TIMEOUT_MS)
#         self.page.goto(BASE + "/order", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
#         self.page.wait_for_timeout(BROWSER_SETTLE_MS)
#         current_url = self.page.url.lower()
#         if "login" in current_url or "passport" in current_url:
#             sys.exit(f"[COOKIE] Browser bị chuyển sang trang đăng nhập: {self.page.url}. "
#                      "Hãy cập nhật cookie rồi chạy lại.")
#         if "verify" in current_url or "captcha" in current_url:
#             raise BrowserRestartRequired(f"browser mở vào trang xác minh: {self.page.url}")
#         log(f"    Chromium #{self.browser_number} sẵn sàng: {self.page.url}")

#     def _restart_browser(self, page_description, error):
#         self._destroy_browser()
#         if self.restart_count >= self.browser_restarts:
#             sys.exit(
#                 f"[BROWSER] Đã đập và dựng lại Chromium {self.browser_restarts} lần tại "
#                 f"{page_description} nhưng vẫn lỗi: {error}. Cursor chưa bị tăng và BigQuery "
#                 "chưa bị thay đổi."
#             )
#         self.restart_count += 1
#         log(f"  ! browser lỗi tại {page_description}: {error}")
#         log(f"    đập toàn bộ browser; giữ checkpoint cursor của page hiện tại "
#             f"({self.restart_count}/{self.browser_restarts})")
#         if self.browser_restart_wait:
#             time.sleep(self.browser_restart_wait)
#         cookie_changed = self.cookie_state.reload_if_changed()
#         if cookie_changed:
#             log("    đã nạp cookie mới trước khi dựng Chromium")

#     def _ensure_browser(self, page_description):
#         while self.page is None:
#             try:
#                 self._launch_browser()
#             except SystemExit:
#                 self._destroy_browser()
#                 raise
#             except Exception as error:
#                 self._restart_browser(page_description, error)

#     def close(self):
#         self._destroy_browser()

#     def post(self, url, body, page_description):
#         while True:
#             self._ensure_browser(page_description)
#             try:
#                 result = self.page.evaluate(
#                     FETCH_IN_PAGE,
#                     {"url": url, "body": body, "timeoutMs": JS_FETCH_TIMEOUT_MS},
#                 )
#                 if result.get("error"):
#                     raise BrowserRestartRequired(result["error"])
#                 status = result.get("status")
#                 if status in (401, 403):
#                     sys.exit(f"[COOKIE] Browser nhận HTTP {status} — cookie hết hạn hoặc bị "
#                              "risk-control chặn. Lấy cookie mới rồi chạy lại. "
#                              "(Dựng lại browser cũng không cứu được, nên dừng luôn.)")
#                 if status != 200:
#                     raise BrowserRestartRequired(f"HTTP status {status}")
#                 js = result.get("data") or {}
#                 code = js.get("code")
#                 if str(code) == "0":
#                     # Ngân sách restart là cho một CHUỖI lỗi liên tiếp, không phải cho cả run:
#                     # backfill vài trăm trang dính 3 lỗi vặt cách xa nhau vẫn phải chạy tiếp.
#                     self.restart_count = 0
#                     return js
#                 if str(code) in ("98001002", "98001008"):
#                     sys.exit(f"[COOKIE] TikTok trả code {code}: {js.get('message')}. "
#                              "Cookie đã hết hạn — lấy cookie mới rồi chạy lại.")
#                 if str(code) == "10000":
#                     raise BrowserRestartRequired(
#                         js.get("message") or "TikTok yêu cầu xác minh browser"
#                     )
#                 raise BrowserRestartRequired(f"API code={code}: {js.get('message')}")
#             except SystemExit:
#                 raise
#             except Exception as error:
#                 self._restart_browser(page_description, error)


# # ---------------------------------------------------------------- kéo dữ liệu
# def keo_mot_kenh(client, seller_id, sale_source, t0, t1, kiem_tra_total=True):
#     """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh."""
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
#         js = client.post(url, body, f"sale_source={sale_source}, trang {pages + 1}")
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
#         con_tiep = bool(d.get("search_next_has_more")) and bool(cursor)
#         if not con_tiep:
#             break

#     log(f"  sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
#         f"total_count={total}, {pages} trang, {so_don} đơn, {len(out)} cặp đơn-creator")
#     if bo_qua:
#         log(f"    ! bỏ {bo_qua} đơn thiếu main_order_id hoặc create_time")
#     if lech_regex:
#         log(f"    ({lech_regex} item có 'hoa hồng:' nhưng không phải 'Người nhận hoa hồng:' "
#             f"— đã bỏ qua)")

#     # Hai guard dưới đây phải là sys.exit chứ không phải log: nap_bq sẽ DELETE nguyên
#     # khoảng ngày rồi INSERT lại, nên kéo thiếu mà chạy tiếp là mất dữ liệu thật.
#     if con_tiep:
#         sys.exit(f"[PHÂN TRANG] sale_source={sale_source} chạm trần MAX_PAGES={MAX_PAGES} "
#                  f"mà TikTok vẫn báo còn dữ liệu. DỪNG, không đụng BigQuery. "
#                  f"Hãy chia nhỏ khoảng --from/--to hoặc tăng MAX_PAGES.")
#     try:
#         total_int = int(total)
#     except (TypeError, ValueError):
#         total_int = None
#     if kiem_tra_total and total_int is not None and so_don < total_int:
#         sys.exit(f"[PHÂN TRANG] sale_source={sale_source}: TikTok báo total_count={total_int} "
#                  f"nhưng chỉ lấy được {so_don} đơn. DỪNG, không đụng BigQuery. "
#                  f"Nếu chắc chắn total_count của TikTok sai thì chạy lại với --skip-total-check.")
#     return out


# def keo(client, seller_id, t0, t1, kiem_tra_total=True):
#     rows, la = {}, set()
#     for ss in ("1", "2", "3"):
#         for oid, t, cu in keo_mot_kenh(client, seller_id, ss, t0, t1, kiem_tra_total):
#             if cu not in CREATORS:
#                 la.add(cu)
#                 continue
#             key = (oid, cu)
#             pos = SALE_SOURCE_TO_POSITION[ss]
#             if key in rows:
#                 if rows[key]["promotion_position_type"] != pos:
#                     log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
#                         f"({rows[key]['promotion_position_type']} và {pos}) — giữ kênh đầu")
#                 continue
#             rows[key] = {
#                 "main_order_id": oid,
#                 "creator_nickname": CREATORS[cu],
#                 "creator_username": cu,
#                 "promotion_position_type": pos,
#                 "create_time": datetime.datetime.fromtimestamp(
#                     t, datetime.timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
#                 "cos_ratio": None,
#                 "estimated_cos_fee": None,
#                 "shop_ads_commission_ratio": None,
#                 "estimated_shop_ads_commission": None,
#                 "id_shop": seller_id,
#             }
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
#     """Nạp qua bảng staging rồi DELETE + INSERT trong MỘT transaction.

#     Cách cũ (DELETE thẳng rồi mới load) mà load hỏng là thủng nguyên khoảng ngày trong
#     bảng thật — daily thì chạy lại là xong, nhưng backfill 1 tháng thì mất 1 tháng dữ
#     liệu cho tới lần chạy sau. Staging cũng bắt luôn lỗi lệch kiểu vì nó load theo đúng
#     schema bảng đích thay vì để BigQuery tự đoán.
#     """
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
# def main():
#     ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery")
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
#                     help="bỏ đối chiếu số đơn lấy được với total_count của TikTok "
#                          "(chỉ dùng khi biết chắc total_count của TikTok sai)")
#     ap.add_argument("--browser-restarts", type=int, default=BROWSER_RESTARTS,
#                     help="số lần đập và dựng lại toàn bộ Chromium khi browser lỗi "
#                          f"(mặc định {BROWSER_RESTARTS})")
#     ap.add_argument("--browser-restart-wait", type=int, default=BROWSER_RESTART_WAIT_SECONDS,
#                     help="số giây chờ trước khi launch Chromium mới "
#                          f"(mặc định {BROWSER_RESTART_WAIT_SECONDS})")
#     a = ap.parse_args()

#     if a.browser_restarts < 0 or a.browser_restart_wait < 0:
#         sys.exit("--browser-restarts và --browser-restart-wait không được âm.")

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
#     t0, t1 = int(start.timestamp()), int(end.timestamp())
#     utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
#     utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

#     log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
#     log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")

#     try:
#         from playwright.sync_api import sync_playwright
#     except ImportError:
#         sys.exit("[PLAYWRIGHT] Chưa cài Playwright. Chạy: pip install playwright && "
#                  "python -m playwright install chromium")

#     with sync_playwright() as playwright:
#         client = TikTokBrowserClient(
#             playwright,
#             cookie_state,
#             a.browser_restarts,
#             a.browser_restart_wait,
#         )
#         try:
#             rows = keo(client, seller_id, t0, t1, not a.skip_total_check)
#         finally:
#             client.close()
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

CÁCH GỌI API — LANDING MẶC ĐỊNH KHÔNG PHẢI /order, ĐÂY LÀ CHỦ Ý:
  Script chạy fetch bên trong Chromium (Playwright) để request đi từ một trang cùng
  origin với Seller Center. Trang đó mặc định là /robots.txt.
  /order là SPA: load xong nó còn hydrate, tự gọi CHÍNH cái API này, và nạp SDK captcha
  — tất cả trên đúng main thread mà page.evaluate() cần. Máy nhiều nhân nuốt trôi nên
  không ai thấy; runner 2 vCPU của GitHub Actions thì evaluate bị bỏ đói và treo.
  Hai đường cho dữ liệu giống hệt nhau (đối chiếu 288 dòng ngày 31/08/2026).

CHỐNG TREO — BA TẦNG, ĐỪNG GỠ TẦNG NÀO:
  1. CHẾT HẲN, thoát ngay, không xoá gì:
       HTTP 401/403, code 98001002 / 98001008, bị đá sang trang login/passport.
  2. BỊ CHẶN, đập browser dựng lại tại đúng cursor:
       overlay CAPTCHA, URL verify/captcha, code 10000, context bị huỷ, evaluate quá hạn.
       Cursor chỉ tăng sau khi một trang trả thành công -> không mất, không trùng dữ liệu.
  3. TRỤC TRẶC TẠM THỜI, thử lại TẠI CHỖ, không đập browser:
       code lạ (vd 21008301 "Lỗi hệ thống"), HTTP 5xx, JS fetch lỗi mạng.
       Xếp nhóm này vào tầng 2 là sai: một blip gateway bị khuếch đại thành reload nguyên
       Chromium, mà reload mới chính là lúc dễ treo nhất.

  page.evaluate() BẮT BUỘC phải bọc asyncio.wait_for. Playwright không có timeout cho
  evaluate và set_default_timeout() KHÔNG áp vào nó; AbortController đặt trong JS cũng vô
  dụng vì setTimeout của nó nằm trên đúng main thread đang bị khoá. Gỡ lớp này ra là job
  treo tới khi CI cancel — đã xảy ra thật, treo 2 tiếng, hỏng ~11/12 run mỗi ngày.
  Script KHÔNG giải CAPTCHA: chỉ phát hiện, bỏ browser đó đi, dựng cái sạch, đi tiếp.
  Hết ngân sách restart thì DỪNG TRƯỚC bước BigQuery, dữ liệu cũ giữ nguyên.

VÍ DỤ:
  py -X utf8 keo_creator_IH.py                      # 41 ngày gần nhất
  py -X utf8 keo_creator_IH.py --date 2026-08-05 --no-bq --csv out.csv
  py -X utf8 keo_creator_IH.py --from 2026-06-29 --to 2026-07-31   # backfill
  py -X utf8 keo_creator_IH.py --last-days 3 --cookie cookie_shop2.txt

Idempotent: load vào bảng staging, rồi DELETE (id_shop x creator_username x khoảng
create_time) + INSERT trong MỘT transaction. Chạy lại bao nhiêu lần cũng không nhân đôi,
và load hỏng thì bảng thật không bị thủng. Service account cần quyền tạo/xoá bảng trong
dataset (roles/bigquery.dataEditor là đủ).

create_time ghi xuống BigQuery là UTC (không tzinfo) — đúng quy ước sẵn có của bảng,
đã đối chiếu phân bố giờ của dữ liệu cũ ngày 06/08/2026. Đừng đổi sang giờ VN.

Kéo thiếu là hỏng dữ liệu chứ không phải chạy chậm: vì nap_bq xoá cả khoảng ngày rồi ghi
lại, script sẽ DỪNG HẲN (không đụng BigQuery) nếu chạm trần MAX_PAGES mà TikTok còn báo
dữ liệu, hoặc nếu số đơn lấy được ít hơn total_count TikTok tự báo. Chỉ dùng
--skip-total-check khi đã kiểm tay và biết chắc total_count của TikTok sai.

BỘ TEST: D:\\Kho\\_lab\\creator-captcha\\test_offline.py — 14 kịch bản / 45 assert, chạy
với Seller Center giả lập, không cần cookie và không đụng BigQuery. Sửa file này xong
BẮT BUỘC chạy lại:  cd D:\\Kho\\_lab\\creator-captcha && python -X utf8 test_offline.py
"""
import argparse, asyncio, datetime, json, os, re, sys, time, urllib.parse, uuid

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

LAST_DAYS_MAC_DINH = 41

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
EVAL_TIMEOUT_S = 90.0          # trần cứng cho page.evaluate — thứ bản prod THIẾU
CANARY_TIMEOUT_S = 15.0        # renderer còn sống không, hỏi ngay sau khi launch
CLOSE_TIMEOUT_S = 20.0         # browser.close() cũng treo được khi renderer đang spin
BROWSER_SETTLE_MS = 4_000
DEADLINE_PHUT_MAC_DINH = 45

# Trang để inject cookie. Chỉ cần CÙNG ORIGIN là fetch gửi kèm cookie — không cần mở SPA.
# Mặc định KHÔNG phải /order, và đây là chủ ý: /order là SPA, sau khi load xong nó còn
# hydrate + tự gọi chính /api/fulfillment/order/list + nạp SDK captcha, tất cả trên đúng
# cái main thread mà page.evaluate() cần. Máy nhiều nhân nuốt trôi nên local không thấy;
# runner 2 vCPU của GitHub Actions thì evaluate bị bỏ đói -> "renderer treo".
# Test spa_nang_order_vs_robots chạy cùng một kịch bản với 2 landing: /order chết cả run,
# /robots.txt sạch. Dữ liệu hai đường giống hệt nhau (đối chiếu 288 dòng ngày 31/08/2026).
LANDING_MAC_DINH = "/robots.txt"

# --- ngân sách thử lại -------------------------------------------------------
BROWSER_RESTARTS = 3           # số restart LIÊN TIẾP cho phép (reset sau mỗi lần thành công)
MAX_RESTART_TONG = 12          # trần cộng dồn cả run, chống flap vô tận
BROWSER_RESTART_WAIT_SECONDS = 5
API_RETRY_TAI_CHO = 3          # thử lại tại chỗ trước khi nghĩ tới chuyện đập browser
API_RETRY_SLEEP = (3, 8, 20)
HEARTBEAT_MOI = 20             # log tiến độ mỗi N trang

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
    """Tầng 2 — cần đập browser dựng lại tại đúng cursor (CAPTCHA, renderer treo...)."""


class TrucTracTamThoi(RuntimeError):
    """Tầng 3 — thử lại tại chỗ, KHÔNG đập browser (code lạ, HTTP 5xx, fetch lỗi)."""


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
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (error) {
            return {
                status: response.status,
                error: `response không phải JSON: ${responseText.slice(0, 500)}`
            };
        }
        return {status: response.status, data};
    } catch (error) {
        return {error: error instanceof Error ? error.message : String(error)};
    } finally {
        clearTimeout(timeoutId);
    }
}
"""


class TikTokBrowserClient:
    """Gọi API trong page, phát hiện CAPTCHA và thay toàn bộ Chromium khi bị chặn.

    Không giải CAPTCHA. Bị chặn thì bỏ browser đó đi, dựng cái sạch, quay lại đúng
    search_cursor đang dở.
    """

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
        self.browser = None
        self.context = None
        self.page = None

    # ------------------------------------------------------------ tiện ích
    def _kiem_deadline(self):
        if self.deadline is not None and time.monotonic() > self.deadline:
            sys.exit(f"[DEADLINE] Quá {self.deadline_phut} phút. DỪNG, KHÔNG đụng BigQuery. "
                     f"(restart {self.tong_restart} lần, gặp CAPTCHA {self.so_lan_captcha} lần, "
                     f"renderer treo {self.so_lan_treo} lần.) "
                     f"Nếu run nào cũng chạm deadline thì giảm --last-days hoặc lấy cookie mới.")

    async def _han(self, coro, giay, mo_ta):
        """Bọc timeout Python quanh mọi lời gọi vào renderer. Đây là lớp bản prod thiếu."""
        try:
            return await asyncio.wait_for(coro, timeout=giay)
        except asyncio.TimeoutError:
            self.so_lan_treo += 1
            raise BiChan(f"{mo_ta} quá {giay}s không trả về — renderer treo")

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
        """Phát hiện chặn. KHÔNG thao tác gì lên CAPTCHA — chỉ báo để đi dựng browser mới."""
        url = (self.page.url or "").lower()
        if "login" in url or "passport" in url:
            sys.exit(f"[COOKIE] Browser bị chuyển sang trang đăng nhập: {self.page.url}. "
                     "Hãy cập nhật cookie rồi chạy lại.")
        if "verify" in url or "captcha" in url:
            self.so_lan_captcha += 1
            raise BiChan(f"URL là trang xác minh: {self.page.url}")

        # Canary TRƯỚC: nếu renderer đã chết thì query_selector cũng sẽ treo.
        await self._han(self.page.evaluate("() => 1"), self.canary_timeout, "canary evaluate")

        el = await self._han(self.page.evaluate(JS_DO_CAPTCHA, CAPTCHA_SELECTORS),
                             self.canary_timeout, "dò CAPTCHA trong DOM")
        if el is not None:
            self.so_lan_captcha += 1
            raise BiChan(f"overlay CAPTCHA đang HIỆN (không giải, dựng browser sạch) — "
                         f"<{el['tag']} id={el['id']!r} class={el['cls']!r}> "
                         f"{el['w']}x{el['h']}")

    async def _launch_browser(self):
        self.browser_number += 1
        log(f"    launch Chromium process #{self.browser_number}")
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={"width": 1440, "height": 960},
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
        )
        await self.context.add_cookies(self._playwright_cookies())
        self.page = await self.context.new_page()
        self.page.set_default_timeout(PAGE_TIMEOUT_MS)
        await self._han(
            self.page.goto(self.landing, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS),
            PAGE_TIMEOUT_MS / 1000 + 15, "page.goto")
        await asyncio.sleep(BROWSER_SETTLE_MS / 1000)
        await self._dò_captcha()
        log(f"    Chromium #{self.browser_number} sẵn sàng: {self.page.url}")

    async def _restart_browser(self, mo_ta_trang, error):
        await self._destroy_browser()
        if self.restart_count >= self.browser_restarts:
            sys.exit(
                f"[BROWSER] Đã đập và dựng lại Chromium {self.browser_restarts} lần liên tiếp "
                f"tại {mo_ta_trang} nhưng vẫn lỗi: {error}. Cursor chưa tăng và BigQuery chưa "
                f"bị thay đổi. (Gặp CAPTCHA {self.so_lan_captcha} lần — nếu con số này cao thì "
                f"cookie đang bị risk-control, cần đăng nhập tay lấy cookie mới.)"
            )
        if self.tong_restart >= self.max_restart_tong:
            sys.exit(
                f"[BROWSER] Chạm trần {self.max_restart_tong} lần restart cộng dồn cả run. "
                f"DỪNG, không đụng BigQuery. TikTok đang chặn liên tục — lấy cookie mới."
            )
        self.restart_count += 1
        self.tong_restart += 1
        log(f"  ! browser lỗi tại {mo_ta_trang}: {error}")
        log(f"    đập toàn bộ browser; giữ checkpoint cursor của page hiện tại "
            f"({self.restart_count}/{self.browser_restarts}, cộng dồn "
            f"{self.tong_restart}/{self.max_restart_tong})")
        if self.browser_restart_wait:
            await asyncio.sleep(self.browser_restart_wait)
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
                await self._restart_browser(mo_ta_trang, error)

    async def close(self):
        await self._destroy_browser()

    # ------------------------------------------------------------ gọi API
    async def _goi_mot_lan(self, url, body):
        result = await self._han(
            self.page.evaluate(FETCH_IN_PAGE,
                               {"url": url, "body": body, "timeoutMs": JS_FETCH_TIMEOUT_MS}),
            self.eval_timeout, "page.evaluate")

        if result.get("error"):
            err = str(result["error"])
            # TikTok trả thẳng trang HTML xác minh thay vì JSON -> đây là bị chặn.
            if co_dau_hieu_chan(err):
                self.so_lan_captcha += 1
                raise BiChan(f"phản hồi có dấu hiệu chặn: {err[:200]}")
            raise TrucTracTamThoi(f"JS fetch lỗi: {err[:200]}")

        status = result.get("status")
        if status in (401, 403):
            sys.exit(f"[COOKIE] Browser nhận HTTP {status} — cookie hết hạn hoặc bị "
                     "risk-control chặn. Lấy cookie mới rồi chạy lại. "
                     "(Dựng lại browser cũng không cứu được, nên dừng luôn.)")
        if status != 200:
            if isinstance(status, int) and 500 <= status < 600:
                raise TrucTracTamThoi(f"HTTP {status}")
            raise BiChan(f"HTTP status {status}")

        js = result.get("data") or {}
        code = str(js.get("code"))
        if code == "0":
            return js
        if code in ("98001002", "98001008"):
            sys.exit(f"[COOKIE] TikTok trả code {code}: {js.get('message')}. "
                     "Cookie đã hết hạn — lấy cookie mới rồi chạy lại.")
        if code == "10000":
            self.so_lan_captcha += 1
            raise BiChan(js.get("message") or "TikTok yêu cầu xác minh browser")
        # Code lạ (vd 21008301 "Lỗi hệ thống") là trục trặc phía TikTok, không phải bị
        # chặn. Bản prod đập nguyên Chromium ở đây — đắt và đúng lúc dễ dính CAPTCHA nhất.
        raise TrucTracTamThoi(f"API code={code}: {js.get('message')}")

    async def post(self, url, body, mo_ta_trang):
        lan_thu = 0
        while True:
            self._kiem_deadline()
            await self._ensure_browser(mo_ta_trang)
            try:
                js = await self._goi_mot_lan(url, body)
                # Ngân sách restart là cho một CHUỖI lỗi liên tiếp, không phải cho cả run:
                # backfill vài trăm trang dính 3 lỗi vặt cách xa nhau vẫn phải chạy tiếp.
                self.restart_count = 0
                return js
            except SystemExit:
                raise
            except TrucTracTamThoi as error:
                lan_thu += 1
                if lan_thu > API_RETRY_TAI_CHO:
                    await self._restart_browser(
                        mo_ta_trang, f"{API_RETRY_TAI_CHO} lần trục trặc liên tiếp: {error}")
                    lan_thu = 0
                    continue
                cho = API_RETRY_SLEEP[min(lan_thu - 1, len(API_RETRY_SLEEP) - 1)]
                log(f"    {mo_ta_trang}: {error} — thử lại TẠI CHỖ sau {cho}s "
                    f"({lan_thu}/{API_RETRY_TAI_CHO}, không đập browser)")
                await asyncio.sleep(cho)
            except BiChan as error:
                await self._restart_browser(mo_ta_trang, error)
                lan_thu = 0
            except Exception as error:
                await self._restart_browser(mo_ta_trang, error)
                lan_thu = 0


# ---------------------------------------------------------------- kéo dữ liệu
async def keo_mot_kenh(client, seller_id, sale_source, t0, t1, kiem_tra_total=True):
    """Trả list (main_order_id, create_time_epoch, creator_username) cho 1 kênh."""
    url = API + "?" + urllib.parse.urlencode(common_params(seller_id))
    cond = {
        "order_source": {"value": ["1"]},
        "time_order_created": {"value": [str(t0), str(t1)]},
        "sale_source": {"value": [sale_source]},
    }
    out, cursor, pages, total = [], "", 0, None
    so_don, bo_qua, lech_regex, con_tiep = 0, 0, 0, False
    while pages < MAX_PAGES:
        body = {"count": PAGE_SIZE, "offset": 0, "pagination_type": 1, "sort_info": "6",
                "search_cursor": cursor, "search_condition": {"condition_list": cond}}
        js = await client.post(url, body, f"sale_source={sale_source}, trang {pages + 1}")
        d = js.get("data") or {}
        if total is None:                      # lấy ở trang đầu: đơn mới về giữa chừng
            total = d.get("total_count")       # không được làm mốc kiểm tra nhảy số
        orders = d.get("main_orders") or []
        if not orders:
            break
        so_don += len(orders)
        for o in orders:
            try:
                oid = int(o.get("main_order_id"))
                t = int((o.get("trade_order_module") or {}).get("create_time"))
            except (TypeError, ValueError):
                bo_qua += 1                    # thiếu field -> bỏ đơn, đừng để crash cả run
                continue
            seen = set()
            for sku in (o.get("sku_module") or []):
                for it in ((sku.get("creator_info_name") or {}).get("items") or []):
                    msg = it.get("message_content") or ""
                    m = RE_NGUOI_NHAN.search(msg)
                    if m:
                        seen.add(m.group(1))
                    elif RE_LONG.search(msg):
                        lech_regex += 1
            for c in seen:
                out.append((oid, t, c))
        cursor = d.get("search_next_cursor") or ""
        pages += 1
        # Bản prod im lặng suốt lúc crawl nên nhìn log không phân biệt được "treo" với
        # "chậm". Heartbeat để lần sau đọc log là biết ngay.
        if pages % HEARTBEAT_MOI == 0:
            log(f"    ... sale_source={sale_source}: {pages} trang, {so_don} đơn")
        con_tiep = bool(d.get("search_next_has_more")) and bool(cursor)
        if not con_tiep:
            break

    log(f"  sale_source={sale_source} ({SALE_SOURCE_TEN[sale_source]}): "
        f"total_count={total}, {pages} trang, {so_don} đơn, {len(out)} cặp đơn-creator")
    if bo_qua:
        log(f"    ! bỏ {bo_qua} đơn thiếu main_order_id hoặc create_time")
    if lech_regex:
        log(f"    ({lech_regex} item có 'hoa hồng:' nhưng không phải 'Người nhận hoa hồng:' "
            f"— đã bỏ qua)")

    # Hai guard dưới đây phải là sys.exit chứ không phải log: nap_bq sẽ DELETE nguyên
    # khoảng ngày rồi INSERT lại, nên kéo thiếu mà chạy tiếp là mất dữ liệu thật.
    if con_tiep:
        sys.exit(f"[PHÂN TRANG] sale_source={sale_source} chạm trần MAX_PAGES={MAX_PAGES} "
                 f"mà TikTok vẫn báo còn dữ liệu. DỪNG, không đụng BigQuery. "
                 f"Hãy chia nhỏ khoảng --from/--to hoặc tăng MAX_PAGES.")
    try:
        total_int = int(total)
    except (TypeError, ValueError):
        total_int = None
    if kiem_tra_total and total_int is not None and so_don < total_int:
        sys.exit(f"[PHÂN TRANG] sale_source={sale_source}: TikTok báo total_count={total_int} "
                 f"nhưng chỉ lấy được {so_don} đơn. DỪNG, không đụng BigQuery. "
                 f"Nếu chắc chắn total_count của TikTok sai thì chạy lại với --skip-total-check.")
    return out


async def keo(client, seller_id, t0, t1, kiem_tra_total=True):
    rows, la = {}, set()
    for ss in ("1", "2", "3"):
        for oid, t, cu in await keo_mot_kenh(client, seller_id, ss, t0, t1, kiem_tra_total):
            if cu not in CREATORS:
                la.add(cu)
                continue
            key = (oid, cu)
            pos = SALE_SOURCE_TO_POSITION[ss]
            if key in rows:
                if rows[key]["promotion_position_type"] != pos:
                    log(f"  ! đơn {oid} creator {cu} xuất hiện ở 2 kênh "
                        f"({rows[key]['promotion_position_type']} và {pos}) — giữ kênh đầu")
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
    job = client.load_table_from_json(
        rows, stg_id,
        job_config=bigquery.LoadJobConfig(
            schema=bang_dich.schema, write_disposition="WRITE_TRUNCATE"))
    job.result()
    log(f"  staging {stg_id.split('.')[-1]}: {len(rows)} dòng")

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
        job = client.query(sql, job_config=cfg)
        job.result()
    finally:
        client.delete_table(stg_id, not_found_ok=True)
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


async def chay_crawl(cookie_state, seller_id, t0, t1, a):
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
            rows = await keo(client, seller_id, t0, t1, not a.skip_total_check)
            log(f"thống kê chặn: CAPTCHA {client.so_lan_captcha} lần, renderer treo "
                f"{client.so_lan_treo} lần, restart browser {client.tong_restart} lần")
            return rows
        finally:
            await client.close()


def main():
    ap = argparse.ArgumentParser(description="Kéo người nhận hoa hồng TikTok -> BigQuery (BẢN LAB)")
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
                    help=f"trần thời gian cả run, quá thì dừng và KHÔNG đụng BigQuery "
                         f"(mặc định {DEADLINE_PHUT_MAC_DINH}; 0 = tắt)")
    ap.add_argument("--landing", default=LANDING_MAC_DINH,
                    help=f"đường dẫn trang landing để inject cookie (mặc định "
                         f"{LANDING_MAC_DINH}). Chỉ cần cùng origin là cookie vẫn gửi. "
                         f"Đổi sang '/order' là mở lại SPA — chỉ dùng khi cần debug.")
    ap.add_argument("--headful", action="store_true",
                    help="hiện cửa sổ Chromium (xem tận mắt CAPTCHA lúc debug ở máy)")
    a = ap.parse_args()

    if a.browser_restarts < 0 or a.browser_restart_wait < 0 or a.deadline_phut < 0:
        sys.exit("--browser-restarts, --browser-restart-wait, --deadline-phut không được âm.")

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
    t0, t1 = int(start.timestamp()), int(end.timestamp())
    utc0 = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    utc1 = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    log(f"shop {seller_id} | {d0} -> {d1} (giờ VN)")
    log(f"khoảng create_time UTC sẽ ghi đè: {utc0} -> {utc1}")

    try:
        import playwright  # noqa: F401
    except ImportError:
        sys.exit("[PLAYWRIGHT] Chưa cài Playwright. Chạy: pip install playwright && "
                 "python -m playwright install chromium")

    rows = asyncio.run(chay_crawl(cookie_state, seller_id, t0, t1, a))

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

