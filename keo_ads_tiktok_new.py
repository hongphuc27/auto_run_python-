#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TikTok Ads Manager — CHI PHÍ THEO CHIẾN DỊCH TỪNG NGÀY (Payment → Transactions → tab Cost) → BigQuery.

Nguồn: POST https://ads.tiktok.com/pa/api/trade/query_cost (gọi thẳng bằng cookie, không cần ký msToken/X-Bogus).
Đích : rhysman-data-warehouse-488306.rhysman.fact_cost_campaign_tiktok
       date DATE | campaigns_name STRING | amount NUMERIC (VND) | account_id STRING

VÍ DỤ:
  py -X utf8 keo_cost_campaign_tiktok.py --from 2026-04-01              # backfill từ 01/04 tới hôm nay
  py -X utf8 keo_cost_campaign_tiktok.py --last-days 3                  # chạy định kỳ
  py -X utf8 keo_cost_campaign_tiktok.py --from 2026-04-01 --no-bq --csv cost.csv   # xem thử, không ghi BQ
  py -X utf8 keo_cost_campaign_tiktok.py --last-days 3 --adv-id 7625589331687948309   # chỉ 1 tài khoản

GitHub Actions (repo auto_run_python-, file .py đặt ở gốc repo), chỉ cần cài: pip install google-cloud-bigquery
  - Cookie : env TIKTOK_COOKIE  <- secret TIKTOK_COOKIE_RHYSMAN
  - BigQuery: env GOOGLE_SERVICE_ACCOUNT_JSON <- secret cùng tên (JSON inline, không ghi ra file)
Chạy ở máy: không có env thì đọc file --cookie (mặc định cookie_ads.txt) và dùng ADC (gcloud).

Cookie chỉ cần `sessionid_ads` — dán cả chuỗi cookie cũng được, script tự lọc ra.
Phiên Ads chỉ sống ~3 ngày → hết hạn API trả code 401, script DỪNG trước khi đụng BigQuery.

Idempotent + an toàn: nạp vào bảng tạm trước, rồi trong 1 transaction xoá đúng khoảng ngày CỦA ĐÚNG
account_id đang kéo và insert lại → chạy lại không trùng, lỗi giữa chừng không để lại lỗ dữ liệu.
Kiểm tra trước khi ghi: số dòng == total_record và tổng amount == statistics.amount TikTok tự báo.
"""
import argparse, csv, datetime as dt, json, os, re, sys, time, uuid
from decimal import Decimal

import requests

API = "https://ads.tiktok.com/pa/api/trade/query_cost"
DEFAULT_ADV_IDS = ["7628879252477231124", "7625593030732709908", "7625589331687948309"]
PAGE_SIZE = 100          # server chỉ phân trang theo 100 dù xin nhiều hơn -> giữ 100 cho khớp total_page
WINDOW_DAYS = 31         # chia khoảng dài thành cửa sổ ≤31 ngày, mỗi cửa sổ tự đối chiếu tổng

ENV_COOKIE = "TIKTOK_COOKIE"   # GitHub Actions: map từ secret TIKTOK_COOKIE_RHYSMAN

BQ_PROJECT = os.environ.get("BQ_PROJECT_ID", "rhysman-data-warehouse-488306")
BQ_DATASET = os.environ.get("BQ_DATASET", "rhysman")
BQ_TABLE = os.environ.get("BQ_TABLE", "fact_cost_campaign_tiktok")

VN = dt.timezone(dt.timedelta(hours=7))
UTC = dt.timezone.utc
COLUMNS = ["date", "campaigns_name", "amount", "account_id"]


class SessionExpired(RuntimeError):
    pass


# ─── Cookie ──────────────────────────────────────────────────────────────────

def load_cookie(path):
    raw = os.environ.get(ENV_COOKIE, "").strip()
    source = f"env {ENV_COOKIE}"
    if not raw:
        if not os.path.exists(path):
            sys.exit(f"[CONFIG] env {ENV_COOKIE} trống và không thấy file cookie '{path}'.")
        raw = open(path, encoding="utf-8").read().strip()
        source = f"file {path}"
    if "^" in raw:                                   # chuỗi copy từ "Copy as cURL (cmd)"
        raw = re.sub(r"\^(.)", r"\1", raw)
    raw = re.sub(r"^(cookie:\s*)", "", raw, flags=re.I).strip().strip('"')
    m = re.search(r"(?:^|;\s*)sessionid_ads=([^;\s]+)", raw)
    if m:
        print(f"[CONFIG] cookie: sessionid_ads lấy từ {source}", flush=True)
        return f"sessionid_ads={m.group(1)}"
    if "=" not in raw:                               # dán mỗi giá trị sessionid_ads
        return f"sessionid_ads={raw}"
    sys.exit(f"[CONFIG] cookie ở {source} KHÔNG có sessionid_ads — copy lại cookie từ một request tới "
             f"ads.tiktok.com (đang đăng nhập) rồi cập nhật secret TIKTOK_COOKIE_RHYSMAN.")


# ─── TikTok API ──────────────────────────────────────────────────────────────

def day_ms(d, end=False):
    """Trang web gửi mốc NGÀY theo nửa đêm UTC (kèm timezone +7) — làm y hệt."""
    t = dt.datetime.combine(d, dt.time(23, 59, 59, 999000) if end else dt.time.min, tzinfo=UTC)
    return str(int(t.timestamp() * 1000))


def post(sess, adv_id, d_from, d_to, page, tries=5):
    body = {
        "page": page, "page_size": PAGE_SIZE,
        "filter": {"relation_type": 1, "filters": [
            {"field": "stat_time", "operator": 9, "values": [day_ms(d_from), day_ms(d_to, end=True)]}]},
        "campaign_keyword": "", "timezone": "+7", "timezone_type": 2,
        "Context": {"platform": 1, "adv_id": adv_id},
    }
    headers = {"referer": f"https://ads.tiktok.com/i18n/account/transaction?aadvid={adv_id}&tab=2"}
    for k in range(tries):
        try:
            r = sess.post(API, json=body, headers=headers, timeout=60)
            r.raise_for_status()
            j = r.json()
            code = j.get("code")
            if code == 0:
                return j["data"]
            if code == 401:
                raise SessionExpired(f"cookie Ads hết hạn/không hợp lệ (adv {adv_id}): {j.get('msg')}")
            raise RuntimeError(f"API code={code} msg={j.get('msg')}")
        except SessionExpired:
            raise
        except Exception as e:
            if k == tries - 1:
                raise
            print(f"    retry {k + 1}/{tries - 1} ({e})", flush=True)
            time.sleep(3 * (k + 1))


def fetch_window(sess, adv_id, d_from, d_to):
    rows, page, total_page, stat = [], 1, 1, None
    while page <= total_page:
        data = post(sess, adv_id, d_from, d_to, page)
        p = data.get("pagination") or {}
        if page == 1:
            total_page = int(p.get("total_page") or 0)
            total_record = int(p.get("total_record") or 0)
            stat = Decimal(str((data.get("statistics") or {}).get("amount") or 0))
        rows.extend(data.get("costs") or [])
        page += 1
        time.sleep(0.3)

    got = sum(Decimal(str(x.get("amount") or 0)) for x in rows)
    if len(rows) != total_record or got != stat:
        raise RuntimeError(f"[{adv_id}] {d_from}..{d_to} KHÔNG KHỚP: {len(rows)}/{total_record} dòng, "
                           f"tổng {got} vs TikTok báo {stat} — dừng, không ghi BQ.")
    print(f"  [{adv_id}] {d_from}..{d_to}: {len(rows)} dòng, tổng {got:,.0f} (khớp TikTok)", flush=True)
    return rows


def to_records(raw, adv_id, d_from, d_to):
    out = []
    for x in raw:
        d = dt.datetime.fromtimestamp(int(x["cost_time"]) / 1000, VN).date()
        if not (d_from <= d <= d_to):
            raise RuntimeError(f"cost_time {x['cost_time']} -> {d} nằm ngoài {d_from}..{d_to} — lệch múi giờ?")
        if x.get("currency") not in (None, "", "VND"):
            raise RuntimeError(f"[{adv_id}] gặp currency={x.get('currency')} — cột amount đang giả định VND.")
        if str(x.get("account_id") or adv_id) != adv_id:
            raise RuntimeError(f"hỏi adv {adv_id} nhưng nhận account_id={x.get('account_id')}")
        out.append({
            "date": d.isoformat(),
            "campaigns_name": (x.get("campaign_name") or "").strip(),
            "amount": str(Decimal(str(x.get("amount") or 0))),
            "account_id": adv_id,
        })
    return out


def fetch_all(cookie, adv_ids, d_from, d_to):
    sess = requests.Session()
    sess.headers.update({
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://ads.tiktok.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
        "cookie": cookie,
    })
    records = []
    for adv_id in adv_ids:
        start = d_from
        while start <= d_to:
            end = min(start + dt.timedelta(days=WINDOW_DAYS - 1), d_to)
            records += to_records(fetch_window(sess, adv_id, start, end), adv_id, start, end)
            start = end + dt.timedelta(days=1)
    return records


# ─── BigQuery ────────────────────────────────────────────────────────────────

def bq_client():
    """Giống các job khác trong repo: key file -> JSON inline GOOGLE_SERVICE_ACCOUNT_JSON -> ADC."""
    from google.cloud import bigquery
    from google.oauth2 import service_account
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if key_path and os.path.exists(key_path):
        print(f"[BQ] credentials: key file {key_path}", flush=True)
        creds = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)
    if key_json:
        try:
            info = json.loads(key_json)
        except json.JSONDecodeError as e:
            sys.exit(f"[BQ] GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}")
        print(f"[BQ] credentials: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email', '?')})", flush=True)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)
    print("[BQ] credentials: ADC của máy (gcloud)", flush=True)
    return bigquery.Client(project=BQ_PROJECT)


def load_to_bq(records, adv_ids, d_from, d_to):
    from google.cloud import bigquery
    client = bq_client()

    tbl = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    stg = f"{BQ_PROJECT}.{BQ_DATASET}._stg_{BQ_TABLE}_{uuid.uuid4().hex[:8]}"
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaigns_name", "STRING"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("account_id", "STRING"),
    ]

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{tbl}` (
          date DATE, campaigns_name STRING, amount NUMERIC, account_id STRING)
        PARTITION BY `date` CLUSTER BY account_id
        OPTIONS (description = "TikTok Ads — chi phí theo chiến dịch từng ngày (Transactions/Cost). Nguồn keo_cost_campaign_tiktok.py")
    """).result()

    # 1) nạp vào bảng tạm (tự hết hạn sau 1 giờ nếu script chết giữa chừng)
    client.load_table_from_json(records, stg, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE")).result()
    t = client.get_table(stg)
    t.expires = dt.datetime.now(UTC) + dt.timedelta(hours=1)
    client.update_table(t, ["expires"])

    # 2) xoá khoảng cũ của đúng các account + chèn bản mới trong 1 transaction
    try:
        client.query(f"""
            BEGIN TRANSACTION;
            DELETE FROM `{tbl}` WHERE account_id IN UNNEST(@acc) AND date BETWEEN @d1 AND @d2;
            INSERT INTO `{tbl}` (date, campaigns_name, amount, account_id)
              SELECT date, campaigns_name, amount, account_id FROM `{stg}`;
            COMMIT TRANSACTION;
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("acc", "STRING", adv_ids),
            bigquery.ScalarQueryParameter("d1", "DATE", d_from.isoformat()),
            bigquery.ScalarQueryParameter("d2", "DATE", d_to.isoformat()),
        ])).result()
    finally:
        client.delete_table(stg, not_found_ok=True)
    print(f"[BQ] đã thay {d_from}..{d_to} của {', '.join(adv_ids)}: {len(records)} dòng -> {tbl}", flush=True)


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="TikTok Ads cost theo chiến dịch/ngày -> BigQuery")
    ap.add_argument("--from", dest="d_from", help="YYYY-MM-DD")
    ap.add_argument("--to", dest="d_to", help="YYYY-MM-DD (bao gồm; mặc định hôm nay giờ VN)")
    ap.add_argument("--last-days", type=int, help="kéo N ngày gần nhất tính cả hôm nay (mặc định 3)")
    ap.add_argument("--adv-id", action="append", help="ID tài khoản quảng cáo (aadvid), lặp lại được")
    ap.add_argument("--cookie", default="cookie_ads.txt", help=f"file cookie khi chạy ở máy (env {ENV_COOKIE} được ưu tiên)")
    ap.add_argument("--csv", help="(tùy chọn) ghi thêm ra file CSV")
    ap.add_argument("--no-bq", action="store_true", help="KHÔNG ghi BigQuery")
    a = ap.parse_args()

    today = dt.datetime.now(VN).date()
    d_to = dt.date.fromisoformat(a.d_to) if a.d_to else today
    if a.d_from:
        d_from = dt.date.fromisoformat(a.d_from)
    else:
        d_from = d_to - dt.timedelta(days=(a.last_days or 3) - 1)
    if d_from > d_to:
        sys.exit(f"[CONFIG] --from {d_from} sau --to {d_to}")

    env_ids = [s.strip() for s in os.environ.get("TT_ADV_IDS", "").split(",") if s.strip()]
    adv_ids = a.adv_id or env_ids or DEFAULT_ADV_IDS
    print(f"[CONFIG] {d_from} -> {d_to} | account: {', '.join(adv_ids)}", flush=True)

    cookie = load_cookie(a.cookie)
    try:
        records = fetch_all(cookie, adv_ids, d_from, d_to)
    except SessionExpired as e:
        sys.exit(f"[COOKIE] {e}\n  -> Đăng nhập ads.tiktok.com, copy lại cookie (có sessionid_ads) "
                 f"vào secret TIKTOK_COOKIE_RHYSMAN rồi chạy lại.")

    total = sum(Decimal(r["amount"]) for r in records)
    print(f"[DATA] {len(records)} dòng | tổng {total:,.0f} VND", flush=True)

    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(records)
        print(f"[CSV] -> {a.csv}", flush=True)

    if a.no_bq:
        print("[BQ] bỏ qua (--no-bq).", flush=True)
        return
    if not records:
        print("[BQ] không có dòng chi phí nào trong khoảng — giữ nguyên BigQuery, không xoá gì.", flush=True)
        return
    load_to_bq(records, adv_ids, d_from, d_to)


if __name__ == "__main__":
    main()
