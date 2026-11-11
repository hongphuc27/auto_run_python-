"""Meta (Facebook) Ads theo từng quảng cáo, từng ngày (4 tài khoản AKA) -> BigQuery fact_facebook_ads_video (hoặc Excel).
File chạy một mình, không cần file nào khác (cài: pip install google-cloud-bigquery requests; --excel cần thêm openpyxl).

Nguồn (Marketing API, chỉ đọc):
  GET  graph.facebook.com/{VERSION}/act_{ID}/insights?level=ad&time_increment=1&fields=...   (số liệu theo ngày)
  POST graph.facebook.com/{VERSION}/ batch=[GET <ad_id>?fields=created_time,creative{body,effective_object_story_id}]
       (Batch API chỉ gộp các lệnh GET; tham số ids đã bỏ từ v26.0)

Xác thực:
  - Meta    : env META_ACCESS_TOKEN = token người dùng hệ thống (quyền ads_read, đã gán 4 tài khoản). Trên Windows nếu
              phiên chưa đặt thì đọc biến môi trường cấp người dùng.
  - BigQuery: env GOOGLE_SERVICE_ACCOUNT_JSON (JSON inline), không có thì ADC của máy (gcloud).
              Service account cần quyền BigQuery Data Editor trên dataset rhysman + BigQuery Job User.

GitHub Actions (.github/workflows/meta_ads_video_to_bq.yml, file .py để ở gốc repo):
  secret META_ACCESS_TOKEN + GOOGLE_SERVICE_ACCOUNT_JSON -> env, chạy `python meta_ads_video_to_bq.py --write`.

Mỗi lần chạy: lấy 7 ngày gần nhất (hôm nay + 6 ngày trước, giờ VN); --start / --end để chọn khoảng khác.
Mặc định CHỈ CHẠY THỬ (kéo + kiểm, in tóm tắt, không ghi gì). Ghi BigQuery phải thêm --write: xoá rồi nạp lại
khoảng ngày đó của các tài khoản vừa kéo, trong 1 transaction.

    python meta_ads_video_to_bq.py                                              # chạy thử 7 ngày gần nhất
    python meta_ads_video_to_bq.py --start 2026-11-08 --end 2026-11-10 --excel thu.xlsx
    python meta_ads_video_to_bq.py --write                                      # xoá + nạp lại 7 ngày gần nhất
    python meta_ads_video_to_bq.py --start 2026-10-01 --end 2026-11-10 --write

Số lượt video / thêm vào giỏ: Meta không trả trường khi bằng 0 -> ghi 0.
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import uuid

import requests

VERSION = "v27.0"
GRAPH = "https://graph.facebook.com"
TABLE = "rhysman-data-warehouse-488306.rhysman.fact_facebook_ads_video"
ACCOUNTS = {
    "AKA_01": "1740037856691243",
    "AKA_02": "1168303438744405",
    "AKA_05": "1431105755606260",
    "AKA_06": "27368800846154436",
}
DAYS = 7
CHUNK_DAYS = 7
VN = dt.timezone(dt.timedelta(hours=7))
INSIGHT_FIELDS = [
    "account_id", "account_name", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
    "spend", "impressions", "reach", "frequency", "cpm", "clicks", "ctr", "cpc",
    "video_6_sec_watched_actions", "video_15_sec_watched_actions", "video_30_sec_watched_actions",
    "video_thruplay_watched_actions", "actions",
]
AD_FIELDS = "created_time,creative{body,effective_object_story_id}"
# (tên cột BQ, kiểu, bắt buộc, mô tả) — đúng thứ tự trong create_fact_facebook_ads_video.sql
COLUMNS = [
    ("account_id", "STRING", True, "ID tài khoản quảng cáo"),
    ("account_name", "STRING", False, "Tên tài khoản quảng cáo"),
    ("campaign_id", "STRING", True, "ID chiến dịch"),
    ("campaign_name", "STRING", False, "Tên chiến dịch"),
    ("ad_id", "STRING", True, "ID quảng cáo"),
    ("ad_name", "STRING", False, "Tên quảng cáo"),
    ("adset_id", "STRING", False, "ID nhóm quảng cáo"),
    ("adset_name", "STRING", False, "Tên nhóm quảng cáo"),
    ("post_url", "STRING", False, "Link bài viết của quảng cáo = facebook.com/<creative.effective_object_story_id>"),
    ("created_time", "DATETIME", False, "Thời điểm tạo quảng cáo, giờ VN (API ad.created_time)"),
    ("date_start", "DATE", True, "Ngày dữ liệu (giờ tài khoản, Asia/Ho_Chi_Minh). API: date_start"),
    ("spend", "NUMERIC", False, "Số tiền đã chi tiêu (VND). API: spend"),
    ("impressions", "INT64", False, "Lượt hiển thị. API: impressions"),
    ("reach", "INT64", False, "Người tiếp cận trong ngày (không cộng các ngày để ra reach nhiều ngày). API: reach"),
    ("frequency", "FLOAT64", False, "Tần suất = impressions / reach. API: frequency"),
    ("cpm", "NUMERIC", False, "Chi phí / 1.000 lượt hiển thị (VND). API: cpm"),
    ("clicks", "INT64", False, "Lượt click (tất cả). API: clicks"),
    ("ctr", "FLOAT64", False, "CTR (tất cả), dạng phần trăm: 1.6 = 1,6%. API: ctr"),
    ("cpc", "NUMERIC", False, "CPC (tất cả) (VND). API: cpc"),
    ("video_6_sec_watched_actions", "INT64", False, "Lượt xem video 6 giây. API: video_6_sec_watched_actions"),
    ("video_15_sec_watched_actions", "INT64", False, "Lượt xem video 15 giây. API: video_15_sec_watched_actions"),
    ("video_30_sec_watched_actions", "INT64", False, "Lượt xem video 30 giây. API: video_30_sec_watched_actions"),
    ("video_thruplay_watched_actions", "INT64", False, "ThruPlay. API: video_thruplay_watched_actions"),
    ("actions_add_to_cart", "INT64", False, "Lượt thêm vào giỏ hàng. API: actions[action_type = add_to_cart]"),
    ("creative_body", "STRING", False, "Nội dung chính (văn bản) của quảng cáo. API: creative.body"),
]
INT_COLS = {c for c, t, _, _ in COLUMNS if t == "INT64"}
ZERO_IF_MISSING = {"video_6_sec_watched_actions", "video_15_sec_watched_actions", "video_30_sec_watched_actions",
                   "video_thruplay_watched_actions", "actions_add_to_cart"}
THROTTLE_CODES = {4, 17, 341, 613, 80000, 80004}
SERVER_CODES = {1, 2}
WAITS = [10, 30, 60, 120]

for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8")


class GraphError(Exception):
    def __init__(self, err):
        self.err = err
        super().__init__(f"({err.get('code')}/{err.get('error_subcode')}) {err.get('message')}")


# ---------------------------------------------------------------- Meta
def token():
    t = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not t and os.name == "nt":
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
                t = str(winreg.QueryValueEx(k, "META_ACCESS_TOKEN")[0]).strip()
        except OSError:
            pass
    if not t:
        sys.exit("Thiếu env META_ACCESS_TOKEN (token người dùng hệ thống, quyền ads_read).")
    return t


def request(method, url, **kw):
    """Gọi Graph API, thử lại khi bị giới hạn tốc độ / lỗi máy chủ tạm thời / lỗi mạng."""
    for i in range(len(WAITS) + 1):
        try:
            r = requests.request(method, url, timeout=180, **kw)
        except requests.RequestException as e:  # không in e: trong đó có URL chứa token
            if i < len(WAITS):
                print(f"  ...lỗi mạng ({type(e).__name__}), thử lại sau {WAITS[i]}s", flush=True)
                time.sleep(WAITS[i])
                continue
            raise RuntimeError(f"Không gọi được Meta sau {len(WAITS) + 1} lần ({type(e).__name__})") from None
        try:
            body = r.json()
        except ValueError:
            body = {"error": {"code": r.status_code, "message": r.text[:300]}}
        err = body.get("error") if isinstance(body, dict) else None
        if not err:
            return body
        if (err.get("code") in THROTTLE_CODES | SERVER_CODES) and i < len(WAITS):
            print(f"  ...Meta lỗi tạm ({err.get('code')}) {err.get('message', '')[:60]}, thử lại sau {WAITS[i]}s", flush=True)
            time.sleep(WAITS[i])
            continue
        if err.get("code") == 190:
            print("::error::Token Meta hết hạn hoặc đã bị thu hồi (lỗi 190): tạo token mới cho người dùng hệ thống "
                  "rồi cập nhật secret META_ACCESS_TOKEN", flush=True)
        raise GraphError(err)


def insights(tok, act, since, until):
    params = {
        "level": "ad", "time_increment": 1, "limit": 500, "use_unified_attribution_setting": "true",
        "time_range": json.dumps({"since": since, "until": until}), "fields": ",".join(INSIGHT_FIELDS),
        "access_token": tok,
    }
    body = request("GET", f"{GRAPH}/{VERSION}/act_{act}/insights", params=params)
    rows = body.get("data", [])
    nxt = body.get("paging", {}).get("next")
    while nxt:
        body = request("GET", nxt)
        rows += body.get("data", [])
        nxt = body.get("paging", {}).get("next")
    return rows


def ad_info(tok, ad_ids):
    """created_time + creative của từng quảng cáo, Batch API lô 50."""
    out, fails = {}, 0
    ids = sorted(ad_ids)
    for i in range(0, len(ids), 50):
        part = ids[i:i + 50]
        batch = json.dumps([{"method": "GET", "relative_url": f"{a}?fields={AD_FIELDS}"} for a in part])
        res = request("POST", f"{GRAPH}/{VERSION}/", data={"batch": batch, "include_headers": "false",
                                                           "access_token": tok})
        for a, x in zip(part, res):
            if x and x.get("code") == 200:
                out[a] = json.loads(x["body"])
            else:
                fails += 1
    if fails:
        print(f"  {fails}/{len(ids)} quảng cáo không đọc được created_time / creative", flush=True)
    return out


def action_sum(v, action_type=None):
    if not isinstance(v, list):
        return None
    vals = [float(x.get("value", 0)) for x in v if action_type is None or x.get("action_type") == action_type]
    return sum(vals) if vals else None


def to_datetime_vn(s):
    if not s:
        return None
    d = dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").astimezone(VN)
    return d.strftime("%Y-%m-%dT%H:%M:%S")


def build_row(r, info):
    c = info.get("creative", {})
    post = c.get("effective_object_story_id")
    row = {
        "account_id": r.get("account_id"), "account_name": r.get("account_name"),
        "campaign_id": r.get("campaign_id"), "campaign_name": r.get("campaign_name"),
        "ad_id": r.get("ad_id"), "ad_name": r.get("ad_name"),
        "adset_id": r.get("adset_id"), "adset_name": r.get("adset_name"),
        "post_url": f"https://www.facebook.com/{post}" if post else None,
        "created_time": to_datetime_vn(info.get("created_time")),
        "date_start": r.get("date_start"),
        "spend": r.get("spend"), "impressions": r.get("impressions"), "reach": r.get("reach"),
        "frequency": r.get("frequency"), "cpm": r.get("cpm"), "clicks": r.get("clicks"),
        "ctr": r.get("ctr"), "cpc": r.get("cpc"),
        "video_6_sec_watched_actions": action_sum(r.get("video_6_sec_watched_actions")),
        "video_15_sec_watched_actions": action_sum(r.get("video_15_sec_watched_actions")),
        "video_30_sec_watched_actions": action_sum(r.get("video_30_sec_watched_actions")),
        "video_thruplay_watched_actions": action_sum(r.get("video_thruplay_watched_actions")),
        "actions_add_to_cart": action_sum(r.get("actions"), "add_to_cart"),
        "creative_body": c.get("body"),
    }
    for k in ZERO_IF_MISSING:
        if row[k] is None:
            row[k] = 0
    for k in INT_COLS:
        if row[k] is not None:
            row[k] = int(round(float(row[k])))
    for k in ("frequency", "ctr"):
        if row[k] is not None:
            row[k] = float(row[k])
    for k in ("spend", "cpm", "cpc"):  # NUMERIC: giữ dạng chuỗi số để không mất chính xác
        if row[k] is not None:
            row[k] = str(row[k])
    return row


def fetch(tok, labels, start, end):
    rows = []
    for lb in labels:
        act, raw = ACCOUNTS[lb], []
        s = start
        while s <= end:
            e = min(s + dt.timedelta(days=CHUNK_DAYS - 1), end)
            raw += insights(tok, act, s.isoformat(), e.isoformat())
            s = e + dt.timedelta(days=1)
        info = ad_info(tok, {r["ad_id"] for r in raw}) if raw else {}
        part = [build_row(r, info.get(r["ad_id"], {})) for r in raw]
        spend = sum(float(x["spend"] or 0) for x in part)
        print(f"{lb} act_{act}: {len(part)} dòng, {len({x['ad_id'] for x in part})} quảng cáo, chi phí {spend:,.0f}",
              flush=True)
        if not part:
            print(f"::warning::{lb} không có dòng nào {start} → {end} (tài khoản không chạy quảng cáo?)", flush=True)
        rows += part
    return rows


def check(rows):
    """Kiểm trước khi ghi: không trùng khoá, cột bắt buộc đủ."""
    keys = [(r["ad_id"], r["date_start"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise RuntimeError("Có dòng trùng (ad_id, date_start)")
    for c, _, req, _ in COLUMNS:
        if req and any(r[c] in (None, "") for r in rows):
            raise RuntimeError(f"Cột bắt buộc {c} bị trống")
    empty = {c: sum(1 for r in rows if r[c] in (None, "")) for c, _, req, _ in COLUMNS if not req}
    print("Ô trống theo cột:", {c: n for c, n in empty.items() if n} or "không có", flush=True)


# ---------------------------------------------------------------- BigQuery
def bq_client():
    from google.cloud import bigquery
    project = TABLE.split(".")[0]
    key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not key_json:
        print("[BQ] credentials: ADC của máy (gcloud)", flush=True)
        return bigquery.Client(project=project)
    from google.oauth2 import service_account
    info = json.loads(key_json)
    print(f"[BQ] credentials: GOOGLE_SERVICE_ACCOUNT_JSON ({info.get('client_email', '?')})", flush=True)
    return bigquery.Client(project=project, credentials=service_account.Credentials.from_service_account_info(info))


def schema():
    from google.cloud import bigquery
    return [bigquery.SchemaField(c, t, mode="REQUIRED" if req else "NULLABLE", description=d) for c, t, req, d in COLUMNS]


def replace_range(client, rows, start, end, account_ids):
    """Xoá [start, end] của các tài khoản vừa kéo rồi chèn rows, trong 1 transaction."""
    from google.cloud import bigquery
    table = client.get_table(TABLE)  # bảng phải tạo trước bằng create_fact_facebook_ads_video.sql
    have, want = [f.name for f in table.schema], [c for c, *_ in COLUMNS]
    if have != want:
        raise RuntimeError(f"Schema bảng {TABLE} khác script.\n  bảng:   {have}\n  script: {want}")
    staging = f"{TABLE.rsplit('.', 1)[0]}._staging_fb_ads_video_{uuid.uuid4().hex[:8]}"
    tmp = bigquery.Table(staging, schema=schema())
    tmp.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    client.create_table(tmp)
    try:
        if rows:
            client.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(
                schema=schema(), write_disposition="WRITE_TRUNCATE")).result()
        cols = ", ".join(f"`{c}`" for c, *_ in COLUMNS)
        job = client.query(f"""
            BEGIN TRANSACTION;
            DELETE FROM `{TABLE}` WHERE account_id IN UNNEST(@acc) AND date_start BETWEEN @start AND @end;
            INSERT INTO `{TABLE}` ({cols}) SELECT {cols} FROM `{staging}`;
            COMMIT TRANSACTION;
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("acc", "STRING", account_ids),
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]))
        job.result()
        done = {c.statement_type: c.num_dml_affected_rows for c in client.list_jobs(parent_job=job.job_id)
                if c.statement_type in ("DELETE", "INSERT")}
        return done.get("DELETE", 0), done.get("INSERT", 0)
    finally:
        client.delete_table(staging, not_found_ok=True)


def write_excel(rows, path):
    import pandas as pd
    df = pd.DataFrame(rows, columns=[c for c, *_ in COLUMNS])
    for c in ("spend", "cpm", "cpc"):
        df[c] = pd.to_numeric(df[c])
    df.sort_values(["date_start", "account_id", "spend"], ascending=[True, True, False]).to_excel(path, index=False)
    print(f"Đã ghi Excel {len(df)} dòng -> {path}", flush=True)


def main():
    today = dt.datetime.now(VN).date()
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=dt.date.fromisoformat, default=today - dt.timedelta(days=DAYS - 1))
    p.add_argument("--end", type=dt.date.fromisoformat, default=today)
    p.add_argument("--accounts", nargs="+", choices=list(ACCOUNTS), default=list(ACCOUNTS))
    p.add_argument("--excel", help="xuất Excel thay vì ghi BigQuery")
    p.add_argument("--write", action="store_true", help="xoá + nạp lại khoảng ngày trên BigQuery")
    a = p.parse_args()
    if a.start > a.end:
        sys.exit("--start phải <= --end")
    print(f"Meta {VERSION} | {a.start} → {a.end} | {', '.join(a.accounts)}", flush=True)
    rows = fetch(token(), a.accounts, a.start, a.end)
    check(rows)
    if a.excel:
        write_excel(rows, a.excel)
    if not a.write:
        print(f"Chạy thử: {len(rows)} dòng, chưa ghi BigQuery (thêm --write để ghi {TABLE}).", flush=True)
        return
    deleted, inserted = replace_range(bq_client(), rows, a.start.isoformat(), a.end.isoformat(),
                                      [ACCOUNTS[lb] for lb in a.accounts])
    print(f"[BQ] {TABLE}: xoá {deleted} dòng cũ, chèn {inserted} dòng ({a.start} → {a.end}).", flush=True)


if __name__ == "__main__":
    try:
        main()
    except GraphError as e:
        sys.exit(f"::error::Meta API lỗi {e}")
