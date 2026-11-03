"""TikTok Ads cấp creative theo ngày -> BigQuery fact_tiktok_ads_creative (hoặc Excel).
Giống tab Ad trên Ads Manager ("Total of N creatives"). Dùng chung cookie (env TIKTOK_COOKIE <- secret COOKIE_ADS),
key BigQuery (env GOOGLE_SERVICE_ACCOUNT_JSON) và cách gọi API của tiktok_ads_to_bq.py (đặt cùng thư mục),
đổi sang statistics/op/ad/list/ với dimensions = creative_id, mỗi ngày gọi st = et = ngày đó.

Campaign Smart+ bản mới: "Ad group" trên giao diện = ad_id trong API, "Ad" trên giao diện = creative_id trong API.
Tên cột theo giao diện: adgroup_id / adgroup_name = API ad_id / ad_name; ad_id / ad_name = API creative_id / creative_name.

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
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests
from google.cloud import bigquery

import tiktok_ads_to_bq as t

TABLE = os.environ.get("BQ_TABLE_CREATIVE",
                       "rhysman-data-warehouse-488306.rhysman_bigcampaigns.fact_tiktok_ads_creative")
DAYS = 5
EXTRA_METRICS = ["creative_platform_status", "material_primary_source", "material_secondary_source"]
# Không truyền filters = mọi trạng thái kể cả đã xóa (bộ lọc "999" của giao diện bỏ creative đã xóa).
STATUS = {**t.PLATFORM_STATUS, "8": "Deleted"}

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
] + [f for f in t.SCHEMA if f.name in t.COL.values()]  # 63 cột chỉ số: cùng tên, kiểu, mô tả với bảng campaign


RETRY_WAITS = [10, 30, 60, 120]  # giây; TikTok thỉnh thoảng trả code 1300400001 "Couldn't load" khi gọi liên tục


def post_with_retry(session, url, headers, body):
    """POST, thử lại khi lỗi tạm thời (5xx, mạng, code != 0); 401/403 hoặc hết lượt thử -> TikTokAuthError."""
    data = json.dumps(body, separators=(",", ":")).encode()
    for attempt, wait in enumerate(RETRY_WAITS + [None], 1):
        try:
            r = session.post(url, headers=headers, data=data, timeout=60)
            if r.status_code in (401, 403):
                raise t.TikTokAuthError(f"HTTP {r.status_code}")
            r.raise_for_status()
            j = r.json()
            if j.get("code") == 0:
                return j
            err = f"code={j.get('code')} msg={j.get('msg')}"
        except (requests.RequestException, ValueError) as e:
            err = f"{type(e).__name__}: {str(e)[:200]}"
        if wait is None:
            raise t.TikTokAuthError(f"{err} (sau {attempt} lần thử)")
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


def video_id(cr):
    try:
        return ", ".join(v["video_id"] for v in json.loads(cr.get("video_info") or "[]")) or None
    except (ValueError, TypeError, KeyError):
        return None


def to_bq_row(cr, day, advertiser_id):
    rd = cr["row_data"]
    missing = [k for k, *_ in t.METRICS if k not in rd]
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
        "on_off": t.OPT_STATUS.get(str(cr.get("creative_opt_status")), str(cr.get("creative_opt_status"))),
        "status": STATUS.get(rd.get("creative_platform_status"), f"Khác (mã {rd.get('creative_platform_status')})"),
    }
    row.update({t.COL[k]: t.conv(rd[k], kind) for k, kind, *_ in t.METRICS})
    return row


# ---------------------------------------------------------------- BigQuery
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
            ("on_off", "On/Off", None), ("status", "Status", None)]
    cols += [(t.COL[k], ui, fmt[kind]) for k, kind, _, ui in t.METRICS]
    vnd_cols = {t.COL[k] for k, kind, *_ in t.METRICS if kind == "vnd"}
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
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--days", type=int, default=DAYS, help=f"số ngày gần nhất tính cả hôm nay (mặc định {DAYS})")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD")
    p.add_argument("--write", action="store_true", help="ghi BigQuery (mặc định chỉ chạy thử)")
    p.add_argument("--excel", help="ghi ra file Excel này thay vì BigQuery")
    args = p.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    start, end = t.date_range(args)

    url, headers = t.api_request(t.read_cookie())
    base_body, advertiser_id = t.BASE_BODY, t.ADVERTISER_ID
    print(f"Advertiser {advertiser_id} | {start} -> {end} | bảng {TABLE}")
    session = requests.Session()
    rows, day = [], start
    try:
        while day <= end:
            creatives = fetch_all(session, url, headers, base_body, day, "creative",
                                  [m[0] for m in t.METRICS] + EXTRA_METRICS)
            camps = fetch_all(session, url, headers, base_body, day, "campaign", ["stat_cost"])
            day_rows = sorted((to_bq_row(c, day, advertiser_id) for c in creatives),
                              key=lambda r: -float(r["cost"] or 0))
            cr_cost = sum(float(r["cost"] or 0) for r in day_rows)
            camp_cost = sum(float(c["row_data"]["stat_cost"] or 0) for c in camps)
            active = sum(1 for r in day_rows if float(r["cost"] or 0) > 0)
            deleted = sum(1 for r in day_rows if r["status"] == "Deleted")
            print(f"  {day}: {len(day_rows)} creative ({deleted} đã xóa, {active} có chi phí)"
                  f" | chi phí cấp creative {cr_cost:,.0f} | cấp campaign {camp_cost:,.0f}")
            if round(cr_cost) != round(camp_cost):
                raise SystemExit(f"Chi phí ngày {day} lệch cấp campaign — dừng, không ghi")
            rows += day_rows
            day += timedelta(days=1)
    except t.TikTokAuthError as e:
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
    client = t.bq_client()
    ensure_table(client)
    deleted, inserted = replace_range(client, rows, start, end, advertiser_id)
    print(f"BigQuery: xóa {deleted} dòng cũ, chèn {inserted} dòng mới ({start} -> {end})")


if __name__ == "__main__":
    main()
