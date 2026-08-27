from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from google.cloud import bigquery

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

HERE = Path(__file__).resolve().parent
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

TABLE = "rhysman-data-warehouse-488306.rhysman.fact_ads_shopee"
ENDPOINT = "https://nhanh.vn/ecommerce/manage/reportads"
SHOPEE_APP_ID = 8195                    # nhanh trả nhiều sàn; bảng này chỉ Shopee
VAT_RATE = 0.08                         # bảng hiện tại: vat = amount * 8% ở mọi dòng
NHANH_DATA_STARTS = date(2026, 5, 1)    # trước mốc này nhanh không có dữ liệu

# Đúng thứ tự và kiểu của bảng trên BQ. Lệch là dừng, không ghi.
SCHEMA = [("shopId", "STRING"), ("date", "DATE"),
          ("amount", "FLOAT"), ("vat", "FLOAT")]


def fail(msg: str):
    raise SystemExit(msg)


def today_vn() -> date:
    """Giờ VN, không phải giờ máy — Cloud Run chạy UTC."""
    return datetime.now(VN_TZ).date()


# ── Cấu hình ────────────────────────────────────────────────────────────────

def load_dotenv() -> None:
    """Nạp .env nếu có. Trên Cloud Run không có file này, hàm tự bỏ qua."""
    env = HERE / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8-sig").splitlines():
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("\"'"))


# Cookie phiên đăng nhập nhanh.vn, để thẳng trong code cho khỏi phải khai secret.
# ĐỔI CHỖ NÀY KHI COOKIE HẾT HẠN, rồi commit lại.
# Lấy lại: đăng nhập nhanh.vn > F12 > Network > bấm 1 request bất kỳ sang nhanh.vn
#          > Request Headers > copy toàn bộ giá trị header "cookie".
# Bắt buộc phải chứa Npos-Csrf-Token-V1=...
COOKIE_MAC_DINH = "_ga=GA1.1.657311034.1780478512; _gcl_au=1.1.100242684.1780478512; _tt_enable_cookie=1; _ttp=01KT6CM33ZZCSVNM1DHRR88N6V_.tt.1; _fbp=fb.1.1780478512657.37388055819271719; posUIver=v3; nvnKn0x6mr3=5icdefk2gv20t8h72b8cl82gr1; Npos-Csrf-Token-V1=XERv53ZlZx2AV6s8zARKWLfR2RGJ9cBizMDv93rl9hhHi8AmbRP0NqLxaLhF2xlEztoOmxQL6yiVjCw2zdaNHpj2U5ukHpJRzHBxU4eukAPL1nSTF3paiCjWysEIMpjFhTrIfxyNU4B4G6O6iUfhn9x7ya1Oobd5XJmIY0q6F17EHulRUCeHXyD70ug2OhqfDm2e0IzZ7DGH1Y; isSignin=1; _ga_V1DEQFVWF2=GS2.1.s1782301117$o3$g0$t1782301117$j60$l0$h0; ttcsid_CAV5POJC77U5NQUHH8NG=1786189300032::KYn5HErjQ5MlQLcPoSrM.65.1786189328150.0; ttcsid=1786189300033::pA1RIiC5mrBni_bbsw5N.65.1786189328151.0::0.27218.28118::1398.2.307.399::0.0.0; _ga_5SRHFYM711=GS2.1.s1786189299$o117$g1$t1786191450$j60$l0$h674799338"


def read_cookie() -> str:
    """Biến môi trường thắng, không có mới dùng chuỗi hardcode ở trên.

    Để sau này muốn chuyển sang GitHub Secrets thì chỉ cần khai secret
    NHANH_COOKIE, không phải sửa code.
    """
    cookie = (os.getenv("NHANH_COOKIE") or "").strip() or COOKIE_MAC_DINH.strip()
    if not cookie:
        fail("Không có cookie. Sửa COOKIE_MAC_DINH trong file này, "
             "hoặc khai biến môi trường NHANH_COOKIE.")
    return cookie


def build_request(cookie: str, a: date, b: date) -> dict:
    """Dựng request cho một khoảng ngày. Csrf token nằm sẵn trong cookie."""
    m = re.search(r"Npos-Csrf-Token-V1=([^;]+)", cookie, re.I)
    if not m:
        fail("Cookie không chứa Npos-Csrf-Token-V1 — phiên đăng nhập không hợp lệ.")
    biz = os.getenv("NHANH_BUSINESS_ID", "224108")
    dates = f"fromDate={a.isoformat()}&toDate={b.isoformat()}"
    return {
        "url": f"{ENDPOINT}?businessId={biz}",
        "headers": {
            "accept": "application/json, text/plain, */*",
            "app-version": "3",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "npos-csrf-token-v1": m.group(1),
            "origin": "https://nhanh.vn",
            "referer": f"{ENDPOINT}?{dates}&businessId={biz}",
            "cookie": cookie,
        },
        "body": f"{dates}&businessId={biz}",
    }


# ── Nguồn nhanh.vn ──────────────────────────────────────────────────────────

def call_nhanh(req: dict) -> dict:
    r = requests.post(req["url"], headers=req["headers"],
                      data=req["body"].encode("utf-8"),
                      allow_redirects=False, timeout=90)
    if r.status_code in (301, 302, 303, 307, 308):
        loc = r.headers.get("Location", "")
        fail("COOKIE HẾT HẠN — nhanh.vn đá về login. Cập nhật NHANH_COOKIE."
             if "login" in loc.lower() else f"Bị redirect tới {loc}")
    if r.status_code != 200:
        fail(f"HTTP {r.status_code}\n{r.text[:500]}")
    if "json" not in r.headers.get("Content-Type", ""):
        fail("Server không trả JSON — nhiều khả năng cookie/csrf đã hết hạn.")
    data = r.json()
    if data.get("code") != 1:
        fail(f"nhanh báo lỗi: code={data.get('code')} "
             f"errorCode={data.get('errorCode')} {data.get('messages')}")
    return data


def flatten(payload: dict, year: int, month: int) -> list[dict]:
    """data[] > shopInfors[] > dateValues[] -> dòng phẳng.

    API chỉ trả ngày trong tháng ("01".."31"), year/month lấy từ chunk đang gọi.
    """
    rows = []
    for app in payload.get("data") or []:
        if app.get("appId") != SHOPEE_APP_ID:
            continue
        for shop in app.get("shopInfors") or []:
            for dv in shop.get("dateValues") or []:
                try:
                    d = date(year, month, int(str(dv.get("date")).strip()))
                except (TypeError, ValueError):
                    continue
                rows.append({"shopId": str(shop.get("shopId")), "date": d,
                             "amount": float(dv.get("amount") or 0)})
    return rows


def month_chunks(a: date, b: date) -> list[tuple[date, date]]:
    """Cắt theo tháng vì API không trả tháng/năm, phải tự gắn vào."""
    out, y, m = [], a.year, a.month
    while (y, m) <= (b.year, b.month):
        last = date(y, m, calendar.monthrange(y, m)[1])
        out.append((max(date(y, m, 1), a), min(last, b)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def pull(cookie: str, a: date, b: date) -> pd.DataFrame:
    rows: list[dict] = []
    for ca, cb in month_chunks(a, b):
        rows.extend(flatten(call_nhanh(build_request(cookie, ca, cb)), ca.year, ca.month))
    if not rows:
        return pd.DataFrame(columns=["shopId", "date", "amount", "vat"])
    df = pd.DataFrame(rows)
    df = df[(df["date"] >= a) & (df["date"] <= b)]
    df = df.groupby(["shopId", "date"], as_index=False)["amount"].sum()
    df["vat"] = (df["amount"] * VAT_RATE).round(6)
    return df.sort_values(["date", "shopId"]).reset_index(drop=True)


# ── BigQuery ────────────────────────────────────────────────────────────────

def bq_client() -> bigquery.Client:
    project = os.getenv("BIGQUERY_PROJECT_ID", TABLE.split(".")[0])

    # GitHub Actions: nội dung file .json nằm thẳng trong biến môi trường,
    # không có file nào trên đĩa để mà trỏ tới.
    raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        try:
            info = json.loads(raw)
        except json.JSONDecodeError as e:
            fail(f"GOOGLE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ: {e}. "
                 "Secret phải là nguyên nội dung file service account, cả dấu { }.")
        return bigquery.Client.from_service_account_info(info, project=project)

    # Máy cá nhân: trỏ tới đường dẫn file .json
    key = os.getenv("BIGQUERY_KEY_PATH")
    if key and Path(key).exists():
        return bigquery.Client.from_service_account_json(key, project=project)

    return bigquery.Client(project=project)     # Cloud Run: service account gắn sẵn


def check_schema(client: bigquery.Client) -> None:
    actual = [(f.name, f.field_type) for f in client.get_table(TABLE).schema]
    if actual != SCHEMA:
        fail(f"SCHEMA BẢNG ĐÃ THAY ĐỔI — dừng để khỏi ghi sai.\n"
             f"  kỳ vọng: {SCHEMA}\n  thực tế: {actual}")


def read_bq(client: bigquery.Client, a: date, b: date) -> pd.DataFrame:
    """Đọc dữ liệu đang có, gộp trùng để so cho công bằng."""
    sql = f"""
        SELECT shopId, date, ANY_VALUE(amount) AS amount, COUNT(*) AS n_rows
        FROM `{TABLE}` WHERE date BETWEEN @a AND @b
        GROUP BY shopId, date
    """
    df = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("a", "DATE", a),
        bigquery.ScalarQueryParameter("b", "DATE", b),
    ])).to_dataframe()
    if df.empty:
        return pd.DataFrame(columns=["shopId", "date", "amount", "n_rows"])
    df["shopId"] = df["shopId"].astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def delete_range(client: bigquery.Client, a: date, b: date) -> int:
    job = client.query(f"DELETE FROM `{TABLE}` WHERE date BETWEEN @a AND @b",
                       job_config=bigquery.QueryJobConfig(query_parameters=[
                           bigquery.ScalarQueryParameter("a", "DATE", a),
                           bigquery.ScalarQueryParameter("b", "DATE", b),
                       ]))
    job.result()
    return job.num_dml_affected_rows or 0


def insert(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Load job chứ không streaming insert, để lần sau DELETE không bị chặn."""
    out = df[["shopId", "date", "amount", "vat"]].copy()
    out["date"] = pd.to_datetime(out["date"])
    client.load_table_from_dataframe(out, TABLE, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(n, t) for n, t in SCHEMA])).result()


def compare(new: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    m = new[["shopId", "date", "amount"]].merge(
        old[["shopId", "date", "amount"]], on=["shopId", "date"],
        how="outer", suffixes=("_new", "_old")).fillna({"amount_new": 0, "amount_old": 0})
    m["chenh"] = m["amount_new"] - m["amount_old"]
    m = m.sort_values(["date", "shopId"])

    print(f"\n  {'ngày':<12} {'shopId':<11} {'NHANH':>16} {'BQ':>16} {'chênh':>15}")
    print("  " + "-" * 74)
    for r in m.itertuples():
        print(f"  {str(r.date):<12} {r.shopId:<11} {r.amount_new:>16,.0f} "
              f"{r.amount_old:>16,.0f} {r.chenh:>15,.0f}{'' if r.chenh == 0 else '  *'}")
    print("  " + "-" * 74)
    print(f"  {'TỔNG':<24} {m['amount_new'].sum():>16,.0f} "
          f"{m['amount_old'].sum():>16,.0f} {m['chenh'].sum():>15,.0f}")
    return m


# ── Main ────────────────────────────────────────────────────────────────────

def resolve_range(args) -> tuple[date, date]:
    if args.days < 1:
        fail("--days phải >= 1")
    today = today_vn()
    end = (datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date
           else (today - timedelta(days=1) if args.exclude_today else today))
    start = end - timedelta(days=args.days - 1)

    if end > today:
        fail(f"TỪ CHỐI: {end} là ngày tương lai.")
    if start < NHANH_DATA_STARTS:
        fail(f"TỪ CHỐI: {start} sớm hơn {NHANH_DATA_STARTS} — nhanh.vn không có dữ "
             "liệu giai đoạn đó, chạy tiếp sẽ xoá lịch sử BQ mà không nạp lại được.")
    return start, end


def main() -> None:
    p = argparse.ArgumentParser(description="Kéo ads Shopee từ nhanh.vn -> BigQuery")
    p.add_argument("--days", type=int, default=int(os.getenv("DAYS", "3")))
    p.add_argument("--end-date", default=None, help="YYYY-MM-DD (mặc định hôm nay)")
    p.add_argument("--exclude-today", action="store_true", help="Chỉ lấy ngày đã chốt")
    p.add_argument("--dry-run", action="store_true", help="Chỉ so sánh, không ghi BQ")
    args = p.parse_args()

    load_dotenv()
    start, end = resolve_range(args)
    today = today_vn()

    print("=" * 78)
    print(f"  Khoảng xử lý : {start} -> {end}  ({args.days} ngày, giờ VN)")
    print(f"  Bảng đích    : {TABLE}")
    print(f"  Chế độ       : {'DRY-RUN (không ghi)' if args.dry_run else 'GHI THẬT (xoá rồi nạp lại)'}")
    print("=" * 78)

    # 1. Kéo
    print("\n[1/5] Kéo dữ liệu từ nhanh.vn ...")
    new = pull(read_cookie(), start, end)
    if new.empty:
        fail("TỪ CHỐI: nhanh.vn trả về 0 dòng. Không xoá gì cả.")
    print(f"      {len(new)} dòng | {new['date'].nunique()} ngày | "
          f"{new['shopId'].nunique()} shop | tổng {new['amount'].sum():,.0f} VND")
    for d, sub in new.groupby("date"):
        tag = "  <- hôm nay, số chưa chốt" if d == today else ""
        print(f"        {d}  {len(sub)} shop  {sub['amount'].sum():>14,.0f} VND{tag}")

    # DELETE quét cả khoảng nên ngày nào nhanh không trả về sẽ bị xoá khỏi BQ
    missing = [start + timedelta(days=i) for i in range((end - start).days + 1)
               if (start + timedelta(days=i)) not in set(new["date"])]
    if missing:
        print(f"      ! nhanh.vn không có {len(missing)} ngày: "
              + ", ".join(map(str, missing)) + " -> sẽ bị xoá khỏi BQ")

    # 2. Schema
    print("\n[2/5] Kiểm tra schema bảng đích ...")
    client = bq_client()
    check_schema(client)
    print(f"      OK — {', '.join(f'{n} {t}' for n, t in SCHEMA)}")

    # 3. So sánh trước khi ghi
    print("\n[3/5] So sánh dữ liệu ĐANG CÓ trên BQ với dữ liệu vừa kéo:")
    old = read_bq(client, start, end)
    dups = int((old["n_rows"] > 1).sum())
    if dups:
        print(f"      ! {dups} khoá đang trùng dòng trên BQ — nạp lại sẽ dọn luôn.")
    diff = compare(new, old)
    n_lech = int((diff["chenh"] != 0).sum())
    print(f"\n      => {'KHỚP HOÀN TOÀN' if n_lech == 0 else 'CÓ LỆCH'} "
          f"({n_lech}/{len(diff)} khoá lệch)")

    if args.dry_run:
        print("\n[dry-run] Dừng ở đây, không ghi gì lên BigQuery.")
        return

    # 4. Xoá rồi nạp
    print(f"\n[4/5] Xoá {start} -> {end} rồi nạp lại ...")
    print(f"      đã xoá {delete_range(client, start, end)} dòng")
    insert(client, new)
    print(f"      đã nạp {len(new)} dòng")

    # 5. Đọc lại kiểm chứng
    print("\n[5/5] Đọc lại từ BQ để kiểm chứng ...")
    back = read_bq(client, start, end)
    chk = new.merge(back[["shopId", "date", "amount", "n_rows"]],
                    on=["shopId", "date"], how="outer", suffixes=("_gui", "_bq"))
    bad_amount = chk[(chk["amount_gui"].fillna(-1) - chk["amount_bq"].fillna(-1)).abs() > 0.01]
    bad_dup = chk[chk["n_rows"].fillna(0) > 1]
    print(f"      dòng trên BQ  : {len(back)} (kỳ vọng {len(new)})")
    print(f"      lệch số tiền  : {len(bad_amount)}")
    print(f"      khoá bị trùng : {len(bad_dup)}")
    if len(bad_amount) or len(bad_dup) or len(back) != len(new):
        print(bad_amount.head(10).to_string())
        fail("KIỂM CHỨNG THẤT BẠI — BQ không khớp bản vừa gửi. Chạy lại để vá.")
    print("      OK — BQ khớp 100% với dữ liệu vừa kéo.")

    print("\n" + "=" * 78)
    print(f"  XONG. {len(new)} dòng cho {start} -> {end}, "
          f"tổng {new['amount'].sum():,.0f} VND")
    print("=" * 78)


if __name__ == "__main__":
    main()
