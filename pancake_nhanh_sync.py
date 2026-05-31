"""
PANCAKE POS → NHANH.VN  —  SYNC ĐƠN + UPDATE TRẠNG THÁI
Chạy: python pancake_nhanh_sync.py

Luồng:
  Backfill lần đầu : kéo createdAt từ BACKFILL_START → hôm nay, chia theo tháng
  Incremental       : kéo updatedAt 2 ngày gần nhất — bắt đơn mới + đơn cũ đổi trạng thái
  Step 2            : so sánh status trong DB vs status Pancake vừa kéo — push lên Nhanh nếu lệch
"""

import asyncio
import aiohttp
import sqlite3
import json
import logging
import os
import sys
import time
import unicodedata
import re
import random
from datetime import date, timedelta, datetime
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


def normalize(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

PANCAKE_API_KEY   = "d21dd0c3e91a474d87634f27f898e3cc"
PANCAKE_SHOP_ID   = "120214703"
PANCAKE_BASE_URL  = "https://pos.pages.fm/api/v1"
PANCAKE_PAGE_SIZE = 200

NHANH_APP_ID       = 77571
NHANH_BUSINESS_ID  = 224108
NHANH_ACCESS_TOKEN = "Td5xd7XZOD3Nq7eEKekAE5tsdRBn2h6ZFpotZyTPHIXUBuKySfjKhrgQFqXkKZ8ItXnYWLeuKSblbIGEqIhpXPo0JAftYcFwJghL4uNoMuNawRYJIixGMx1wt6BXSnR2KsEf5tRZd4MZ"
NHANH_BASE_URL     = "https://pos.open.nhanh.vn"
NHANH_SOURCE_NAME  = "Poscake"
NHANH_DEPOT_ID     = 232863

# Map tên carrier Pancake (raw) → carrierId Nhanh.vn
CARRIER_MAP_RAW: dict[str, int] = {
    "giaohangnhanh":       5,
    "giao hang nhanh":     5,
    "ghn":                 5,
    "ghtk":                8,
    "giao hang tiet kiem": 8,
    "viettel post":        2,
    "viettelpost":         2,
    "vtp":                 2,
    "viet nam post hn":    9,
    "viet nam post":       22,
    "vnpost":              22,
    "vn post":             22,
    "j&t":                 24,
    "j&t express":         24,
    "jt express":          24,
    "jte":                 24,
    "best express":        26,
    "best":                26,
    "spx express":         29,
    "spx":                 29,
    "shopee express":      29,
    "supership":           28,
    "ahamove":             18,
    "grab express":        31,
    "grab":                31,
    "ems":                 25,
    "nhat tin":            35,
    "nhat tin logistics":  35,
    "tu van chuyen":       12,
}

# Normalized carrier map keys để so khớp chính xác hơn khi tên carrier có dấu câu / cách viết khác nhau.
CARRIER_MAP: dict[str, int] = {normalize(k): v for k, v in CARRIER_MAP_RAW.items()}

# Map partner_id từ Poscake → carrierId Nhanh.vn
# Nếu Poscake chỉ trả partner_id thay vì tên hãng, thêm mapping tại đây.
CARRIER_PARTNER_ID_MAP: dict[str, int] = {
    "1": 8,   # Giao hàng tiết kiệm (GHTK)
    "2": 25,  # EMS
    "5": 5,   # Giao hàng nhanh (GHN)
    "11": 18, # Ahamove
    "15": 24, # J&T
    "16": 26, # Best Inc
    "17": 22, # VN Post v2
    "32": 28, # SuperShip
    "37": 31, # Grab Express
}

BACKFILL_START = date.today()
INCREMENTAL_DAYS  = 3      # hôm nay + hôm qua (updatedAt), tránh miss đơn đêm

NHANH_WORKERS        = 20
PANCAKE_WORKERS      = 10
NHANH_RATE_PER_SEC   = 25
PANCAKE_RATE_PER_SEC = 10

MAX_RETRIES    = 6
RETRY_BASE_SEC = 2.0
RETRY_MAX_SEC  = 120

DB_FILE            = "sync_state.db"
PRODUCT_CACHE_FILE = "cache_products.json"
PRODUCT_EXCEL_FILE = "Nhanh_vn_Products.xlsx"   # Export từ Nhanh.vn → đặt cùng thư mục
CITY_CACHE_FILE    = "cache_cities.json"
CARRIER_SERVICE_CACHE_FILE = "cache_carrier_services.json"
LOG_FILE           = "sync.log"

# Bắt buộc đồng bộ carrier cho các đơn KHÔNG ở trạng thái "Mới" (status=0).
# Khi thiếu carrier từ Poscake hoặc carrier không dùng được trên Nhanh (vd: thiếu serviceId),
# fallback sang hãng mặc định để đảm bảo đơn có tên đơn vị vận chuyển.
FORCE_CARRIER_FOR_NON_NEW = True
FALLBACK_CARRIER_RAW      = "Tự vận chuyển"


POSCAKE_STATUS_NAME = {
    0: "Mới",
    1: "Đã xác nhận",
    2: "Đã gửi hàng",
    3: "Đã nhận",
    4: "Đang hoàn",
    5: "Đã hoàn",
    6: "Đã hủy",
    7: "Đã xóa",
    8: "Đang đóng hàng",
    9: "Chờ chuyển hàng",
    11: "Chờ hàng",
    12: "Chờ in",
    13: "Đã in",
    15: "Hoàn một phần",
    16: "Đã thu tiền",
    17: "Chờ xác nhận",
    20: "Đã đặt hàng",
}

NHANH_STATUS_NAME = {
    40: "Đã đóng gói",
    42: "Đang đóng gói",
    43: "Chờ thu gom",
    54: "Đơn mới",
    55: "Đang xác nhận",
    56: "Đã xác nhận",
    57: "Chờ khách xác nhận",
    58: "Hãng vận chuyển hủy đơn",
    59: "Đang chuyển",
    60: "Thành công",
    61: "Thất bại",
    63: "Khách hủy",
    64: "Hệ thống hủy",
    68: "Hết hàng",
    71: "Đang chuyển hoàn",
    72: "Đã chuyển hoàn",
    73: "Đổi kho xuất hàng",
    74: "Xác nhận hoàn",
}

STATUS_MAP = {
    0:  54,  # Mới                → Đơn mới
    1:  56,  # Đã xác nhận        → Đã xác nhận
    # 2 (Đã gửi hàng): Nhanh không cho set 59 "Đang chuyển" qua API (hãng vận chuyển quản lý) → chỉ lưu DB
    3:  60,  # Đã nhận            → Thành công
    # 4 và 5 (hoàn): Nhanh không cho set 71/72 qua API (do hãng vận chuyển quản lý)
    6:  63,  # Đã hủy             → Khách hủy
    7:  64,  # Đã xóa             → Hệ thống hủy
    8:  42,  # Đang đóng hàng     → Đang đóng gói
    9:  40,  # Chờ chuyển hàng    → Đã đóng gói
    11: 43,  # Chờ hàng           → Chờ thu gom
    12: 42,  # Chờ in             → Đang đóng gói
    13: 40,  # Đã in              → Đã đóng gói
    15: 74,  # Hoàn một phần      → Xác nhận hoàn
    16: 60,  # Đã thu tiền        → Thành công
    17: 57,  # Chờ xác nhận       → Chờ khách xác nhận
    20: 54,  # Đã đặt hàng        → Đơn mới
}

# Hiện tại chưa dùng để lọc. Giữ lại nếu sau này muốn tối ưu không check đơn đã kết thúc.
TERMINAL_STATUSES = {3, 5, 6, 7, 15, 16}

# Hiện tại chưa dùng. Code đang sync theo STATUS_MAP.
PUSH_ALL_MAPPED_STATUSES = True


def is_poscake_new_status(status) -> bool:
    try:
        return int(status) == 0
    except Exception:
        return False

# Map tên SP trên Pancake → product_id trên Nhanh
# Key: tên normalize (lowercase, bỏ dấu) — Value: product_id trên Nhanh (string)
PRODUCT_ALIASES: dict = {
    # Nước hoa — Pancake dùng "hương Gỗ/Biển/Hoa cỏ", Nhanh dùng "woody/aquatic/fern"
    "nuoc hoa huong go musky":               "164",
    "nuoc hoa huong bien aquatic":           "162",
    "nuoc hoa huong bien  aquatic":          "162",
    "nuoc hoa huong hoa co fern":            "163",
    # Box Yêu
    "box yeu huong go":                      "339",
    "box yeu huong bien":                    "338",
    "box yeu huong hoa co":                  "340",
    # Sữa tắm — Pancake có thêm "3 in 1", "3in1", "nam", "gội"
    "sua tam goi 3 in 1 rhys legend 350ml":  "173",
    "sua tam goi 3in1 rhys legend 350ml":    "173",
    "sua tam goi nam rhys legend 350ml":     "173",
    "sua tam goi 3in1 rhys noble new 350ml": "174",
    "sua tam goi nam rhys noble 350ml":      "174",
}

# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  TOKEN BUCKET
# ══════════════════════════════════════════════════════════════════

class TokenBucket:
    def __init__(self, rate: float):
        self.rate   = rate
        self.tokens = rate
        self.last   = time.monotonic()
        self._lock  = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now         = time.monotonic()
            self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
            self.last   = now
            if self.tokens < 1:
                await asyncio.sleep((1 - self.tokens) / self.rate)
                self.tokens = 0
            else:
                self.tokens -= 1


# ══════════════════════════════════════════════════════════════════
#  RETRY
# ══════════════════════════════════════════════════════════════════

async def with_retry(coro_fn, label: str = ""):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await coro_fn()
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                retry_after = 0
                if e.headers:
                    try: retry_after = float(e.headers.get("Retry-After", 0))
                    except: pass
                wait = min(max(retry_after, RETRY_BASE_SEC ** attempt) + random.uniform(0, 2), RETRY_MAX_SEC)
                log.warning(f"⏳ 429 [{label}] chờ {wait:.1f}s (lần {attempt}/{MAX_RETRIES})")
                await asyncio.sleep(wait)
            elif e.status >= 500:
                wait = min(RETRY_BASE_SEC ** attempt + random.uniform(0, 2), RETRY_MAX_SEC)
                log.warning(f"⚠️  {e.status} [{label}] retry {attempt}/{MAX_RETRIES} sau {wait:.1f}s")
                await asyncio.sleep(wait)
            else:
                raise
        except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError) as e:
            wait = min(RETRY_BASE_SEC ** attempt + random.uniform(0, 2), RETRY_MAX_SEC)
            log.warning(f"🔌 {type(e).__name__} [{label}] retry {attempt}/{MAX_RETRIES} sau {wait:.1f}s")
            await asyncio.sleep(wait)
    raise RuntimeError(f"Thất bại sau {MAX_RETRIES} lần retry: [{label}]")


# ══════════════════════════════════════════════════════════════════
#  SQLITE
# ══════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS synced_orders (
            pancake_id     TEXT PRIMARY KEY,
            nhanh_id       TEXT,
            pancake_status TEXT,
            carrier        TEXT DEFAULT '',
            synced_at      TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS sync_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(synced_orders)")}
    if "carrier" not in cols:
        conn.execute("ALTER TABLE synced_orders ADD COLUMN carrier TEXT DEFAULT ''")
    conn.commit()
    return conn


class SyncDB:
    def __init__(self, conn):
        self.conn  = conn
        self._lock = asyncio.Lock()

    async def is_synced(self, pid: str) -> bool:
        """Trả True chỉ khi đơn đã được tạo thành công trên Nhanh (nhanh_id không rỗng)."""
        async with self._lock:
            row = self.conn.execute(
                "SELECT nhanh_id FROM synced_orders WHERE pancake_id=?", (pid,)
            ).fetchone()
            return row is not None and row[0] != ""

    async def get_status(self, pid: str) -> Optional[str]:
        """Lấy status hiện tại trong DB của 1 đơn."""
        async with self._lock:
            row = self.conn.execute(
                "SELECT pancake_status FROM synced_orders WHERE pancake_id=?", (pid,)
            ).fetchone()
            return row[0] if row else None

    async def mark_synced(self, pid: str, nid: str, status: str = "", carrier: str = ""):
        async with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO synced_orders (pancake_id, nhanh_id, pancake_status, carrier) VALUES (?,?,?,?)",
                (pid, nid, status, carrier),
            )
            self.conn.commit()

    async def update_status_in_db(self, pid: str, new_status: str):
        async with self._lock:
            self.conn.execute(
                "UPDATE synced_orders SET pancake_status=? WHERE pancake_id=?",
                (new_status, pid),
            )
            self.conn.commit()

    async def update_carrier_in_db(self, pid: str, carrier: str):
        async with self._lock:
            self.conn.execute(
                "UPDATE synced_orders SET carrier=? WHERE pancake_id=?",
                (carrier, pid),
            )
            self.conn.commit()

    async def reset_all_status(self) -> int:
        """Reset status + carrier của tất cả đơn đã có nhanh_id → buộc Step 2 push lại.
        Trả về số đơn bị reset."""
        async with self._lock:
            cur = self.conn.execute(
                "UPDATE synced_orders SET pancake_status='', carrier='' "
                "WHERE nhanh_id IS NOT NULL AND nhanh_id != ''"
            )
            self.conn.commit()
            return cur.rowcount

    async def get_nhanh_id(self, pid: str) -> Optional[str]:
        async with self._lock:
            row = self.conn.execute(
                "SELECT nhanh_id FROM synced_orders WHERE pancake_id=?", (pid,)
            ).fetchone()
            return row[0] if row else None

    async def count(self) -> int:
        async with self._lock:
            return self.conn.execute("SELECT COUNT(*) FROM synced_orders").fetchone()[0]

    async def set_meta(self, key: str, value: str):
        async with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?,?)", (key, value)
            )
            self.conn.commit()

    async def get_all_synced(self, days: int = 21) -> list:
        """Lấy tất cả đơn đã sync có nhanh_id, trong N ngày gần nhất.
        Trả về list (pancake_id, nhanh_id, pancake_status, carrier)."""
        async with self._lock:
            return self.conn.execute(
                """
                SELECT pancake_id, nhanh_id, pancake_status, COALESCE(carrier, '')
                FROM synced_orders
                WHERE nhanh_id IS NOT NULL
                AND nhanh_id != ''
                AND synced_at >= datetime('now', ?, 'localtime')
                """,
                (f"-{days} days",)
            ).fetchall()

    async def get_meta(self, key: str) -> Optional[str]:
        async with self._lock:
            row = self.conn.execute("SELECT value FROM sync_meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def extract_carrier(order: dict) -> tuple[Optional[int], str]:
    """Trả về (carrier_id_nhanh, carrier_raw_name). carrier_raw dùng để lưu DB và so sánh thay đổi."""
    def get_str(value):
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, dict):
            for key in ("carrier_name", "name", "title", "label", "method", "service", "company", "type"):
                v = value.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            for key in ("id", "partner_id", "code", "partnerId"):
                v = value.get(key)
                if isinstance(v, (str, int, float)) and str(v).strip():
                    return str(v).strip()
        return None

    def search_fields(obj):
        if isinstance(obj, dict):
            for field in (
                "carrier_name", "carrier", "carrier_code", "carrier_id", "carrierId",
                "transport_name", "shipping_method", "shipping_method_name",
                "delivery_service", "delivery_method", "shipping_provider",
                "shipper", "partner", "partner_id"
            ):
                if field in obj:
                    raw_val = get_str(obj[field])
                    if raw_val:
                        return raw_val
            for value in obj.values():
                if isinstance(value, (dict, list)):
                    raw_val = search_fields(value)
                    if raw_val:
                        return raw_val
        elif isinstance(obj, list):
            for item in obj:
                raw_val = search_fields(item)
                if raw_val:
                    return raw_val
        return None

    raw = search_fields(order)
    if not raw:
        return None, ""

    key = normalize(raw)
    cid = CARRIER_MAP.get(key)
    if cid is None:
        cid = CARRIER_PARTNER_ID_MAP.get(raw)

    if cid is None:
        for k, v in CARRIER_MAP.items():
            if k in key or key in k:
                cid = v
                break

    if cid is None:
        # Nếu raw là partner_id mà chưa có trong CARRIER_PARTNER_ID_MAP, log rõ để thêm mapping.
        log.warning(f"  [CARRIER UNKNOWN] raw='{raw}' normalized='{key}'")
    return cid, raw


def resolve_carrier_for_sync(order: dict, carrier_service_map: dict[int, int], force_missing: bool) -> tuple[Optional[int], str, bool, str]:
    """
    Trả về carrier dùng để sync:
      - ưu tiên carrier thực tế từ Poscake (nếu có serviceId hợp lệ trên Nhanh)
      - nếu force_missing=True và carrier thực tế không sync được -> fallback hãng mặc định
    """
    carrier_id, carrier_raw = extract_carrier(order)

    if carrier_id and int(carrier_id) in carrier_service_map:
        return int(carrier_id), carrier_raw, False, ""

    if carrier_id and int(carrier_id) not in carrier_service_map:
        reason = f"carrierId={carrier_id} ('{carrier_raw}') chưa có serviceId"
    elif carrier_raw:
        reason = f"carrier '{carrier_raw}' chưa map carrierId"
    else:
        reason = "không có carrier từ Poscake"

    if not force_missing:
        return None, carrier_raw, False, reason

    fallback_id, fallback_raw = extract_carrier({"carrier_name": FALLBACK_CARRIER_RAW})
    if not fallback_id:
        return None, carrier_raw, False, reason + " | fallback chưa có trong CARRIER_MAP"

    if int(fallback_id) not in carrier_service_map:
        return None, carrier_raw, False, (
            reason + f" | fallback carrierId={fallback_id} chưa có serviceId"
        )

    return int(fallback_id), (fallback_raw or FALLBACK_CARRIER_RAW), True, reason


def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def nhanh_headers() -> dict:
    return {"Authorization": NHANH_ACCESS_TOKEN, "Content-Type": "application/json"}

def month_ranges(start: date, end: date) -> list:
    """Chia khoảng [start, end] thành các đoạn theo tháng (max 31 ngày/đoạn theo giới hạn Pancake API)."""
    ranges, cur = [], start.replace(day=1)
    while cur <= end:
        if cur.month == 12:
            month_end = cur.replace(year=cur.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            month_end = cur.replace(month=cur.month + 1, day=1) - timedelta(days=1)
        ranges.append((max(cur, start), min(month_end, end)))
        cur = (cur.replace(month=cur.month + 1, day=1) if cur.month < 12
               else cur.replace(year=cur.year + 1, month=1, day=1))
    return ranges

STOPWORDS = {"ml", "g", "set", "combo", "x", "va", "cho", "sp", "model", "pin", "sac"}

def build_product_index(product_map: dict) -> dict:
    index: dict = defaultdict(list)
    for name, pid in product_map.items():
        for w in set(name.split()) - STOPWORDS:
            if len(w) >= 3:
                index[w].append((name, pid))
    return dict(index)

def match_product(name: str, product_map: dict, product_index: dict) -> Optional[str]:
    """
    Match tên SP Pancake → product_id Nhanh theo 5 tầng ưu tiên:
    1. Alias thủ công (PRODUCT_ALIASES)
    2. Khớp chính xác sau normalize
    3. Subset match: tên Pancake là substring của tên Nhanh hoặc ngược lại
    4. Token coverage: tất cả token Pancake đều có trong tên Nhanh (best for short names)
    5. Weighted score: đếm token khớp, chọn SP có tỷ lệ cao nhất
    """
    key = normalize(name)
    if not key:
        return None

    # Tầng 1: alias thủ công
    if key in PRODUCT_ALIASES:
        return PRODUCT_ALIASES[key]

    # Tầng 2: khớp chính xác
    if key in product_map:
        return product_map[key]

    # Tầng 3: subset — tên Pancake nằm trong tên Nhanh hoặc ngược lại
    for nname, pid in product_map.items():
        if key in nname or nname in key:
            return pid

    # Tầng 4: token coverage — tất cả token Pancake đều xuất hiện trong tên Nhanh
    # "may cao rau" → tokens ["may","cao","rau"] đều có trong "may cao rau pin sac model rm001"
    kw = set(key.split()) - STOPWORDS
    if kw:
        for nname, pid in product_map.items():
            nname_tokens = set(nname.split()) - STOPWORDS
            if kw and kw.issubset(nname_tokens):
                return pid

    # Tầng 5: weighted score — chọn SP có tỷ lệ token khớp / tổng token cao nhất
    if kw:
        score: dict = defaultdict(float)
        for w in kw:
            for nname, pid in product_index.get(w, []):
                nname_tokens = set(nname.split()) - STOPWORDS
                if nname_tokens:
                    # Tỷ lệ: số token Pancake khớp / tổng token Nhanh (tránh SP dài khớp nhiều)
                    score[pid] += 1 / len(nname_tokens)
        if score:
            best = max(score, key=score.get)
            # Ngưỡng: phải khớp ít nhất 50% token Pancake
            best_nname = next(n for n, p in product_map.items() if p == best)
            best_tokens = set(best_nname.split()) - STOPWORDS
            matched = kw & best_tokens
            if best_tokens and len(matched) / len(kw) >= 0.5:
                return best

    return None

def extract_city_id(addr: str, city_map: dict) -> Optional[int]:
    if not addr:
        return None
    norm = normalize(addr)
    for key, cid in city_map.items():
        if key in norm:
            return cid
    for part in reversed(re.split(r"[,\-\|/]", addr)):
        pn = re.sub(r"^(tinh|thanh pho|tp\.?)\s*", "", normalize(part.strip())).strip()
        if pn in city_map:
            return city_map[pn]
        for key, cid in city_map.items():
            if key in pn or pn in key:
                return cid
    return None


# ══════════════════════════════════════════════════════════════════
#  NHANH: PRODUCT & CITY  (fetch + cache gộp)
# ══════════════════════════════════════════════════════════════════

async def load_products(session) -> dict:
    # Ưu tiên 1: đọc từ file Excel export của Nhanh (chính xác nhất, đủ 246 SP)
    if os.path.exists(PRODUCT_EXCEL_FILE):
        try:
            import pandas as pd
            df = pd.read_excel(PRODUCT_EXCEL_FILE)
            df.columns = [str(c).strip() for c in df.columns]
            id_col   = next(c for c in df.columns if "id" in c.lower())
            name_col = next(c for c in df.columns if "tên" in c.lower() or "ten" in c.lower() or "name" in c.lower())
            product_map = {}
            for _, row in df.iterrows():
                pid  = str(int(row[id_col]))
                name = str(row[name_col])
                if pid and name:
                    product_map[normalize(name)] = pid
            log.info(f"📦 Đọc từ Excel: {len(product_map):,} SP ({PRODUCT_EXCEL_FILE})")
            save_json(PRODUCT_CACHE_FILE, product_map)
            return product_map
        except Exception as e:
            log.warning(f"  ⚠️ Không đọc được Excel {PRODUCT_EXCEL_FILE}: {e} → fallback cache/API")

    # Ưu tiên 2: cache json
    cached = load_json(PRODUCT_CACHE_FILE, {})
    if cached:
        log.info(f"📦 Cache sản phẩm: {len(cached):,} SP")
        return cached

    log.info("📦 Tải toàn bộ sản phẩm từ Nhanh.vn...")
    product_map = {}
    last_id     = None   # dùng id offset để phân trang: lần sau lấy id < last_id
    page        = 1

    while True:
        url  = f"{NHANH_BASE_URL}/v3.0/product/list?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"
        # Nhanh product/list trả array (không có paginator object)
        # Sort desc theo id + filter id < last_id để lấy trang tiếp
        body: dict = {"paginator": {"size": 100, "sort": {"id": "desc"}}}
        if last_id:
            body["filters"] = {"idTo": last_id - 1}

        async def _do():
            async with session.post(url, json=body) as r:
                r.raise_for_status()
                return await r.json()

        data  = await with_retry(_do, f"products p{page}")
        raw   = data.get("data") or []
        items = raw if isinstance(raw, list) else (raw.get("products") or [])

        if not items:
            break

        batch_count = 0
        min_id      = None
        for p in items:
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            if pid:
                if min_id is None or pid < min_id:
                    min_id = pid
            if pid and p.get("name"):
                product_map[normalize(p["name"])] = str(pid)
            for v in (p.get("variants") or []):
                if v.get("id") and v.get("name"):
                    product_map[normalize(v["name"])] = str(v["id"])
            batch_count += 1

        log.info(f"  [SP] page={page} | +{batch_count} SP | tổng={len(product_map):,} | min_id={min_id}")

        # Nếu trả ít hơn size → đã hết data
        if batch_count < 100 or min_id is None:
            break

        last_id = min_id
        page   += 1
        await asyncio.sleep(0.1)

    log.info(f"  ✅ Tổng {len(product_map):,} sản phẩm/biến thể")
    save_json(PRODUCT_CACHE_FILE, product_map)
    return product_map


async def load_cities(session) -> dict:
    cached = load_json(CITY_CACHE_FILE, {})
    if cached:
        log.info(f"🗺️  Cache tỉnh/thành: {len(cached)} tỉnh")
        return cached

    log.info("🗺️  Tải tỉnh/thành từ Nhanh.vn...")
    url = f"{NHANH_BASE_URL}/v3.0/shipping/location?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"
    async def _do():
        async with session.post(url, json={"filters": {"locationVersion": "v1", "type": "CITY"}}) as r:
            r.raise_for_status()
            return await r.json()
    data   = await with_retry(_do, "cities")
    cities = data.get("data") or []
    if isinstance(cities, dict):
        cities = list(cities.values())

    city_map = {}
    for c in cities:
        if c.get("id") and c.get("name"):
            city_map[normalize(c["name"])] = int(c["id"])

    for alias, canon in {
        "ha noi": "ha noi", "hanoi": "ha noi", "hn": "ha noi",
        "ho chi minh": "ho chi minh", "hcm": "ho chi minh",
        "sai gon": "ho chi minh", "saigon": "ho chi minh",
        "tp hcm": "ho chi minh", "tphcm": "ho chi minh",
    }.items():
        if canon in city_map and alias not in city_map:
            city_map[alias] = city_map[canon]

    log.info(f"  ✅ {len(city_map)} tỉnh/thành")
    save_json(CITY_CACHE_FILE, city_map)
    return city_map


async def load_carrier_services(session) -> dict[int, int]:
    """
    Map carrierId -> serviceId mặc định từ API shipping/carrier.
    Nhanh yêu cầu carrier.serviceId khi update carrier.
    """
    cached_map: dict[int, int] = {}
    cached = load_json(CARRIER_SERVICE_CACHE_FILE, {})
    if isinstance(cached, dict) and cached:
        for cid, sid in cached.items():
            try:
                cached_map[int(cid)] = int(sid)
            except Exception:
                continue
        if cached_map:
            log.info(f"🚚 Cache service hãng VC: {len(cached_map)} hãng")

    log.info("🚚 Tải danh sách hãng VC từ Nhanh.vn...")
    url = f"{NHANH_BASE_URL}/v3.0/shipping/carrier?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"

    async def _do():
        async with session.post(url, json={}) as r:
            r.raise_for_status()
            return await r.json()

    try:
        data = await with_retry(_do, "shipping/carrier")
    except Exception as e:
        if cached_map:
            log.warning(f"  ⚠️ Không tải được shipping/carrier: {e} → dùng cache cũ")
            return cached_map
        raise

    carriers = data.get("data") or []
    service_map: dict[int, int] = dict(cached_map)
    missing: list[int] = []

    for c in carriers:
        cid = c.get("id")
        if not cid:
            continue
        services = c.get("services") or []
        service_id = None
        for svc in services:
            if isinstance(svc, dict) and svc.get("id"):
                service_id = int(svc["id"])
                break
        if service_id:
            service_map[int(cid)] = service_id
        else:
            missing.append(int(cid))

    save_json(CARRIER_SERVICE_CACHE_FILE, {str(k): v for k, v in service_map.items()})
    log.info(
        f"  ✅ serviceId khả dụng: {len(service_map)} hãng"
        f" | chưa có service: {len(missing)} hãng"
    )
    return service_map


# ══════════════════════════════════════════════════════════════════
#  PANCAKE: FETCH ORDERS
#
#  FIX: đổi inserted_at_from/to → createdAtFrom/To (đúng tên param Pancake API)
#  FIX: thêm use_updated_at — incremental dùng updatedAtFrom/To để bắt
#       cả đơn mới lẫn đơn cũ vừa đổi trạng thái
# ══════════════════════════════════════════════════════════════════

async def fetch_pancake_page(session, bucket, date_from, date_to, page,
                              use_updated_at: bool = False) -> tuple:
    await bucket.acquire()
    date_param = "updatedAt" if use_updated_at else "createdAt"
    url = (
        f"{PANCAKE_BASE_URL}/shops/{PANCAKE_SHOP_ID}/orders"
        f"?api_key={PANCAKE_API_KEY}"
        f"&page={page}&page_size={PANCAKE_PAGE_SIZE}"
        f"&{date_param}From={date_from}&{date_param}To={date_to}"
    )
    async def _do():
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            full        = await r.json()
            data        = full.get("data") or []
            total_pages = full.get("total_pages") or 1
            log.info(f"  [FETCH] {date_param} page={page}/{total_pages} | {date_from}→{date_to} | {len(data)} đơn")
            return data, total_pages
    return await with_retry(_do, f"pancake {date_from}~{date_to} p{page}")


async def fetch_orders_for_range(session, bucket, date_from, date_to,
                                  use_updated_at: bool = False) -> list:
    """
    Kéo tất cả đơn trong khoảng [date_from, date_to].
    Dùng updatedAt khi incremental để bắt đơn cũ đổi trạng thái.
    """
    first, _ = await fetch_pancake_page(session, bucket, date_from, date_to, 1, use_updated_at)
    if not first:
        return []

    # Field để filter in_range tương ứng với param đang dùng
    ts_field = "updated_at" if use_updated_at else "inserted_at"

    def in_range(order):
        d = (order.get(ts_field) or "")[:10]
        return str(date_from) <= d <= str(date_to)

    all_orders = [o for o in first if in_range(o)]

    if len(all_orders) < len(first):
        return all_orders

    page = 2
    while True:
        data, _ = await fetch_pancake_page(session, bucket, date_from, date_to, page, use_updated_at)
        if not data:
            break
        filtered = [o for o in data if in_range(o)]
        all_orders.extend(filtered)
        if len(filtered) < len(data):
            break
        page += 1
        await asyncio.sleep(0.1)

    log.info(f"  [DONE] {len(all_orders)} đơn | {ts_field} {date_from}→{date_to}")
    return all_orders


async def fetch_pancake_single_order(session, bucket, order_id) -> Optional[dict]:
    """Fallback: lấy 1 đơn cụ thể từ Pancake khi không có trong updatedAt window."""
    await bucket.acquire()
    url = f"{PANCAKE_BASE_URL}/shops/{PANCAKE_SHOP_ID}/orders/{order_id}?api_key={PANCAKE_API_KEY}"
    async def _do():
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            r.raise_for_status()
            return (await r.json()).get("data") or {}
    try:
        return await with_retry(_do, f"single {order_id}")
    except Exception as e:
        log.warning(f"  Không lấy được đơn {order_id}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
#  BUILD PAYLOAD
# ══════════════════════════════════════════════════════════════════

def build_create_payload(order, product_map, product_index, city_map, carrier_service_map) -> Optional[tuple]:
    order_id       = str(order.get("id", ""))
    inserted       = (order.get("inserted_at") or "")[:10]
    shipping       = order.get("shipping_address") or {}
    customer_name  = shipping.get("full_name")    or order.get("bill_full_name")    or "Khách lẻ"
    customer_phone = shipping.get("phone_number") or order.get("bill_phone_number") or ""
    full_address   = shipping.get("full_address") or shipping.get("address")        or ""
    total_price    = int(order.get("total_price") or order.get("money_to_collect")  or 0)
    city_id        = extract_city_id(full_address, city_map)
    sp_lines       = []
    products       = []
    unmatched      = []   # item có tên nhưng không tìm được product_id trong Nhanh

    for item in (order.get("items") or []):
        var   = item.get("variation_info") or {}
        name  = var.get("name") or ""
        price = int(var.get("retail_price") or 0)
        qty   = int(item.get("quantity") or 1)
        if not name:
            continue
        sp_lines.append(f"{name} x{qty}")
        pid = match_product(name, product_map, product_index)
        if pid:
            products.append({"id": int(pid), "quantity": qty, "price": price})
        else:
            unmatched.append(name)

    force_missing_carrier = FORCE_CARRIER_FOR_NON_NEW and not is_poscake_new_status(order.get("status"))
    sync_carrier_id, sync_carrier_raw, used_fallback, carrier_reason = resolve_carrier_for_sync(
        order, carrier_service_map, force_missing=force_missing_carrier
    )
    carrier_raw = sync_carrier_raw
    carrier_db_value = ""

    # Đơn không có item nào cả (đơn rỗng) VÀ total_price = 0 → bỏ qua
    # Đơn có item nhưng không match được product → vẫn tạo đơn, log cảnh báo
    if not sp_lines and total_price == 0:
        return None

    if unmatched:
        log.warning(f"  [PRODUCT MISS] {order_id}: không tìm thấy product_id cho: {unmatched}")

    # Nhanh v3 bắt buộc products[].id phải là int hợp lệ — không thể dùng id=0 hay name
    # Nếu không match được product nào → không tạo đơn, log để ops biết cần map thêm SP
    if not products:
        if unmatched:
            log.warning(f"  [NO_PRODUCT] {order_id}: không match được sản phẩm nào → bỏ qua. "
                        f"Cần thêm vào Nhanh hoặc kiểm tra cache_products.json: {unmatched}")
        return None

    # Nhanh yêu cầu SĐT VN hợp lệ: 10 số, bắt đầu 0
    # Clean: bỏ ký tự không phải số, chuẩn hóa +84 → 0
    cleaned_phone = re.sub(r"[^\d]", "", customer_phone)
    if cleaned_phone.startswith("84") and len(cleaned_phone) == 11:
        cleaned_phone = "0" + cleaned_phone[2:]
    # Nếu vẫn không hợp lệ → dùng placeholder để Nhanh không reject cả đơn
    if not (9 <= len(cleaned_phone) <= 11 and cleaned_phone.startswith("0")):
        log.warning(f"  [PHONE] {order_id}: SĐT không hợp lệ '{customer_phone}' → dùng placeholder")
        cleaned_phone = "0000000000"

    shipping_addr = {
        "name": customer_name,
        "mobile": cleaned_phone,
        "address": full_address,
        "locationVersion": "v1",
    }
    if city_id:
        shipping_addr["cityId"] = city_id

    info = {
        "type": 1,
        "depotId": NHANH_DEPOT_ID,
        "description": (" | ".join(sp_lines))[:500],
        "privateDescription": f"PCAKE:{order_id} | {inserted}",
    }
    payload = {
        "info": info,
        "channel": {
            "appOrderId": f"PCAKE_{PANCAKE_SHOP_ID}_{order_id}",
            "sourceName": NHANH_SOURCE_NAME,
        },
        "shippingAddress": shipping_addr,
        "payment": {"codAmount": total_price},
        "products": products,
    }

    if used_fallback:
        log.warning(
            f"  [CARRIER FALLBACK] {order_id}: {carrier_reason} "
            f"→ dùng '{sync_carrier_raw}' (đơn không phải trạng thái mới)"
        )

    if sync_carrier_id:
        service_id = carrier_service_map.get(int(sync_carrier_id))
        if service_id:
            payload["carrier"] = {
                "id": int(sync_carrier_id),
                "serviceId": int(service_id),
                "sendCarrierType": 1,
            }
            carrier_db_value = sync_carrier_raw
        else:
            log.warning(
                f"  [CARRIER SERVICE MISS] {order_id}: carrierId={sync_carrier_id} "
                f"('{carrier_raw}') chưa có serviceId trong shipping/carrier "
                f"→ tạo đơn không gắn hãng, Step 2 sẽ thử lại"
            )

    if force_missing_carrier and not sync_carrier_id:
        log.warning(
            f"  [CARRIER FORCE MISS] {order_id}: {carrier_reason} "
            f"→ chưa thể ép hãng cho đơn không phải trạng thái mới"
        )

    return payload, sync_carrier_raw, carrier_db_value


# ══════════════════════════════════════════════════════════════════
#  COUNTERS
# ══════════════════════════════════════════════════════════════════

@dataclass
class Counters:
    success:    int   = 0
    skipped:    int   = 0
    error:      int   = 0
    start_time: float = field(default_factory=time.monotonic)

    def print(self, prefix=""):
        elapsed = time.monotonic() - self.start_time
        rate    = (self.success + self.error) / elapsed * 60 if elapsed > 1 else 0
        log.info(f"{prefix} ✅{self.success:,} ⏭️{self.skipped:,} ❌{self.error} | ~{rate:,.0f} đơn/phút")


# ══════════════════════════════════════════════════════════════════
#  INDEX appOrderId → nhanh_id  (khôi phục id đơn đã tồn tại)
#
#  order/add trả data rỗng khi đơn "đã tồn tại" → mất nhanh_id.
#  order/list không lọc được theo appOrderId, nên quét 1 lần (lazy),
#  build map PCAKE_{shop}_{id} → nhanh_id, cache cho cả lần chạy.
# ══════════════════════════════════════════════════════════════════

_apporder_index: dict[str, str] | None = None
_apporder_lock = asyncio.Lock()


async def get_apporder_index(session, bucket) -> dict[str, str]:
    global _apporder_index
    async with _apporder_lock:
        if _apporder_index is not None:
            return _apporder_index

        idx: dict[str, str] = {}
        url = f"{NHANH_BASE_URL}/v3.0/order/list?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"
        next_id = None
        pages = 0

        while True:
            paginator: dict = {"size": 100, "sort": {"id": "desc"}}
            if next_id is not None:
                paginator["next"] = {"id": next_id}
            await bucket.acquire()

            async def _do():
                async with session.post(
                    url, json={"paginator": paginator},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                    body = await r.text()
                    r.raise_for_status()
                    return json.loads(body)

            data = await with_retry(_do, "order/list scan")
            orders = data.get("data") or []
            if not orders:
                break
            for o in orders:
                app = str((o.get("channel") or {}).get("appOrderId") or "")
                if app.startswith("PCAKE_"):
                    idx[app] = str((o.get("info") or {}).get("id") or "")
            pages += 1
            next_id = ((data.get("paginator") or {}).get("next") or {}).get("id")
            if next_id is None or pages >= 300:
                break

        _apporder_index = idx
        log.info(f"  🗂  Index Nhanh: {len(idx):,} đơn PCAKE_ (quét {pages} trang order/list)")
        return idx


# ══════════════════════════════════════════════════════════════════
#  PUSH: TẠO ĐƠN
# ══════════════════════════════════════════════════════════════════

async def push_create_one(session, bucket, sem, db, order, product_map, product_index, city_map, carrier_service_map, counters):
    order_id = str(order.get("id", ""))
    if await db.is_synced(order_id):
        counters.skipped += 1
        return

    built = build_create_payload(order, product_map, product_index, city_map, carrier_service_map)
    if built is None:
        total = order.get("total_price") or order.get("money_to_collect") or 0
        items = order.get("items") or []
        log.warning(
            f"  [SKIP] {order_id}: đơn rỗng | total={total} | "
            f"items={len(items)} | status={order.get('status')}"
        )
        counters.skipped += 1
        return

    payload, carrier_raw, carrier_db_value = built
    url = f"{NHANH_BASE_URL}/v3.0/order/add?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"

    async with sem:
        await bucket.acquire()

        async def _do():
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                body = await r.text()
                r.raise_for_status()
                return json.loads(body)

        try:
            result = await with_retry(_do, f"create {order_id}")
            if result.get("code") == 1:
                nhanh_id = str(result.get("data", {}).get("id", ""))
                # Lưu status="0" — Step 2 phát hiện lệch và push status thực
                await db.mark_synced(order_id, nhanh_id, "0", carrier_db_value)
                log.info(f"  ✅ {order_id} → Nhanh #{nhanh_id} | carrier={carrier_raw or '-'}")
                counters.success += 1
            else:
                msg = str(result.get("messages") or "")
                if "đã tồn tại" in msg or "existed" in msg.lower():
                    existing_id = str(result.get("data", {}).get("id", "")) if result.get("data") else ""
                    # order/add không trả id khi trùng → tra cứu qua index appOrderId
                    if not existing_id:
                        idx = await get_apporder_index(session, bucket)
                        existing_id = idx.get(f"PCAKE_{PANCAKE_SHOP_ID}_{order_id}", "")
                    # Không chắc carrier của đơn đã tồn tại có đúng hay chưa → để rỗng để Step 2 check/push lại.
                    await db.mark_synced(order_id, existing_id, "0", "")
                    log.warning(f"  ⚠️  {order_id}: đã tồn tại trên Nhanh (id={existing_id or '?'})")
                    counters.skipped += 1
                else:
                    log.warning(f"  ❌ {order_id}: code={result.get('code')} | {msg}")
                    counters.error += 1
        except Exception as e:
            log.error(f"  ❌ {order_id}: {type(e).__name__}: {e}", exc_info=True)
            counters.error += 1


# ══════════════════════════════════════════════════════════════════
#  PUSH: UPDATE STATUS
# ══════════════════════════════════════════════════════════════════

async def push_status_one(session, bucket, sem, db, pancake_id, nhanh_id, new_status, counters):
    nhanh_status = STATUS_MAP.get(new_status)
    poscake_name = POSCAKE_STATUS_NAME.get(new_status, "Không rõ")
    nhanh_name = NHANH_STATUS_NAME.get(nhanh_status, "Không rõ") if nhanh_status else "Không map"

    # Không có map thì không push Nhanh, chỉ lưu DB để không retry mãi
    if not nhanh_status:
        await db.update_status_in_db(pancake_id, str(new_status))
        log.warning(
            f"  [STATUS SKIP] {pancake_id}: "
            f"Poscake {new_status} - {poscake_name} chưa có trong STATUS_MAP "
            f"→ chỉ lưu DB, không push Nhanh"
        )
        counters.skipped += 1
        return

    url = f"{NHANH_BASE_URL}/v3.0/order/edit?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"
    payload = {
        "info": {
            "id": int(nhanh_id),
            "status": int(nhanh_status)
        }
    }

    async with sem:
        await bucket.acquire()

        async def _do():
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                body = await r.text()
                r.raise_for_status()
                return json.loads(body)

        try:
            result = await with_retry(_do, f"status {pancake_id}")

            if result.get("code") == 1:
                await db.update_status_in_db(pancake_id, str(new_status))
                log.info(
                    f"  ✅ Status {pancake_id}: "
                    f"Poscake {new_status} - {poscake_name} "
                    f"→ Nhanh {nhanh_status} - {nhanh_name}"
                )
                counters.success += 1
            else:
                msg = str(result.get("messages") or "")
                if "không tồn tại" in msg or "not exist" in msg.lower():
                    await db.mark_synced(pancake_id, "", "")
                    log.warning(f"  ⚠️ Status {pancake_id}: Nhanh báo đơn không tồn tại → reset DB, lần sau sẽ tạo lại đơn")
                    counters.skipped += 1
                elif (
                    "đã hoàn" in msg or "Không thể cập nhật" in msg
                    or "không hợp lệ" in msg or "hợp lệ" in msg
                ):
                    # Nhanh từ chối chuyển trạng thái (đơn đã ở trạng thái cuối/hãng VC quản lý)
                    # → lưu DB để không retry mãi mỗi lần chạy
                    await db.update_status_in_db(pancake_id, str(new_status))
                    log.warning(
                        f"  ⏭️ Status {pancake_id}: Nhanh không cho chuyển "
                        f"{nhanh_status} - {nhanh_name} (đơn đã ở trạng thái cuối / hãng VC quản lý) "
                        f"→ lưu DB, bỏ qua | {msg}"
                    )
                    counters.skipped += 1
                else:
                    log.warning(
                        f"  ❌ Status {pancake_id}: "
                        f"Poscake {new_status} - {poscake_name} "
                        f"→ Nhanh {nhanh_status} - {nhanh_name} | "
                        f"code={result.get('code')} | {msg}"
                    )
                    counters.error += 1

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                await db.mark_synced(pancake_id, "", "")
                log.warning(f"  ⚠️ Status {pancake_id}: Nhanh 404, reset DB → tạo lại lần sau")
                counters.skipped += 1
            else:
                log.error(f"  ❌ Status {pancake_id}: HTTP {e.status}", exc_info=True)
                counters.error += 1

        except Exception as e:
            log.error(f"  ❌ Status {pancake_id}: {type(e).__name__}: {e}", exc_info=True)
            counters.error += 1

async def push_carrier_one(session, bucket, sem, db, pancake_id, nhanh_id, carrier_raw, carrier_service_map, counters):
    carrier_id, _ = extract_carrier({"carrier_name": carrier_raw})
    if not carrier_id:
        log.warning(f"  [CARRIER SKIP] {pancake_id}: '{carrier_raw}' chưa có trong CARRIER_MAP")
        counters.skipped += 1
        return

    service_id = carrier_service_map.get(int(carrier_id))
    if not service_id:
        log.warning(
            f"  [CARRIER SKIP] {pancake_id}: carrierId={carrier_id} ('{carrier_raw}') "
            f"không có serviceId trong shipping/carrier"
        )
        counters.skipped += 1
        return

    url = f"{NHANH_BASE_URL}/v3.0/order/edit?appId={NHANH_APP_ID}&businessId={NHANH_BUSINESS_ID}"
    payload = {
        "info": {"id": int(nhanh_id)},
        "carrier": {
            "id": int(carrier_id),
            "serviceId": int(service_id),
            "sendCarrierType": 1,
        },
    }

    async with sem:
        await bucket.acquire()

        async def _do():
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                body = await r.text()
                r.raise_for_status()
                return json.loads(body)

        try:
            result = await with_retry(_do, f"carrier {pancake_id}")
            if result.get("code") == 1:
                await db.update_carrier_in_db(pancake_id, carrier_raw)
                log.info(
                    f"  ✅ Carrier {pancake_id}: '{carrier_raw}' "
                    f"→ carrierId={carrier_id}, serviceId={service_id}"
                )
                counters.success += 1
            else:
                msg_obj = result.get("messages")
                if isinstance(msg_obj, list):
                    msg_text = " ".join(str(x) for x in msg_obj)
                elif isinstance(msg_obj, dict):
                    msg_text = json.dumps(msg_obj, ensure_ascii=False)
                else:
                    msg_text = str(msg_obj or "")
                msg_key = normalize(msg_text)

                if (
                    "khong the cap nhat trang thai thanh cong hoac" in msg_key
                    and "hoan" in msg_key
                ):
                    await db.update_carrier_in_db(pancake_id, carrier_raw)
                    log.warning(
                        f"  ⏭️ Carrier {pancake_id}: '{carrier_raw}' "
                        f"đơn đã ở trạng thái cuối trên Nhanh → lưu DB, bỏ qua | {msg_text}"
                    )
                    counters.skipped += 1
                else:
                    log.warning(
                        f"  ❌ Carrier {pancake_id}: '{carrier_raw}' | "
                        f"code={result.get('code')} | {msg_obj}"
                    )
                    counters.error += 1
        except Exception as e:
            log.error(f"  ❌ Carrier {pancake_id}: {type(e).__name__}: {e}", exc_info=True)
            counters.error += 1


# ══════════════════════════════════════════════════════════════════
#  STEP 1: SYNC ĐƠN HÀNG
#
#  Backfill lần đầu : createdAt, chia tháng từ BACKFILL_START → hôm nay
#  Incremental      : updatedAt, 2 ngày gần nhất
#    → bắt được đơn mới (createdAt hôm qua/hôm nay) +
#      đơn cũ vừa đổi trạng thái (updatedAt trong 2 ngày)
#    → is_synced() bảo vệ không tạo duplicate
# ══════════════════════════════════════════════════════════════════

async def step_sync_orders(db, nhanh_sess, p_sess, nhanh_bucket, pancake_bucket,
                           product_map, product_index, city_map, carrier_service_map) -> tuple:
    """Trả về (Counters, list tất cả orders đã kéo) để Step 2 tái dùng."""
    today        = date.today()
    counters     = Counters()
    all_orders   = []          # tích lũy toàn bộ đơn kéo về, dùng cho Step 2
    nhanh_sem    = asyncio.Semaphore(NHANH_WORKERS)

    already_backfilled = await db.get_meta("backfill_done")

    if already_backfilled:
        start          = today - timedelta(days=INCREMENTAL_DAYS - 1)
        use_updated_at = True
        log.info(f"  ▶ Incremental (updatedAt): {start} → {today}")
    else:
        start          = BACKFILL_START
        use_updated_at = False
        log.info(f"  ▶ Backfill lần đầu (createdAt): {start} → {today}")

    months = month_ranges(start, today)

    log.info(f"\n{'═'*60}")
    log.info(f"BƯỚC 1: SYNC ĐƠN HÀNG ({len(months)} đợt)")
    log.info(f"{'═'*60}")

    for i, (m_start, m_end) in enumerate(months, 1):
        label = m_start.strftime("%Y-%m")
        log.info(f"\n  [{i}/{len(months)}] {label}: {m_start} → {m_end}")

        orders = await fetch_orders_for_range(p_sess, pancake_bucket, m_start, m_end, use_updated_at)
        log.info(f"  → {len(orders):,} đơn")
        all_orders.extend(orders)

        if orders:
            batch_c = Counters()
            await asyncio.gather(*[
                push_create_one(
                    nhanh_sess, nhanh_bucket, nhanh_sem, db,
                    order, product_map, product_index, city_map, carrier_service_map, batch_c
                )
                for order in orders
            ])
            counters.success += batch_c.success
            counters.skipped += batch_c.skipped
            counters.error   += batch_c.error
            batch_c.print(f"  [{label}]")

        await asyncio.sleep(1)

    if not already_backfilled:
        await db.set_meta("backfill_done", "1")
        log.info("  💾 Backfill xong — các lần sau dùng updatedAt 2 ngày gần nhất")

    return counters, all_orders


# ══════════════════════════════════════════════════════════════════
#  STEP 2: SYNC TRẠNG THÁI
#
#  Tối ưu: không cần gọi Pancake API từng đơn nữa.
#  Incremental đã kéo updatedAt → các đơn vừa đổi trạng thái
#  đã có trong `orders` từ Step 1.
#  Step 2 chỉ cần so sánh status trong `orders` với status trong DB
#  và push lên Nhanh những đơn lệch — không tốn thêm API call nào.
# ══════════════════════════════════════════════════════════════════

async def step_sync_status(db, nhanh_sess, p_sess, nhanh_bucket, pancake_bucket,
                           orders_from_step1: list, carrier_service_map, days: int = 21) -> Counters:
    """
    So sánh status Pancake vs DB và push lên Nhanh nếu lệch.

    Ưu tiên dùng orders_from_step1 (incremental updatedAt — đã có status mới nhất).
    Backfill: orders_from_step1 dùng createdAt nên updated_at field có thể rỗng
    → bổ sung thêm bằng cách fetch từng đơn non-terminal từ DB qua Pancake API.
    """
    counters  = Counters()
    nhanh_sem = asyncio.Semaphore(NHANH_WORKERS)
    p_sem     = asyncio.Semaphore(PANCAKE_WORKERS)

    # Map pancake_id → order dict từ Step 1
    step1_map: dict = {str(o["id"]): o for o in orders_from_step1 if o.get("id")}

    # Tuple: (pancake_id, nhanh_id, pancake_status, carrier)
    all_synced = await db.get_all_synced(days=days)
    pending    = [(pid, nid, st, car) for pid, nid, st, car in all_synced if nid]

    log.info(f"\n{'═'*60}")
    log.info(f"BƯỚC 2: SYNC TRẠNG THÁI + CARRIER")
    log.info(f"  Đơn đã sync trong DB cần kiểm tra : {len(pending):,}")
    log.info(f"  Đơn có data từ Step 1              : {len(step1_map):,}")
    log.info(f"{'═'*60}")

    if not pending:
        log.info("  ✅ Không có đơn nào cần kiểm tra")
        return counters

    async def check_and_update(pancake_id, nhanh_id, db_status, db_carrier):
        # Lấy order: ưu tiên Step 1, fallback gọi Pancake API
        order_data = step1_map.get(pancake_id)
        if order_data is None:
            async with p_sem:
                order_data = await fetch_pancake_single_order(p_sess, pancake_bucket, pancake_id)
            if not order_data:
                counters.error += 1
                return

        current_status = order_data.get("status")
        force_missing_carrier = FORCE_CARRIER_FOR_NON_NEW and not is_poscake_new_status(current_status)
        sync_carrier_id, sync_carrier_raw, used_fallback, carrier_reason = resolve_carrier_for_sync(
            order_data, carrier_service_map, force_missing=force_missing_carrier
        )
        current_carrier = sync_carrier_raw

        # Legacy fix: nếu DB đang lưu carrier nhưng carrier đó chưa có serviceId
        # (chưa sync được lên Nhanh), reset DB để tiếp tục retry ở các lần sau.

        # ── Sync carrier trước status ─────────────────────────────
        if used_fallback:
            log.warning(
                f"  [CARRIER FALLBACK] {pancake_id}: {carrier_reason} "
                f"-> fallback '{current_carrier}' (non-new status)"
            )

        if current_carrier and current_carrier != db_carrier:
            log.info(f"  [UPDATE CARRIER] {pancake_id}: '{db_carrier or '-'}' → '{current_carrier}'")
            await push_carrier_one(
                nhanh_sess, nhanh_bucket, nhanh_sem, db,
                pancake_id, nhanh_id, current_carrier, carrier_service_map, counters
            )

        # ── Sync status ───────────────────────────────────────────
        elif force_missing_carrier and not sync_carrier_id:
            log.warning(
                f"  [CARRIER FORCE MISS] {pancake_id}: {carrier_reason} "
                f"→ chưa thể ép hãng cho đơn không phải trạng thái mới"
            )

        if current_status is not None and str(current_status) != str(db_status):
            if current_status in STATUS_MAP:
                poscake_name = POSCAKE_STATUS_NAME.get(current_status, "Không rõ")
                nhanh_status = STATUS_MAP[current_status]
                nhanh_name   = NHANH_STATUS_NAME.get(nhanh_status, "Không rõ")
                log.info(
                    f"  [UPDATE] {pancake_id}: "
                    f"{db_status} → {current_status} - {poscake_name} "
                    f"→ Nhanh {nhanh_status} - {nhanh_name}"
                )
                await push_status_one(
                    nhanh_sess, nhanh_bucket, nhanh_sem, db,
                    pancake_id, nhanh_id, current_status, counters
                )
            else:
                await db.update_status_in_db(pancake_id, str(current_status))
                poscake_name = POSCAKE_STATUS_NAME.get(current_status, "Không rõ")
                log.warning(
                    f"  [STATUS SKIP] {pancake_id}: "
                    f"Poscake {current_status} - {poscake_name} chưa có trong STATUS_MAP "
                    f"→ chỉ lưu DB, không push Nhanh"
                )
                counters.skipped += 1
        elif current_status is None:
            counters.skipped += 1
        else:
            counters.skipped += 1

    await asyncio.gather(*[check_and_update(pid, nid, st, car) for pid, nid, st, car in pending])
    counters.print("  Kết quả:")
    return counters


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

async def main():
    resync = "--resync" in sys.argv
    log.info("🚀 Bắt đầu sync Pancake → Nhanh.vn" + (" [RESYNC TOÀN BỘ]" if resync else ""))

    conn = init_db()
    db   = SyncDB(conn)
    log.info(f"📋 Đã sync từ trước: {await db.count():,} đơn")

    # Resync: reset status+carrier của tất cả đơn → Bước 2 quét lại toàn bộ và push theo STATUS_MAP mới
    resync_days = 21
    if resync:
        n = await db.reset_all_status()
        resync_days = 36500   # ~100 năm: bỏ giới hạn cửa sổ thời gian, quét tất cả đơn
        log.info(f"  🔄 RESYNC: đã reset status+carrier của {n:,} đơn — sẽ push lại toàn bộ ở Bước 2")

    nhanh_bucket   = TokenBucket(NHANH_RATE_PER_SEC)
    pancake_bucket = TokenBucket(PANCAKE_RATE_PER_SEC)
    start_time     = time.monotonic()

    async with aiohttp.ClientSession(
        headers=nhanh_headers(),
        connector=aiohttp.TCPConnector(limit=80, ttl_dns_cache=300)
    ) as nhanh_sess:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
        ) as p_sess:

            product_map   = await load_products(nhanh_sess)
            city_map      = await load_cities(nhanh_sess)
            carrier_service_map = await load_carrier_services(nhanh_sess)
            product_index = build_product_index(product_map)

            # Step 1: sync đơn, trả về list orders đã kéo để Step 2 tái dùng
            c1, orders_pulled = await step_sync_orders(
                db, nhanh_sess, p_sess, nhanh_bucket, pancake_bucket,
                product_map, product_index, city_map, carrier_service_map
            )

            log.info("\n⏳ Chờ 60s để Nhanh index đơn vừa tạo...")
            await asyncio.sleep(60)

            # Step 2: sync status — ưu tiên orders Step 1, fallback gọi Pancake API cho đơn cũ
            c2 = await step_sync_status(db, nhanh_sess, p_sess, nhanh_bucket, pancake_bucket,
                                        orders_pulled, carrier_service_map, days=resync_days)

    elapsed = (time.monotonic() - start_time) / 60
    log.info(f"\n{'═'*60}")
    log.info(f"  HOÀN THÀNH — {elapsed:.1f} phút")
    log.info(f"  Bước 1 (Sync đơn)       : ✅{c1.success:,} ⏭️{c1.skipped:,} ❌{c1.error}")
    log.info(f"  Bước 2 (Sync trạng thái): ✅{c2.success:,} ⏭️{c2.skipped:,} ❌{c2.error}")
    log.info(f"{'═'*60}")
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
