import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
from flask import Flask, jsonify, request, send_from_directory


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(APP_DIR, "data", "billing_cache.db"))
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "1440"))
PORT = int(os.environ.get("PORT", "8083"))

NINJA_CLIENT_ID = os.environ.get("NINJA_CLIENT_ID", "")
NINJA_CLIENT_SECRET = os.environ.get("NINJA_CLIENT_SECRET", "")
NINJA_API_BASE = os.environ.get("NINJA_API_BASE", "https://app.ninjarmm.com")
NINJA_TOKEN_PATH = os.environ.get("NINJA_TOKEN_PATH", "/ws/oauth/token")
NINJA_DEVICES_PATH = os.environ.get("NINJA_DEVICES_PATH", "/v2/devices-detailed")
NINJA_OAUTH_SCOPE = os.environ.get("NINJA_OAUTH_SCOPE", "monitoring")

SOPHOS_CLIENT_ID = os.environ.get("SOPHOS_CLIENT_ID", "")
SOPHOS_CLIENT_SECRET = os.environ.get("SOPHOS_CLIENT_SECRET", "")
SOPHOS_TOKEN_URL = os.environ.get(
    "SOPHOS_TOKEN_URL",
    "https://id.sophos.com/api/v2/oauth2/token",
)

REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = float(os.environ.get("RETRY_DELAY_SECONDS", "1.5"))
RECENT_DEVICE_CUTOFF_DAYS_DEFAULT = int(
    os.environ.get("RECENT_DEVICE_CUTOFF_DAYS", os.environ.get("SOPHOS_RECENTLY_ONLINE_DAYS", "30"))
)
ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT = (os.environ.get("ENABLE_RECENT_DEVICE_CUTOFF", "true").strip().lower() == "true")


app = Flask(__name__, static_folder="assets")
db_lock = threading.Lock()
sync_lock = threading.Lock()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] %(levelname)s %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger("billing-portal")

runtime_state = {
    "sync_running": False,
    "last_sync_started_at": None,
    "last_sync_finished_at": None,
    "last_sync_status": "never",
    "last_sync_error": None,
    "last_sync_details": {},
}

LEGAL_SUFFIX_STOPWORDS = {
    "ltd",
    "limited",
    "llc",
    "inc",
    "corp",
    "co",
    "plc",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_utc(raw: str):
    if not raw:
        return None
    try:
        # Handle trailing Z from API timestamps.
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None


def parse_timestamp_utc(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
        if value > 1_000_000_000_000:
            value = value / 1000.0
        if value <= 0:
            return None
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        parsed_iso = parse_iso_utc(text)
        if parsed_iso:
            return parsed_iso.astimezone(timezone.utc)
        try:
            numeric = float(text)
            return parse_timestamp_utc(numeric)
        except Exception:
            return None
    return None


def is_recently_online(last_seen_at: str, days: int = None) -> bool:
    if days is None:
        days = get_recent_device_cutoff_days()
    seen_dt = parse_iso_utc(last_seen_at)
    if not seen_dt:
        return False
    age = datetime.now(timezone.utc) - seen_dt.astimezone(timezone.utc)
    return age.total_seconds() <= (days * 86400)


def get_app_setting(key: str, default_value: str = "") -> str:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
        row = cur.fetchone()
        conn.close()
    if not row:
        return default_value
    return str(row[0] if row[0] is not None else default_value)


def set_app_setting(key: str, value: str) -> None:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, utc_now_iso()),
        )
        conn.commit()
        conn.close()


def get_app_setting_bool(key: str, default_value: bool = False) -> bool:
    raw = get_app_setting(key, "true" if default_value else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_recent_device_cutoff_days() -> int:
    raw = get_app_setting("recent_device_cutoff_days", str(RECENT_DEVICE_CUTOFF_DAYS_DEFAULT)).strip()
    try:
        value = int(raw)
    except Exception:
        value = RECENT_DEVICE_CUTOFF_DAYS_DEFAULT
    return max(1, min(3650, value))


def use_global_device_cutoff() -> bool:
    return get_app_setting_bool("use_global_device_cutoff", True)


def get_provider_cutoff_enabled(provider: str) -> bool:
    if use_global_device_cutoff():
        return get_app_setting_bool("enable_recent_device_cutoff", ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT)
    key = f"enable_recent_device_cutoff_{provider}"
    return get_app_setting_bool(key, get_app_setting_bool("enable_recent_device_cutoff", ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT))


def get_provider_cutoff_days(provider: str) -> int:
    if use_global_device_cutoff():
        return get_recent_device_cutoff_days()
    key = f"recent_device_cutoff_days_{provider}"
    raw = get_app_setting(key, "").strip()
    if raw:
        try:
            value = int(raw)
            return max(1, min(3650, value))
        except Exception:
            pass
    return get_recent_device_cutoff_days()


def singularize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def normalize_customer_name(name: str) -> str:
    lowered = (name or "").strip().lower()
    lowered = re.sub(r"[^\w\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    raw_tokens = [t for t in lowered.split(" ") if t]
    tokens = []
    for token in raw_tokens:
        singular = singularize_token(token)
        if singular in LEGAL_SUFFIX_STOPWORDS:
            continue
        tokens.append(singular)
    # Fall back to cleaned name if everything was filtered.
    return " ".join(tokens) if tokens else lowered


def load_merge_mappings() -> Dict[str, str]:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT from_key, to_key FROM merge_mappings")
        rows = cur.fetchall()
        conn.close()
    return {str(r["from_key"]): str(r["to_key"]) for r in rows}


def resolve_merge_key(key: str, mappings: Dict[str, str]) -> str:
    current = key
    seen = set()
    while current in mappings and current not in seen:
        seen.add(current)
        current = mappings[current]
    return current


def load_platform_links() -> List[Dict]:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, ninja_key, sophos_key, canonical_name FROM platform_links ORDER BY id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    return rows


def apply_platform_links(ninja_counts: Dict[str, Dict], sophos_counts: Dict[str, Dict]) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    links = load_platform_links()
    if not links:
        return ninja_counts, sophos_counts

    merge_mappings = load_merge_mappings()
    ninja_result = dict(ninja_counts)
    sophos_result = dict(sophos_counts)
    consumed_ninja = set()
    consumed_sophos = set()

    for link in links:
        nkey = resolve_merge_key(str(link["ninja_key"]), merge_mappings)
        skey = resolve_merge_key(str(link["sophos_key"]), merge_mappings)
        nentry = ninja_counts.get(nkey)
        sentry = sophos_counts.get(skey)
        if not nentry and not sentry:
            continue

        joined_key = f"link:{link['id']}"
        canonical_name = (link.get("canonical_name") or "").strip() or (nentry or sentry).get("display_name", joined_key)

        if nentry:
            ninja_result[joined_key] = {
                "display_name": canonical_name,
                "count": int(nentry.get("count") or 0),
                "server_count": int(nentry.get("server_count") or 0),
                "device_count": int(nentry.get("device_count") or 0),
                "source_name": nentry.get("source_name") or nentry.get("display_name"),
            }
            consumed_ninja.add(nkey)
        if sentry:
            sophos_result[joined_key] = {
                "display_name": canonical_name,
                "count": int(sentry.get("count") or 0),
                "server_count": int(sentry.get("server_count") or 0),
                "device_count": int(sentry.get("device_count") or 0),
                "source_name": sentry.get("source_name") or sentry.get("display_name"),
            }
            consumed_sophos.add(skey)

    for key in consumed_ninja:
        ninja_result.pop(key, None)
    for key in consumed_sophos:
        sophos_result.pop(key, None)

    return ninja_result, sophos_result


def apply_platform_link_to_cached_data(link_id: int, ninja_key: str, sophos_key: str, canonical_name: str) -> Dict[str, int]:
    summary = {"rows_collapsed": 0}
    joined_key = f"link:{link_id}"
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                c.id,
                c.normalized_key,
                l.ninja_count,
                l.ninja_server_count,
                l.ninja_device_count,
                l.sophos_count,
                l.sophos_server_count,
                l.sophos_device_count
            FROM customers c
            LEFT JOIN customer_counts_latest l ON l.customer_id = c.id
            WHERE c.normalized_key IN (?, ?, ?)
            """,
            (ninja_key, sophos_key, joined_key),
        )
        rows = [dict(r) for r in cur.fetchall()]
        total_ninja = sum(int((r.get("ninja_count") or 0)) for r in rows)
        total_ninja_server = sum(int((r.get("ninja_server_count") or 0)) for r in rows)
        total_ninja_device = sum(int((r.get("ninja_device_count") or 0)) for r in rows)
        total_sophos = sum(int((r.get("sophos_count") or 0)) for r in rows)
        total_sophos_server = sum(int((r.get("sophos_server_count") or 0)) for r in rows)
        total_sophos_device = sum(int((r.get("sophos_device_count") or 0)) for r in rows)
        synced_at = utc_now_iso()

        cur.execute(
            """
            INSERT INTO customers (display_name, normalized_key, ninja_source_name, sophos_source_name)
            VALUES (?, ?, NULL, NULL)
            ON CONFLICT(normalized_key) DO UPDATE SET
                display_name = excluded.display_name
            """,
            (canonical_name, joined_key),
        )
        cur.execute("SELECT id FROM customers WHERE normalized_key = ?", (joined_key,))
        joined_customer_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO customer_counts_latest
                (
                    customer_id,
                    ninja_count,
                    ninja_server_count,
                    ninja_device_count,
                    sophos_count,
                    sophos_server_count,
                    sophos_device_count,
                    has_ninja,
                    has_sophos,
                    last_synced_at
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(customer_id) DO UPDATE SET
                ninja_count=excluded.ninja_count,
                ninja_server_count=excluded.ninja_server_count,
                ninja_device_count=excluded.ninja_device_count,
                sophos_count=excluded.sophos_count,
                sophos_server_count=excluded.sophos_server_count,
                sophos_device_count=excluded.sophos_device_count,
                has_ninja=excluded.has_ninja,
                has_sophos=excluded.has_sophos,
                last_synced_at=excluded.last_synced_at
            """,
            (
                joined_customer_id,
                total_ninja,
                total_ninja_server,
                total_ninja_device,
                total_sophos,
                total_sophos_server,
                total_sophos_device,
                1 if total_ninja > 0 else 0,
                1 if total_sophos > 0 else 0,
                synced_at,
            ),
        )

        cur.execute(
            """
            INSERT INTO customer_count_history (
                customer_id,
                ninja_count,
                ninja_server_count,
                ninja_device_count,
                sophos_count,
                sophos_server_count,
                sophos_device_count,
                captured_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                joined_customer_id,
                total_ninja,
                total_ninja_server,
                total_ninja_device,
                total_sophos,
                total_sophos_server,
                total_sophos_device,
                synced_at,
            ),
        )

        # Remove source rows so UI immediately shows single canonical entry.
        cur.execute("SELECT id FROM customers WHERE normalized_key IN (?, ?)", (ninja_key, sophos_key))
        source_ids = [r[0] for r in cur.fetchall()]
        for sid in source_ids:
            if sid == joined_customer_id:
                continue
            cur.execute("DELETE FROM customer_counts_latest WHERE customer_id = ?", (sid,))
            cur.execute("DELETE FROM customers WHERE id = ?", (sid,))
            summary["rows_collapsed"] += 1

        conn.commit()
        conn.close()
    return summary


def ensure_db_dir() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(cur: sqlite3.Cursor, table_name: str, column_name: str, definition_sql: str) -> None:
    cur.execute(f"PRAGMA table_info({table_name})")
    existing = {str(row[1]) for row in cur.fetchall()}
    if column_name not in existing:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition_sql}")


def rename_column_if_needed(cur: sqlite3.Cursor, table_name: str, old_name: str, new_name: str) -> None:
    cur.execute(f"PRAGMA table_info({table_name})")
    existing = {str(row[1]) for row in cur.fetchall()}
    if old_name in existing and new_name not in existing:
        cur.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")


def migrate_nable_schema_to_ninja(cur: sqlite3.Cursor) -> None:
    """Upgrade older N-able schema/setting keys to Ninja names."""
    rename_column_if_needed(cur, "customers", "nable_source_name", "ninja_source_name")
    for table in ("customer_counts_latest", "customer_count_history"):
        rename_column_if_needed(cur, table, "nable_count", "ninja_count")
        rename_column_if_needed(cur, table, "nable_server_count", "ninja_server_count")
        rename_column_if_needed(cur, table, "nable_device_count", "ninja_device_count")
    rename_column_if_needed(cur, "customer_counts_latest", "has_nable", "has_ninja")
    rename_column_if_needed(cur, "platform_links", "nable_key", "ninja_key")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='app_settings'")
    if cur.fetchone():
        key_renames = {
            "enable_recent_device_cutoff_nable": "enable_recent_device_cutoff_ninja",
            "recent_device_cutoff_days_nable": "recent_device_cutoff_days_ninja",
        }
        for old_key, new_key in key_renames.items():
            cur.execute("SELECT value, updated_at FROM app_settings WHERE key = ?", (old_key,))
            row = cur.fetchone()
            if not row:
                continue
            cur.execute("SELECT 1 FROM app_settings WHERE key = ?", (new_key,))
            if cur.fetchone():
                cur.execute("DELETE FROM app_settings WHERE key = ?", (old_key,))
            else:
                cur.execute(
                    "UPDATE app_settings SET key = ? WHERE key = ?",
                    (new_key, old_key),
                )


def ensure_split_count_columns(cur: sqlite3.Cursor) -> None:
    ensure_column(cur, "customer_counts_latest", "ninja_server_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_counts_latest", "ninja_device_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_counts_latest", "sophos_server_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_counts_latest", "sophos_device_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_count_history", "ninja_server_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_count_history", "ninja_device_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_count_history", "sophos_server_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(cur, "customer_count_history", "sophos_device_count", "INTEGER NOT NULL DEFAULT 0")


def init_db() -> None:
    ensure_db_dir()
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                display_name TEXT NOT NULL,
                normalized_key TEXT NOT NULL UNIQUE,
                ninja_source_name TEXT,
                sophos_source_name TEXT
            );

            CREATE TABLE IF NOT EXISTS customer_counts_latest (
                customer_id INTEGER PRIMARY KEY,
                ninja_count INTEGER NOT NULL DEFAULT 0,
                ninja_server_count INTEGER NOT NULL DEFAULT 0,
                ninja_device_count INTEGER NOT NULL DEFAULT 0,
                sophos_count INTEGER NOT NULL DEFAULT 0,
                sophos_server_count INTEGER NOT NULL DEFAULT 0,
                sophos_device_count INTEGER NOT NULL DEFAULT 0,
                has_ninja INTEGER NOT NULL DEFAULT 0,
                has_sophos INTEGER NOT NULL DEFAULT 0,
                last_synced_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            );

            CREATE TABLE IF NOT EXISTS customer_count_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                ninja_count INTEGER NOT NULL DEFAULT 0,
                ninja_server_count INTEGER NOT NULL DEFAULT 0,
                ninja_device_count INTEGER NOT NULL DEFAULT 0,
                sophos_count INTEGER NOT NULL DEFAULT 0,
                sophos_server_count INTEGER NOT NULL DEFAULT 0,
                sophos_device_count INTEGER NOT NULL DEFAULT 0,
                captured_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                error_summary TEXT
            );

            CREATE TABLE IF NOT EXISTS merge_mappings (
                from_key TEXT PRIMARY KEY,
                to_key TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS platform_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ninja_key TEXT NOT NULL UNIQUE,
                sophos_key TEXT NOT NULL UNIQUE,
                canonical_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        migrate_nable_schema_to_ninja(cur)
        ensure_split_count_columns(cur)
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            (
                "enable_recent_device_cutoff",
                "true" if ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT else "false",
                utc_now_iso(),
            ),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("recent_device_cutoff_days", str(RECENT_DEVICE_CUTOFF_DAYS_DEFAULT), utc_now_iso()),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            (
                "enable_recent_device_cutoff_ninja",
                "true" if ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT else "false",
                utc_now_iso(),
            ),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            (
                "enable_recent_device_cutoff_sophos",
                "true" if ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT else "false",
                utc_now_iso(),
            ),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("recent_device_cutoff_days_ninja", str(RECENT_DEVICE_CUTOFF_DAYS_DEFAULT), utc_now_iso()),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("recent_device_cutoff_days_sophos", str(RECENT_DEVICE_CUTOFF_DAYS_DEFAULT), utc_now_iso()),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("use_global_device_cutoff", "true", utc_now_iso()),
        )
        conn.commit()
        conn.close()


def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug("HTTP %s %s attempt=%s", method, url, attempt)
            response = requests.request(
                method,
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            logger.warning("HTTP request failed method=%s url=%s attempt=%s error=%s", method, url, attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Request failed after retries method={method} url={url}: {last_error}")


def extract_ninja_customer_name(device: Dict) -> str:
    candidates = [
        device.get("organizationName"),
        device.get("clientName"),
        device.get("customerName"),
        (device.get("organization") or {}).get("name") if isinstance(device.get("organization"), dict) else None,
        ((device.get("references") or {}).get("organization") or {}).get("name")
        if isinstance(device.get("references"), dict)
        else None,
        (device.get("client") or {}).get("name") if isinstance(device.get("client"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "Unknown Customer"


def extract_ninja_device_name(device: Dict) -> str:
    candidates = extract_ninja_device_name_candidates(device)
    return candidates[0] if candidates else ""


def extract_ninja_device_name_candidates(device: Dict) -> List[str]:
    candidates: List[str] = []
    field_names = (
        "systemName",
        "dnsName",
        "displayName",
        "name",
        "hostname",
        "netbiosName",
        "computerName",
        "deviceName",
        "machineName",
    )
    nested_objects = (
        "references",
        "device",
        "system",
        "network",
        "computer",
    )

    for field in field_names:
        value = device.get(field)
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())

    for obj_key in nested_objects:
        nested = device.get(obj_key)
        if not isinstance(nested, dict):
            continue
        for field in field_names:
            value = nested.get(field)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())

    # Preserve order, dedupe case-insensitively.
    deduped: List[str] = []
    seen = set()
    for candidate in candidates:
        folded = candidate.lower()
        if folded in seen:
            continue
        seen.add(folded)
        deduped.append(candidate)

    return deduped


def extract_devices_from_response(payload) -> List[Dict]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("devices", "items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [v for v in value if isinstance(v, dict)]
    return []


def normalize_device_name(name: str) -> str:
    cleaned = (name or "").strip().lower()
    cleaned = re.sub(r"\.local$|\.lan$|\.corp$|\.internal$", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\-_.]", "", cleaned)
    return cleaned


def device_name_skeleton(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").strip().lower())


def fetch_ninja_access_token() -> str:
    if not NINJA_CLIENT_ID or not NINJA_CLIENT_SECRET:
        raise RuntimeError("Missing NINJA_CLIENT_ID or NINJA_CLIENT_SECRET")

    token_url = f"{NINJA_API_BASE.rstrip('/')}{NINJA_TOKEN_PATH}"
    response = request_with_retry(
        "POST",
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": NINJA_CLIENT_ID,
            "client_secret": NINJA_CLIENT_SECRET,
            "scope": NINJA_OAUTH_SCOPE,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
    )
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Ninja authenticate response missing access_token")
    logger.info("Ninja auth succeeded.")
    return token


def fetch_all_ninja_devices(access_token: str) -> List[Dict]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    devices: List[Dict] = []
    after = None
    page_size = 1000

    while True:
        params = {"pageSize": page_size}
        if after is not None:
            params["after"] = after
        response = request_with_retry(
            "GET",
            f"{NINJA_API_BASE.rstrip('/')}{NINJA_DEVICES_PATH}",
            headers=headers,
            params=params,
        )
        payload = response.json()
        batch = extract_devices_from_response(payload)
        if not batch:
            break
        devices.extend(batch)
        last_id = batch[-1].get("id")
        if last_id is None or len(batch) < page_size:
            break
        after = last_id

    logger.info("Ninja device fetch complete total_devices=%s", len(devices))
    return devices


def extract_ninja_last_seen(device: Dict):
    preferred_paths = [
        ("lastContact",),
        ("lastUpdate",),
        ("lastSeenAt",),
        ("lastSeen",),
        ("offline",),
        ("references", "lastContact"),
    ]
    candidates = []

    for path in preferred_paths:
        current = device
        ok = True
        for key in path:
            if not isinstance(current, dict):
                ok = False
                break
            current = current.get(key)
        if ok:
            parsed = parse_timestamp_utc(current)
            if parsed:
                candidates.append(parsed)

    return max(candidates) if candidates else None


def fetch_ninja_counts() -> Dict[str, Dict]:
    logger.info(
        "Starting Ninja count sync base=%s token_path=%s devices_path=%s",
        NINJA_API_BASE,
        NINJA_TOKEN_PATH,
        NINJA_DEVICES_PATH,
    )
    access_token = fetch_ninja_access_token()
    devices = fetch_all_ninja_devices(access_token)
    merge_mappings = load_merge_mappings()
    apply_recent_cutoff = get_provider_cutoff_enabled("ninja")
    cutoff_days = get_provider_cutoff_days("ninja")
    cutoff_seconds = cutoff_days * 86400
    cutoff_applied = 0
    missing_last_seen = 0

    counts: Dict[str, Dict] = {}
    for device in devices:
        if apply_recent_cutoff:
            latest_seen = extract_ninja_last_seen(device)
            if latest_seen is None:
                # Keep devices with unknown last-contact rather than dropping them.
                missing_last_seen += 1
            else:
                age_seconds = (datetime.now(timezone.utc) - latest_seen).total_seconds()
                if age_seconds > cutoff_seconds:
                    cutoff_applied += 1
                    continue

        raw_name = extract_ninja_customer_name(device)
        normalized = normalize_customer_name(raw_name)
        normalized = resolve_merge_key(normalized, merge_mappings)
        if not normalized:
            continue
        kind = classify_ninja_device_kind(device)
        if normalized not in counts:
            counts[normalized] = {
                "display_name": raw_name,
                "count": 0,
                "server_count": 0,
                "device_count": 0,
                "source_name": raw_name,
            }
        counts[normalized]["count"] += 1
        if kind == "server":
            counts[normalized]["server_count"] += 1
        else:
            counts[normalized]["device_count"] += 1
        if len(raw_name) > len(counts[normalized]["display_name"]):
            counts[normalized]["display_name"] = raw_name

    logger.info(
        "Ninja customer count aggregation complete customers=%s recent_cutoff_enabled=%s recent_cutoff_days=%s filtered_devices=%s missing_last_seen=%s",
        len(counts),
        apply_recent_cutoff,
        cutoff_days,
        cutoff_applied,
        missing_last_seen,
    )
    return counts


def fetch_sophos_token() -> str:
    if not SOPHOS_CLIENT_ID or not SOPHOS_CLIENT_SECRET:
        raise RuntimeError("Missing SOPHOS_CLIENT_ID or SOPHOS_CLIENT_SECRET")

    response = request_with_retry(
        "POST",
        SOPHOS_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": SOPHOS_CLIENT_ID,
            "client_secret": SOPHOS_CLIENT_SECRET,
            "scope": "token",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Sophos auth response missing access_token")
    logger.info("Sophos auth succeeded.")
    return token


def fetch_sophos_counts() -> Dict[str, Dict]:
    apply_recent_cutoff = get_provider_cutoff_enabled("sophos")
    cutoff_days = get_provider_cutoff_days("sophos")
    logger.info(
        "Starting Sophos count sync recently_online_days=%s recent_cutoff_enabled=%s",
        cutoff_days,
        apply_recent_cutoff,
    )
    token = fetch_sophos_token()
    merge_mappings = load_merge_mappings()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    whoami_resp = request_with_retry(
        "GET",
        "https://api.central.sophos.com/whoami/v1",
        headers=headers,
    )
    whoami = whoami_resp.json()
    id_type = whoami.get("idType")
    if id_type != "partner":
        raise RuntimeError(f"Sophos credential is not partner-level (idType={id_type})")
    partner_id = whoami.get("id")
    partner_host = "https://api.central.sophos.com"
    if not partner_id:
        raise RuntimeError("Sophos whoami response missing partner id")
    logger.info("Sophos whoami succeeded partner_id=%s", partner_id)

    tenants: List[Dict] = []
    page = 1
    while True:
        params = {"pageSize": 100}
        if page == 1:
            params["pageTotal"] = "true"
        else:
            params["page"] = page
        tenants_resp = request_with_retry(
            "GET",
            f"{partner_host}/partner/v1/tenants",
            headers={**headers, "X-Partner-ID": partner_id},
            params=params,
        )
        tenants_payload = tenants_resp.json()
        tenants.extend(tenants_payload.get("items") or [])
        total_pages = ((tenants_payload.get("pages") or {}).get("total")) or 1
        if page >= total_pages:
            break
        page += 1
    logger.info("Sophos tenant fetch complete tenants=%s", len(tenants))

    counts: Dict[str, Dict] = {}
    for tenant in tenants:
        tenant_id = tenant.get("id")
        tenant_name = tenant.get("name", "Unknown Tenant")
        if not tenant_id:
            continue

        tenant_api_host = (tenant.get("apiHost") or "https://api.central.sophos.com").rstrip("/")
        endpoint_url = f"{tenant_api_host}/endpoint/v1/endpoints"
        count = 0
        server_count = 0
        device_count = 0
        try:
            next_key = None
            while True:
                params = {"pageSize": 500, "view": "full"}
                if next_key:
                    params = {"pageFromKey": next_key, "pageSize": 500, "view": "full"}
                endpoint_resp = request_with_retry(
                    "GET",
                    endpoint_url,
                    headers={**headers, "X-Tenant-ID": tenant_id},
                    params=params,
                )
                endpoint_payload = endpoint_resp.json()
                items = endpoint_payload.get("items") or []
                for item in items:
                    if (not apply_recent_cutoff) or is_recently_online(item.get("lastSeenAt"), cutoff_days):
                        count += 1
                        endpoint_kind = classify_sophos_endpoint_kind(item)
                        if endpoint_kind == "server":
                            server_count += 1
                        else:
                            device_count += 1
                next_key = ((endpoint_payload.get("pages") or {}).get("nextKey"))
                if not next_key:
                    break
        except Exception as exc:
            logger.warning("Sophos tenant endpoint count failed tenant=%s tenant_id=%s error=%s", tenant_name, tenant_id, exc)
            count = 0
            server_count = 0
            device_count = 0

        normalized = normalize_customer_name(tenant_name)
        normalized = resolve_merge_key(normalized, merge_mappings)
        if not normalized:
            continue

        if normalized not in counts:
            counts[normalized] = {
                "display_name": tenant_name,
                "count": 0,
                "server_count": 0,
                "device_count": 0,
                "source_name": tenant_name,
            }
        counts[normalized]["count"] += count
        counts[normalized]["server_count"] += server_count
        counts[normalized]["device_count"] += device_count
        if len(tenant_name) > len(counts[normalized]["display_name"]):
            counts[normalized]["display_name"] = tenant_name

    logger.info("Sophos customer count aggregation complete customers=%s", len(counts))
    return counts


def fetch_sophos_tenants(token: str, partner_id: str) -> List[Dict]:
    partner_host = "https://api.central.sophos.com"
    tenants: List[Dict] = []
    page = 1
    while True:
        params = {"pageSize": 100}
        if page == 1:
            params["pageTotal"] = "true"
        else:
            params["page"] = page
        tenants_resp = request_with_retry(
            "GET",
            f"{partner_host}/partner/v1/tenants",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json", "X-Partner-ID": partner_id},
            params=params,
        )
        tenants_payload = tenants_resp.json()
        tenants.extend(tenants_payload.get("items") or [])
        total_pages = ((tenants_payload.get("pages") or {}).get("total")) or 1
        if page >= total_pages:
            break
        page += 1
    return tenants


def fetch_sophos_tenant_device_entries(token: str, tenant: Dict) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    tenant_id = tenant.get("id")
    api_host = (tenant.get("apiHost") or "https://api.central.sophos.com").rstrip("/")
    if not tenant_id:
        return entries

    apply_recent_cutoff = get_provider_cutoff_enabled("sophos")
    cutoff_days = get_provider_cutoff_days("sophos")
    next_key = None
    while True:
        params = {"pageSize": 500, "view": "full"}
        if next_key:
            params = {"pageFromKey": next_key, "pageSize": 500, "view": "full"}
        resp = request_with_retry(
            "GET",
            f"{api_host}/endpoint/v1/endpoints",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json", "X-Tenant-ID": tenant_id},
            params=params,
        )
        payload = resp.json()
        items = payload.get("items") or []
        for item in items:
            if apply_recent_cutoff and (not is_recently_online(item.get("lastSeenAt"), cutoff_days)):
                continue
            host = item.get("hostname")
            if isinstance(host, str) and host.strip():
                entries.append({"name": host.strip(), "kind": classify_sophos_endpoint_kind(item)})
        next_key = ((payload.get("pages") or {}).get("nextKey"))
        if not next_key:
            break

    return entries


def classify_asset_kind(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return "device"
    server_tokens = (
        "server",
        "windows server",
        "domain controller",
        "hyper-v",
        "vm host",
        "vcenter",
        "linux server",
        "ubuntu server",
        "debian server",
        "sql server",
    )
    if any(token in raw for token in server_tokens):
        return "server"
    return "device"


def classify_ninja_device_kind(device: Dict) -> str:
    node_class = str(device.get("nodeClass") or "").strip().upper()
    if node_class:
        server_classes = {
            "WINDOWS_SERVER",
            "LINUX_SERVER",
            "MAC_SERVER",
            "VMWARE_VM_HOST",
            "HYPERV_VMM_HOST",
            "NMS_SERVER",
            "NMS_VM_HOST",
        }
        if node_class in server_classes or node_class.endswith("_SERVER") or "SERVER" in node_class:
            # Avoid classifying workstation-like classes as servers.
            if "WORKSTATION" not in node_class:
                return "server"
        return "device"

    fields_to_probe = [
        "nodeClass",
        "deviceClass",
        "deviceType",
        "type",
        "subType",
        "role",
        "os",
        "osName",
        "operatingSystem",
        "platform",
        "description",
        "systemName",
        "name",
    ]
    for field in fields_to_probe:
        value = device.get(field)
        if isinstance(value, str) and value.strip():
            if classify_asset_kind(value) == "server":
                return "server"
    role = device.get("role")
    if isinstance(role, dict):
        role_name = role.get("name") or role.get("nodeClass")
        if isinstance(role_name, str) and classify_asset_kind(role_name) == "server":
            return "server"
    return "device"


def classify_sophos_endpoint_kind(endpoint: Dict) -> str:
    fields_to_probe = [
        "type",
        "endpointType",
        "productType",
        "os",
        "osName",
        "platform",
        "role",
        "hostname",
    ]
    for field in fields_to_probe:
        value = endpoint.get(field)
        if isinstance(value, str) and value.strip():
            if classify_asset_kind(value) == "server":
                return "server"
    return "device"


def merge_kind(existing_kind: str, incoming_kind: str) -> str:
    kinds = {existing_kind or "device", incoming_kind or "device"}
    if "server" in kinds:
        return "server"
    return "device"


def choose_category_for_row(ninja_kind: str, sophos_kind: str) -> str:
    n_kind = ninja_kind or "device"
    s_kind = sophos_kind or "device"
    if n_kind and s_kind and n_kind != s_kind:
        return "mixed"
    return n_kind or s_kind or "device"


def get_customer_by_id(customer_id: int):
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.id, c.display_name, c.normalized_key, c.ninja_source_name, c.sophos_source_name
            FROM customers c
            WHERE c.id = ?
            """,
            (customer_id,),
        )
        row = cur.fetchone()
        conn.close()
    return dict(row) if row else None


def write_sync_run(started_at: str, finished_at: str, status: str, error_summary: str = None) -> None:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sync_runs (started_at, finished_at, status, error_summary)
            VALUES (?, ?, ?, ?)
            """,
            (started_at, finished_at, status, error_summary),
        )
        conn.commit()
        conn.close()


def upsert_counts(
    ninja_counts: Dict[str, Dict],
    sophos_counts: Dict[str, Dict],
    synced_at: str,
    prune_missing: bool = False,
) -> None:
    all_keys = sorted(set(ninja_counts.keys()) | set(sophos_counts.keys()))
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        touched_customer_ids = set()

        for key in all_keys:
            ninja_entry = ninja_counts.get(key)
            sophos_entry = sophos_counts.get(key)

            display_name = None
            if ninja_entry and sophos_entry:
                display_name = ninja_entry["display_name"] if len(ninja_entry["display_name"]) >= len(sophos_entry["display_name"]) else sophos_entry["display_name"]
            elif ninja_entry:
                display_name = ninja_entry["display_name"]
            elif sophos_entry:
                display_name = sophos_entry["display_name"]
            else:
                display_name = key

            ninja_source_name = (ninja_entry.get("source_name") or ninja_entry.get("display_name")) if ninja_entry else None
            sophos_source_name = (sophos_entry.get("source_name") or sophos_entry.get("display_name")) if sophos_entry else None
            ninja_count = int(ninja_entry["count"]) if ninja_entry else 0
            ninja_server_count = int(ninja_entry.get("server_count") or 0) if ninja_entry else 0
            ninja_device_count = int(ninja_entry.get("device_count") or 0) if ninja_entry else 0
            sophos_count = int(sophos_entry["count"]) if sophos_entry else 0
            sophos_server_count = int(sophos_entry.get("server_count") or 0) if sophos_entry else 0
            sophos_device_count = int(sophos_entry.get("device_count") or 0) if sophos_entry else 0
            has_ninja = 1 if ninja_count > 0 else 0
            has_sophos = 1 if sophos_count > 0 else 0

            cur.execute(
                """
                INSERT INTO customers (display_name, normalized_key, ninja_source_name, sophos_source_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(normalized_key) DO UPDATE SET
                    display_name=excluded.display_name,
                    ninja_source_name=excluded.ninja_source_name,
                    sophos_source_name=excluded.sophos_source_name
                """,
                (display_name, key, ninja_source_name, sophos_source_name),
            )
            cur.execute("SELECT id FROM customers WHERE normalized_key = ?", (key,))
            customer_id = cur.fetchone()[0]
            touched_customer_ids.add(customer_id)
            cur.execute(
                """
                INSERT INTO customer_counts_latest
                    (
                        customer_id,
                        ninja_count,
                        ninja_server_count,
                        ninja_device_count,
                        sophos_count,
                        sophos_server_count,
                        sophos_device_count,
                        has_ninja,
                        has_sophos,
                        last_synced_at
                    )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    ninja_count=excluded.ninja_count,
                    ninja_server_count=excluded.ninja_server_count,
                    ninja_device_count=excluded.ninja_device_count,
                    sophos_count=excluded.sophos_count,
                    sophos_server_count=excluded.sophos_server_count,
                    sophos_device_count=excluded.sophos_device_count,
                    has_ninja=excluded.has_ninja,
                    has_sophos=excluded.has_sophos,
                    last_synced_at=excluded.last_synced_at
                """,
                (
                    customer_id,
                    ninja_count,
                    ninja_server_count,
                    ninja_device_count,
                    sophos_count,
                    sophos_server_count,
                    sophos_device_count,
                    has_ninja,
                    has_sophos,
                    synced_at,
                ),
            )
            cur.execute(
                """
                INSERT INTO customer_count_history (
                    customer_id,
                    ninja_count,
                    ninja_server_count,
                    ninja_device_count,
                    sophos_count,
                    sophos_server_count,
                    sophos_device_count,
                    captured_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    customer_id,
                    ninja_count,
                    ninja_server_count,
                    ninja_device_count,
                    sophos_count,
                    sophos_server_count,
                    sophos_device_count,
                    synced_at,
                ),
            )

        if prune_missing:
            cur.execute("SELECT customer_id FROM customer_counts_latest")
            existing_ids = {int(r[0]) for r in cur.fetchall()}
            stale_ids = existing_ids - touched_customer_ids
            for stale_id in stale_ids:
                cur.execute(
                    """
                    UPDATE customer_counts_latest
                    SET ninja_count = 0,
                        ninja_server_count = 0,
                        ninja_device_count = 0,
                        sophos_count = 0,
                        sophos_server_count = 0,
                        sophos_device_count = 0,
                        has_ninja = 0,
                        has_sophos = 0,
                        last_synced_at = ?
                    WHERE customer_id = ?
                    """,
                    (synced_at, stale_id),
                )
                cur.execute(
                    """
                    INSERT INTO customer_count_history (
                        customer_id,
                        ninja_count,
                        ninja_server_count,
                        ninja_device_count,
                        sophos_count,
                        sophos_server_count,
                        sophos_device_count,
                        captured_at
                    )
                    VALUES (?, 0, 0, 0, 0, 0, 0, ?)
                    """,
                    (stale_id, synced_at),
                )

        conn.commit()
        conn.close()


def apply_merge_to_cached_data(from_key: str, to_key: str) -> Dict[str, int]:
    merged = {"from_customer_removed": 0, "target_updated": 0}
    if not from_key or not to_key or from_key == to_key:
        return merged

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id, display_name, ninja_source_name, sophos_source_name FROM customers WHERE normalized_key = ?", (from_key,))
        from_customer = cur.fetchone()
        cur.execute("SELECT id, display_name, ninja_source_name, sophos_source_name FROM customers WHERE normalized_key = ?", (to_key,))
        to_customer = cur.fetchone()

        if not from_customer:
            conn.close()
            return merged

        if not to_customer:
            cur.execute(
                """
                INSERT INTO customers (display_name, normalized_key, ninja_source_name, sophos_source_name)
                VALUES (?, ?, ?, ?)
                """,
                (
                    from_customer["display_name"],
                    to_key,
                    from_customer["ninja_source_name"],
                    from_customer["sophos_source_name"],
                ),
            )
            cur.execute("SELECT id, display_name, ninja_source_name, sophos_source_name FROM customers WHERE normalized_key = ?", (to_key,))
            to_customer = cur.fetchone()

        cur.execute(
            """
            SELECT
                ninja_count,
                ninja_server_count,
                ninja_device_count,
                sophos_count,
                sophos_server_count,
                sophos_device_count,
                has_ninja,
                has_sophos,
                last_synced_at
            FROM customer_counts_latest
            WHERE customer_id = ?
            """,
            (from_customer["id"],),
        )
        from_latest = cur.fetchone()
        cur.execute(
            """
            SELECT
                ninja_count,
                ninja_server_count,
                ninja_device_count,
                sophos_count,
                sophos_server_count,
                sophos_device_count,
                has_ninja,
                has_sophos,
                last_synced_at
            FROM customer_counts_latest
            WHERE customer_id = ?
            """,
            (to_customer["id"],),
        )
        to_latest = cur.fetchone()

        from_ninja = int((from_latest["ninja_count"] if from_latest else 0) or 0)
        from_ninja_server = int((from_latest["ninja_server_count"] if from_latest else 0) or 0)
        from_ninja_device = int((from_latest["ninja_device_count"] if from_latest else 0) or 0)
        from_sophos = int((from_latest["sophos_count"] if from_latest else 0) or 0)
        from_sophos_server = int((from_latest["sophos_server_count"] if from_latest else 0) or 0)
        from_sophos_device = int((from_latest["sophos_device_count"] if from_latest else 0) or 0)
        to_ninja = int((to_latest["ninja_count"] if to_latest else 0) or 0)
        to_ninja_server = int((to_latest["ninja_server_count"] if to_latest else 0) or 0)
        to_ninja_device = int((to_latest["ninja_device_count"] if to_latest else 0) or 0)
        to_sophos = int((to_latest["sophos_count"] if to_latest else 0) or 0)
        to_sophos_server = int((to_latest["sophos_server_count"] if to_latest else 0) or 0)
        to_sophos_device = int((to_latest["sophos_device_count"] if to_latest else 0) or 0)
        merged_ninja = from_ninja + to_ninja
        merged_ninja_server = from_ninja_server + to_ninja_server
        merged_ninja_device = from_ninja_device + to_ninja_device
        merged_sophos = from_sophos + to_sophos
        merged_sophos_server = from_sophos_server + to_sophos_server
        merged_sophos_device = from_sophos_device + to_sophos_device
        merged_synced = utc_now_iso()

        cur.execute(
            """
            INSERT INTO customer_counts_latest
                (
                    customer_id,
                    ninja_count,
                    ninja_server_count,
                    ninja_device_count,
                    sophos_count,
                    sophos_server_count,
                    sophos_device_count,
                    has_ninja,
                    has_sophos,
                    last_synced_at
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(customer_id) DO UPDATE SET
                ninja_count=excluded.ninja_count,
                ninja_server_count=excluded.ninja_server_count,
                ninja_device_count=excluded.ninja_device_count,
                sophos_count=excluded.sophos_count,
                sophos_server_count=excluded.sophos_server_count,
                sophos_device_count=excluded.sophos_device_count,
                has_ninja=excluded.has_ninja,
                has_sophos=excluded.has_sophos,
                last_synced_at=excluded.last_synced_at
            """,
            (
                to_customer["id"],
                merged_ninja,
                merged_ninja_server,
                merged_ninja_device,
                merged_sophos,
                merged_sophos_server,
                merged_sophos_device,
                1 if merged_ninja > 0 else 0,
                1 if merged_sophos > 0 else 0,
                merged_synced,
            ),
        )
        cur.execute(
            """
            INSERT INTO customer_count_history (
                customer_id,
                ninja_count,
                ninja_server_count,
                ninja_device_count,
                sophos_count,
                sophos_server_count,
                sophos_device_count,
                captured_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                to_customer["id"],
                merged_ninja,
                merged_ninja_server,
                merged_ninja_device,
                merged_sophos,
                merged_sophos_server,
                merged_sophos_device,
                merged_synced,
            ),
        )

        cur.execute("DELETE FROM customer_counts_latest WHERE customer_id = ?", (from_customer["id"],))
        cur.execute("DELETE FROM customers WHERE id = ?", (from_customer["id"],))
        merged["from_customer_removed"] = 1
        merged["target_updated"] = 1

        conn.commit()
        conn.close()
    return merged


def dedupe_customers_by_display_name() -> Dict[str, int]:
    result = {"groups_processed": 0, "rows_removed": 0}
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.display_name,
                c.normalized_key,
                c.ninja_source_name,
                c.sophos_source_name,
                l.ninja_count,
                l.ninja_server_count,
                l.ninja_device_count,
                l.sophos_count
                ,
                l.sophos_server_count,
                l.sophos_device_count
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            ORDER BY LOWER(c.display_name), c.id
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            key = (row.get("display_name") or "").strip().lower()
            groups.setdefault(key, []).append(row)

        for key, members in groups.items():
            if len(members) <= 1:
                continue
            result["groups_processed"] += 1
            members_sorted = sorted(
                members,
                key=lambda m: (-(int(m.get("ninja_count") or 0) + int(m.get("sophos_count") or 0)), m["id"]),
            )
            primary = members_sorted[0]
            others = members_sorted[1:]
            total_ninja = sum(int(m.get("ninja_count") or 0) for m in members_sorted)
            total_ninja_server = sum(int(m.get("ninja_server_count") or 0) for m in members_sorted)
            total_ninja_device = sum(int(m.get("ninja_device_count") or 0) for m in members_sorted)
            total_sophos = sum(int(m.get("sophos_count") or 0) for m in members_sorted)
            total_sophos_server = sum(int(m.get("sophos_server_count") or 0) for m in members_sorted)
            total_sophos_device = sum(int(m.get("sophos_device_count") or 0) for m in members_sorted)

            cur.execute(
                """
                UPDATE customer_counts_latest
                SET ninja_count = ?,
                    ninja_server_count = ?,
                    ninja_device_count = ?,
                    sophos_count = ?,
                    sophos_server_count = ?,
                    sophos_device_count = ?,
                    has_ninja = ?,
                    has_sophos = ?,
                    last_synced_at = ?
                WHERE customer_id = ?
                """,
                (
                    total_ninja,
                    total_ninja_server,
                    total_ninja_device,
                    total_sophos,
                    total_sophos_server,
                    total_sophos_device,
                    1 if total_ninja > 0 else 0,
                    1 if total_sophos > 0 else 0,
                    utc_now_iso(),
                    primary["id"],
                ),
            )

            for other in others:
                cur.execute("DELETE FROM customer_counts_latest WHERE customer_id = ?", (other["id"],))
                cur.execute("DELETE FROM customers WHERE id = ?", (other["id"],))
                result["rows_removed"] += 1

        conn.commit()
        conn.close()
    return result


def purge_empty_duplicate_customer_rows() -> Dict[str, int]:
    result = {"groups_processed": 0, "rows_removed": 0}
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.display_name,
                l.ninja_count,
                l.sophos_count
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            ORDER BY c.id
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            group_key = normalize_customer_name(str(row.get("display_name") or ""))
            if group_key:
                groups.setdefault(group_key, []).append(row)

        for members in groups.values():
            if len(members) <= 1:
                continue
            active_rows = [m for m in members if int(m.get("ninja_count") or 0) > 0 or int(m.get("sophos_count") or 0) > 0]
            if not active_rows:
                continue
            result["groups_processed"] += 1
            stale_rows = [m for m in members if int(m.get("ninja_count") or 0) == 0 and int(m.get("sophos_count") or 0) == 0]
            for stale in stale_rows:
                cur.execute("DELETE FROM customer_counts_latest WHERE customer_id = ?", (stale["id"],))
                cur.execute("DELETE FROM customer_count_history WHERE customer_id = ?", (stale["id"],))
                cur.execute("DELETE FROM customers WHERE id = ?", (stale["id"],))
                result["rows_removed"] += 1

        conn.commit()
        conn.close()
    return result


def reset_merge_state_and_cached_data() -> None:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM merge_mappings")
        cur.execute("DELETE FROM platform_links")
        cur.execute("DELETE FROM customer_counts_latest")
        cur.execute("DELETE FROM customers")
        cur.execute("DELETE FROM customer_count_history")
        conn.commit()
        conn.close()


def run_sync(trigger: str = "scheduled") -> Tuple[bool, str, Dict]:
    if not sync_lock.acquire(blocking=False):
        return False, "Sync already in progress", {"errors": {"sync": "Sync already in progress"}}

    started_at = utc_now_iso()
    runtime_state["sync_running"] = True
    runtime_state["last_sync_started_at"] = started_at
    runtime_state["last_sync_status"] = "running"
    runtime_state["last_sync_error"] = None
    runtime_state["last_sync_details"] = {}
    logger.info("Sync started trigger=%s", trigger)

    try:
        ninja_counts: Dict[str, Dict] = {}
        sophos_counts: Dict[str, Dict] = {}
        provider_errors: Dict[str, str] = {}

        try:
            ninja_counts = fetch_ninja_counts()
        except Exception as exc:
            provider_errors["ninja"] = str(exc)
            logger.exception("Ninja sync failed: %s", exc)

        try:
            sophos_counts = fetch_sophos_counts()
        except Exception as exc:
            provider_errors["sophos"] = str(exc)
            logger.exception("Sophos sync failed: %s", exc)

        ninja_counts, sophos_counts = apply_platform_links(ninja_counts, sophos_counts)

        if not ninja_counts and not sophos_counts:
            combined = "Both provider syncs failed."
            if provider_errors:
                combined += " " + " | ".join([f"{k}: {v}" for k, v in provider_errors.items()])
            raise RuntimeError(combined)

        synced_at = utc_now_iso()
        upsert_counts(ninja_counts, sophos_counts, synced_at, prune_missing=(len(provider_errors) == 0))
        runtime_state["last_sync_finished_at"] = synced_at
        if provider_errors:
            runtime_state["last_sync_status"] = "partial"
            runtime_state["last_sync_error"] = "One or more providers failed during sync."
            runtime_state["last_sync_details"] = provider_errors
            msg = f"Sync partial ({trigger})"
            write_sync_run(started_at, synced_at, "partial", json.dumps(provider_errors))
            logger.warning("Sync partial trigger=%s errors=%s", trigger, provider_errors)
            return True, msg, {"errors": provider_errors}

        runtime_state["last_sync_status"] = "ok"
        runtime_state["last_sync_details"] = {}
        write_sync_run(started_at, synced_at, "ok", None)
        logger.info("Sync completed successfully trigger=%s", trigger)
        return True, f"Sync complete ({trigger})", {"errors": {}}
    except Exception as exc:
        finished_at = utc_now_iso()
        message = str(exc)
        runtime_state["last_sync_finished_at"] = finished_at
        runtime_state["last_sync_status"] = "error"
        runtime_state["last_sync_error"] = message
        runtime_state["last_sync_details"] = {}
        write_sync_run(started_at, finished_at, "error", message)
        logger.exception("Sync failed trigger=%s error=%s", trigger, message)
        return False, message, {"errors": {"sync": message}}
    finally:
        runtime_state["sync_running"] = False
        sync_lock.release()


def trigger_sync_async(trigger: str = "manual") -> None:
    def _runner():
        try:
            run_sync(trigger)
        except Exception as exc:
            logger.exception("Async sync trigger=%s failed: %s", trigger, exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()


def has_cached_rows() -> bool:
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM customer_counts_latest")
        total = cur.fetchone()[0]
        conn.close()
        return total > 0


def scheduler_loop() -> None:
    while True:
        time.sleep(max(30, SYNC_INTERVAL_MINUTES * 60))
        run_sync("scheduled")


@app.route("/")
def index():
    return send_from_directory(APP_DIR, "index.html")


@app.route("/settings")
def settings_page():
    return send_from_directory(APP_DIR, "settings.html")


@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(os.path.join(APP_DIR, "assets"), filename)


@app.route("/api/customers", methods=["GET"])
def api_customers():
    search = (request.args.get("search") or "").strip().lower()
    status_filter = (request.args.get("status") or "all").strip().lower()
    sort_by = (request.args.get("sort") or "name_asc").strip().lower()
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.display_name,
                c.ninja_source_name,
                c.sophos_source_name,
                l.ninja_count,
                l.ninja_server_count,
                l.ninja_device_count,
                l.sophos_count,
                l.sophos_server_count,
                l.sophos_device_count,
                l.has_ninja,
                l.has_sophos,
                l.last_synced_at,
                CASE
                    WHEN l.has_ninja = 1 AND l.has_sophos = 1 AND l.ninja_count <> l.sophos_count THEN 'mismatch'
                    ELSE 'match'
                END AS count_status,
                ABS(l.ninja_count - l.sophos_count) AS count_delta,
                ROUND((l.ninja_count + l.sophos_count) / 2.0, 1) AS average_total
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            WHERE c.normalized_key NOT IN (
                SELECT ninja_key FROM platform_links
                UNION
                SELECT sophos_key FROM platform_links
            )
            ORDER BY c.display_name COLLATE NOCASE ASC
            """
        )
        rows_all = [dict(row) for row in cur.fetchall()]
        conn.close()

    if search:
        rows_all = [r for r in rows_all if search in (r["display_name"] or "").lower()]

    matched_all = len([r for r in rows_all if r.get("count_status") == "match"])
    mismatched_all = len([r for r in rows_all if r.get("count_status") == "mismatch"])

    rows = list(rows_all)
    if search:
        rows = [r for r in rows if search in (r["display_name"] or "").lower()]
    if status_filter in {"match", "mismatch"}:
        rows = [r for r in rows if r.get("count_status") == status_filter]

    if sort_by == "name_desc":
        rows.sort(key=lambda r: (r.get("display_name") or "").lower(), reverse=True)
    elif sort_by == "mismatch_desc":
        rows.sort(key=lambda r: (int(r.get("count_delta") or 0), (r.get("display_name") or "").lower()), reverse=True)
    elif sort_by == "mismatch_asc":
        rows.sort(key=lambda r: (int(r.get("count_delta") or 0), (r.get("display_name") or "").lower()))
    elif sort_by == "total_desc":
        rows.sort(key=lambda r: (int(r.get("ninja_count") or 0) + int(r.get("sophos_count") or 0), (r.get("display_name") or "").lower()), reverse=True)
    elif sort_by == "total_asc":
        rows.sort(key=lambda r: (int(r.get("ninja_count") or 0) + int(r.get("sophos_count") or 0), (r.get("display_name") or "").lower()))
    elif sort_by == "avg_desc":
        rows.sort(key=lambda r: (float(r.get("average_total") or 0), (r.get("display_name") or "").lower()), reverse=True)
    elif sort_by == "avg_asc":
        rows.sort(key=lambda r: (float(r.get("average_total") or 0), (r.get("display_name") or "").lower()))
    else:
        rows.sort(key=lambda r: (r.get("display_name") or "").lower())

    matched_visible = len([r for r in rows if r.get("count_status") == "match"])
    mismatched_visible = len([r for r in rows if r.get("count_status") == "mismatch"])

    return jsonify(
        {
            "items": rows,
            "count": len(rows),
            "summary": {
                "all": {"matched": matched_all, "mismatched": mismatched_all, "total": len(rows_all)},
                "visible": {"matched": matched_visible, "mismatched": mismatched_visible, "total": len(rows)},
            },
        }
    )


@app.route("/api/customers/<int:customer_id>", methods=["GET"])
def api_customer(customer_id: int):
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.display_name,
                c.ninja_source_name,
                c.sophos_source_name,
                l.ninja_count,
                l.ninja_server_count,
                l.ninja_device_count,
                l.sophos_count,
                l.sophos_server_count,
                l.sophos_device_count,
                l.has_ninja,
                l.has_sophos,
                l.last_synced_at,
                CASE
                    WHEN l.has_ninja = 1 AND l.has_sophos = 1 AND l.ninja_count <> l.sophos_count THEN 'mismatch'
                    ELSE 'match'
                END AS count_status,
                ABS(l.ninja_count - l.sophos_count) AS count_delta,
                ROUND((l.ninja_count + l.sophos_count) / 2.0, 1) AS average_total
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            WHERE c.id = ?
              AND c.normalized_key NOT IN (
                  SELECT ninja_key FROM platform_links
                  UNION
                  SELECT sophos_key FROM platform_links
              )
            """,
            (customer_id,),
        )
        row = cur.fetchone()
        conn.close()

    if not row:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(dict(row))


@app.route("/api/customers/<int:customer_id>/device-compare", methods=["GET"])
def api_customer_device_compare(customer_id: int):
    customer = get_customer_by_id(customer_id)
    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    normalized_target = customer["normalized_key"]
    mappings = load_merge_mappings()
    ninja_entries: List[Dict[str, str]] = []
    sophos_entries: List[Dict[str, str]] = []
    warnings: List[str] = []
    ninja_source_name = (customer.get("ninja_source_name") or "").strip().lower()
    sophos_source_name = (customer.get("sophos_source_name") or "").strip().lower()

    # Ninja side
    try:
        ninja_token = fetch_ninja_access_token()
        ninja_devices = fetch_all_ninja_devices(ninja_token)
        for dev in ninja_devices:
            cname = extract_ninja_customer_name(dev)
            ckey = resolve_merge_key(normalize_customer_name(cname), mappings)
            cname_l = (cname or "").strip().lower()
            # Use both normalized key matching and exact source-name fallback.
            if ckey != normalized_target and (not ninja_source_name or cname_l != ninja_source_name):
                continue
            kind = classify_ninja_device_kind(dev)
            aliases = extract_ninja_device_name_candidates(dev)
            if aliases:
                for alias in aliases:
                    ninja_entries.append({"name": alias, "kind": kind})
    except Exception as exc:
        warn = f"Ninja compare fetch failed: {exc}"
        warnings.append(warn)
        logger.warning("Device compare Ninja fetch failed customer_id=%s error=%s", customer_id, exc)

    # Sophos side
    try:
        sophos_token = fetch_sophos_token()
        whoami_resp = request_with_retry(
            "GET",
            "https://api.central.sophos.com/whoami/v1",
            headers={"Authorization": f"Bearer {sophos_token}", "Accept": "application/json"},
        )
        whoami = whoami_resp.json()
        partner_id = whoami.get("id")
        if partner_id:
            tenants = fetch_sophos_tenants(sophos_token, partner_id)
            for tenant in tenants:
                tenant_name = tenant.get("name", "")
                tkey = resolve_merge_key(normalize_customer_name(tenant_name), mappings)
                tenant_name_l = (tenant_name or "").strip().lower()
                if tkey != normalized_target and (not sophos_source_name or tenant_name_l != sophos_source_name):
                    continue
                try:
                    sophos_entries.extend(fetch_sophos_tenant_device_entries(sophos_token, tenant))
                except Exception as t_exc:
                    t_warn = f"Sophos tenant compare fetch failed ({tenant_name}): {t_exc}"
                    warnings.append(t_warn)
                    logger.warning("Device compare Sophos tenant fetch failed customer_id=%s tenant=%s error=%s", customer_id, tenant_name, t_exc)
    except Exception as exc:
        warn = f"Sophos compare fetch failed: {exc}"
        warnings.append(warn)
        logger.warning("Device compare Sophos fetch failed customer_id=%s error=%s", customer_id, exc)

    ninja_by_norm: Dict[str, str] = {}
    ninja_kind_by_norm: Dict[str, str] = {}
    for entry in ninja_entries:
        normalized_name = normalize_device_name(str(entry.get("name") or ""))
        if not normalized_name:
            continue
        ninja_by_norm[normalized_name] = str(entry.get("name") or "")
        ninja_kind_by_norm[normalized_name] = merge_kind(
            ninja_kind_by_norm.get(normalized_name, "device"),
            str(entry.get("kind") or "device"),
        )

    sophos_by_norm: Dict[str, str] = {}
    sophos_kind_by_norm: Dict[str, str] = {}
    for entry in sophos_entries:
        normalized_name = normalize_device_name(str(entry.get("name") or ""))
        if not normalized_name:
            continue
        sophos_by_norm[normalized_name] = str(entry.get("name") or "")
        sophos_kind_by_norm[normalized_name] = merge_kind(
            sophos_kind_by_norm.get(normalized_name, "device"),
            str(entry.get("kind") or "device"),
        )
    ninja_set = set(ninja_by_norm.keys())
    sophos_set = set(sophos_by_norm.keys())

    ninja_skeleton_to_name: Dict[str, str] = {}
    for value in ninja_by_norm.values():
        sk = device_name_skeleton(value)
        if sk and sk not in ninja_skeleton_to_name:
            ninja_skeleton_to_name[sk] = value
    sophos_skeleton_to_name: Dict[str, str] = {}
    for value in sophos_by_norm.values():
        sk = device_name_skeleton(value)
        if sk and sk not in sophos_skeleton_to_name:
            sophos_skeleton_to_name[sk] = value

    missing_from_ninja = sorted([sophos_by_norm[k] for k in (sophos_set - ninja_set)])
    missing_from_sophos = sorted([ninja_by_norm[k] for k in (ninja_set - sophos_set)])
    all_keys = sorted(ninja_set | sophos_set)
    comparison_rows = []
    for key in all_keys:
        n_name = ninja_by_norm.get(key)
        s_name = sophos_by_norm.get(key)
        n_kind = ninja_kind_by_norm.get(key)
        s_kind = sophos_kind_by_norm.get(key)
        category_key = choose_category_for_row(n_kind, s_kind)
        near_match = None
        if n_name and s_name:
            status = "match"
            label = "Match"
            sort_rank = 1
            display = n_name
        elif n_name and not s_name:
            status = "missing_from_sophos"
            label = "Missing from Sophos"
            sort_rank = 0
            display = n_name
            skeleton = device_name_skeleton(n_name)
            near_match = sophos_skeleton_to_name.get(skeleton) if skeleton else None
        else:
            status = "missing_from_ninja"
            label = "Missing from Ninja"
            sort_rank = 0
            display = s_name
            skeleton = device_name_skeleton(s_name)
            near_match = ninja_skeleton_to_name.get(skeleton) if skeleton else None

        comparison_rows.append(
            {
                "status": status,
                "label": label,
                "device_key": key,
                "display_name": display,
                "ninja_name": n_name,
                "sophos_name": s_name,
                "category_key": category_key,
                "ninja_kind": n_kind or "device",
                "sophos_kind": s_kind or "device",
                "near_match_hint": near_match,
                "sort_rank": sort_rank,
            }
        )

    comparison_rows.sort(key=lambda r: (r["sort_rank"], (r["display_name"] or "").lower()))

    return jsonify(
        {
            "customer_id": customer_id,
            "customer_name": customer["display_name"],
            "ninja_total_names": len(ninja_by_norm),
            "sophos_total_names": len(sophos_by_norm),
            "matched_names": len(ninja_set & sophos_set),
            "missing_from_ninja": missing_from_ninja,
            "missing_from_sophos": missing_from_sophos,
            "totals_by_kind": {
                "ninja": {
                    "server": len([k for k in ninja_set if ninja_kind_by_norm.get(k) == "server"]),
                    "device": len([k for k in ninja_set if ninja_kind_by_norm.get(k) != "server"]),
                },
                "sophos": {
                    "server": len([k for k in sophos_set if sophos_kind_by_norm.get(k) == "server"]),
                    "device": len([k for k in sophos_set if sophos_kind_by_norm.get(k) != "server"]),
                },
                "matched": {
                    "server": len(
                        [
                            k
                            for k in (ninja_set & sophos_set)
                            if choose_category_for_row(ninja_kind_by_norm.get(k), sophos_kind_by_norm.get(k)) == "server"
                        ]
                    ),
                    "device": len(
                        [
                            k
                            for k in (ninja_set & sophos_set)
                            if choose_category_for_row(ninja_kind_by_norm.get(k), sophos_kind_by_norm.get(k)) != "server"
                        ]
                    ),
                },
            },
            "warnings": warnings,
            "comparison_rows": comparison_rows,
        }
    )


@app.route("/api/sync/status", methods=["GET"])
def api_sync_status():
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, started_at, finished_at, status, error_summary FROM sync_runs ORDER BY id DESC LIMIT 1")
        latest = cur.fetchone()
        conn.close()

    latest_run = dict(latest) if latest else None
    return jsonify(
        {
            "runtime": runtime_state,
            "latest_run": latest_run,
            "interval_minutes": SYNC_INTERVAL_MINUTES,
        }
    )


@app.route("/api/sync/run", methods=["POST"])
def api_sync_run():
    trigger_sync_async("manual")
    return jsonify({"success": True, "message": "Sync started", "details": {}}), 202


@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    return jsonify(
        {
            "use_global_device_cutoff": use_global_device_cutoff(),
            "enable_recent_device_cutoff": get_app_setting_bool("enable_recent_device_cutoff", ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT),
            "recent_device_cutoff_days": get_recent_device_cutoff_days(),
            "enable_recent_device_cutoff_ninja": get_provider_cutoff_enabled("ninja"),
            "enable_recent_device_cutoff_sophos": get_provider_cutoff_enabled("sophos"),
            "recent_device_cutoff_days_ninja": get_provider_cutoff_days("ninja"),
            "recent_device_cutoff_days_sophos": get_provider_cutoff_days("sophos"),
        }
    )


@app.route("/api/settings", methods=["PUT"])
def api_update_settings():
    payload = request.get_json(silent=True) or {}

    # Backward compatible: old single setting payload still supported.
    legacy_enabled_present = "enable_recent_device_cutoff" in payload
    legacy_days_present = "recent_device_cutoff_days" in payload
    ninja_enabled_present = "enable_recent_device_cutoff_ninja" in payload
    sophos_enabled_present = "enable_recent_device_cutoff_sophos" in payload
    ninja_days_present = "recent_device_cutoff_days_ninja" in payload
    sophos_days_present = "recent_device_cutoff_days_sophos" in payload
    use_global_present = "use_global_device_cutoff" in payload

    if not any([legacy_enabled_present, legacy_days_present, ninja_enabled_present, sophos_enabled_present, ninja_days_present, sophos_days_present, use_global_present]):
        return jsonify({"error": "No cutoff settings were provided"}), 400

    def _parse_days(field_name: str, default_value: int) -> int:
        raw = payload.get(field_name, default_value)
        try:
            value = int(raw)
        except Exception:
            raise ValueError(f"{field_name} must be an integer")
        if value < 1 or value > 3650:
            raise ValueError(f"{field_name} must be between 1 and 3650")
        return value

    try:
        use_global = bool(payload.get("use_global_device_cutoff", use_global_device_cutoff()))
        legacy_enabled = bool(payload.get("enable_recent_device_cutoff", get_app_setting_bool("enable_recent_device_cutoff", ENABLE_RECENT_DEVICE_CUTOFF_DEFAULT)))
        legacy_days = _parse_days("recent_device_cutoff_days", get_recent_device_cutoff_days())
        ninja_enabled = bool(payload.get("enable_recent_device_cutoff_ninja", payload.get("enable_recent_device_cutoff", get_provider_cutoff_enabled("ninja"))))
        sophos_enabled = bool(payload.get("enable_recent_device_cutoff_sophos", payload.get("enable_recent_device_cutoff", get_provider_cutoff_enabled("sophos"))))
        ninja_days = _parse_days("recent_device_cutoff_days_ninja", payload.get("recent_device_cutoff_days", get_provider_cutoff_days("ninja")))
        sophos_days = _parse_days("recent_device_cutoff_days_sophos", payload.get("recent_device_cutoff_days", get_provider_cutoff_days("sophos")))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    if use_global:
        # Keep values consistent while in global mode so switching modes is predictable.
        ninja_enabled = legacy_enabled
        sophos_enabled = legacy_enabled
        ninja_days = legacy_days
        sophos_days = legacy_days

    set_app_setting("use_global_device_cutoff", "true" if use_global else "false")
    set_app_setting("enable_recent_device_cutoff", "true" if legacy_enabled else "false")
    set_app_setting("recent_device_cutoff_days", str(legacy_days))
    set_app_setting("enable_recent_device_cutoff_ninja", "true" if ninja_enabled else "false")
    set_app_setting("enable_recent_device_cutoff_sophos", "true" if sophos_enabled else "false")
    set_app_setting("recent_device_cutoff_days_ninja", str(ninja_days))
    set_app_setting("recent_device_cutoff_days_sophos", str(sophos_days))
    return jsonify(
        {
            "success": True,
            "use_global_device_cutoff": use_global,
            "enable_recent_device_cutoff": legacy_enabled,
            "recent_device_cutoff_days": legacy_days,
            "enable_recent_device_cutoff_ninja": ninja_enabled,
            "enable_recent_device_cutoff_sophos": sophos_enabled,
            "recent_device_cutoff_days_ninja": ninja_days,
            "recent_device_cutoff_days_sophos": sophos_days,
        }
    )


@app.route("/api/merge-mappings", methods=["GET"])
def api_merge_mappings():
    mappings = load_merge_mappings()
    items = [{"from_key": fk, "to_key": tk} for fk, tk in sorted(mappings.items(), key=lambda x: x[0])]
    return jsonify({"items": items, "count": len(items)})


@app.route("/api/platform-options", methods=["GET"])
def api_platform_options():
    include_linked = (request.args.get("include_linked") or "false").strip().lower() == "true"
    merge_mappings = load_merge_mappings()
    links = load_platform_links()
    linked_keys = set()
    for link in links:
        linked_keys.add(resolve_merge_key(str(link.get("ninja_key") or ""), merge_mappings))
        linked_keys.add(resolve_merge_key(str(link.get("sophos_key") or ""), merge_mappings))
    auto_paired_keys = set()

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT normalized_key, ninja_source_name, sophos_source_name
            FROM customers
            WHERE ninja_source_name IS NOT NULL
              AND TRIM(ninja_source_name) <> ''
              AND sophos_source_name IS NOT NULL
              AND TRIM(sophos_source_name) <> ''
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            nkey = resolve_merge_key(normalize_customer_name(str(row.get("ninja_source_name") or "")), merge_mappings)
            skey = resolve_merge_key(normalize_customer_name(str(row.get("sophos_source_name") or "")), merge_mappings)
            if nkey and skey and nkey == skey:
                auto_paired_keys.add(resolve_merge_key(str(row.get("normalized_key") or ""), merge_mappings))

        cur.execute(
            """
            SELECT DISTINCT ninja_source_name
            FROM customers
            WHERE ninja_source_name IS NOT NULL AND TRIM(ninja_source_name) <> ''
            ORDER BY ninja_source_name COLLATE NOCASE ASC
            """
        )
        ninja_all = [r[0] for r in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT sophos_source_name
            FROM customers
            WHERE sophos_source_name IS NOT NULL AND TRIM(sophos_source_name) <> ''
            ORDER BY sophos_source_name COLLATE NOCASE ASC
            """
        )
        sophos_all = [r[0] for r in cur.fetchall()]
        conn.close()

    if include_linked:
        ninja = ninja_all
        sophos = sophos_all
    else:
        ninja = [
            n for n in ninja_all
            if resolve_merge_key(normalize_customer_name(str(n)), merge_mappings) not in linked_keys
            and resolve_merge_key(normalize_customer_name(str(n)), merge_mappings) not in auto_paired_keys
        ]
        sophos = [
            s for s in sophos_all
            if resolve_merge_key(normalize_customer_name(str(s)), merge_mappings) not in linked_keys
            and resolve_merge_key(normalize_customer_name(str(s)), merge_mappings) not in auto_paired_keys
        ]

    return jsonify({"ninja": ninja, "sophos": sophos, "include_linked": include_linked})


@app.route("/api/platform-links", methods=["GET"])
def api_platform_links():
    links = load_platform_links()
    return jsonify({"items": links, "count": len(links)})


@app.route("/api/platform-links", methods=["POST"])
def api_create_platform_link():
    payload = request.get_json(silent=True) or {}
    ninja_name = str(payload.get("ninja_name") or "").strip()
    sophos_name = str(payload.get("sophos_name") or "").strip()
    canonical_name = str(payload.get("canonical_name") or "").strip()
    if not ninja_name or not sophos_name:
        return jsonify({"error": "ninja_name and sophos_name are required"}), 400

    merge_mappings = load_merge_mappings()
    ninja_key = resolve_merge_key(normalize_customer_name(ninja_name), merge_mappings)
    sophos_key = resolve_merge_key(normalize_customer_name(sophos_name), merge_mappings)
    if not ninja_key or not sophos_key:
        return jsonify({"error": "Uninja to normalize one or both platform names"}), 400
    if not canonical_name:
        canonical_name = ninja_name if len(ninja_name) >= len(sophos_name) else sophos_name

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM platform_links WHERE ninja_key = ? OR sophos_key = ?", (ninja_key, sophos_key))
        cur.execute(
            """
            INSERT INTO platform_links (ninja_key, sophos_key, canonical_name, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (ninja_key, sophos_key, canonical_name, utc_now_iso()),
        )
        cur.execute("SELECT id FROM platform_links WHERE ninja_key = ?", (ninja_key,))
        link_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

    collapse_summary = apply_platform_link_to_cached_data(link_id, ninja_key, sophos_key, canonical_name)
    trigger_sync_async("platform-link")
    return jsonify(
        {
            "success": True,
            "message": "Platform link saved, rows collapsed, and sync started",
            "link_id": link_id,
            "collapse_summary": collapse_summary,
        }
    ), 202


@app.route("/api/platform-links/<int:link_id>", methods=["DELETE"])
def api_delete_platform_link(link_id: int):
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM platform_links WHERE id = ?", (link_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "Platform link not found"}), 404
        cur.execute("DELETE FROM platform_links WHERE id = ?", (link_id,))
        conn.commit()
        conn.close()

    trigger_sync_async("platform-link-delete")
    return jsonify({"success": True, "message": "Platform link deleted and sync started"}), 202


@app.route("/api/merge-mappings", methods=["POST"])
def api_create_merge_mapping():
    payload = request.get_json(silent=True) or {}
    from_id_raw = payload.get("from_id")
    to_id_raw = payload.get("to_id")
    from_name = str(payload.get("from_name") or "").strip()
    to_name = str(payload.get("to_name") or "").strip()
    from_key = ""
    to_key = ""

    # Prefer ID-based merge so duplicate display names can still be merged safely.
    if from_id_raw is not None and to_id_raw is not None:
        try:
            from_id = int(from_id_raw)
            to_id = int(to_id_raw)
        except Exception:
            return jsonify({"error": "from_id and to_id must be integers"}), 400
        if from_id == to_id:
            return jsonify({"error": "Source and target must be different customers"}), 400

        from_customer = get_customer_by_id(from_id)
        to_customer = get_customer_by_id(to_id)
        if not from_customer or not to_customer:
            return jsonify({"error": "One or both customers were not found"}), 404

        from_key = str(from_customer.get("normalized_key") or "").strip()
        to_key = str(to_customer.get("normalized_key") or "").strip()
    else:
        if not from_name or not to_name:
            return jsonify({"error": "Provide from_id/to_id or from_name/to_name"}), 400
        from_key = normalize_customer_name(from_name)
        to_key = normalize_customer_name(to_name)

    if not from_key or not to_key:
        return jsonify({"error": "Uninja to resolve one or both customer keys"}), 400
    if from_key == to_key:
        return jsonify({"error": "Source and target already resolve to the same customer"}), 400

    existing = load_merge_mappings()
    if resolve_merge_key(to_key, existing) == from_key:
        return jsonify({"error": "Mapping would create a loop"}), 400

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO merge_mappings (from_key, to_key, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(from_key) DO UPDATE SET
                to_key = excluded.to_key,
                created_at = excluded.created_at
            """,
            (from_key, to_key, utc_now_iso()),
        )
        conn.commit()
        conn.close()

    merge_result = apply_merge_to_cached_data(from_key, to_key)
    trigger_sync_async("manual-merge")
    return jsonify(
        {
            "success": True,
            "message": "Merge applied and sync started",
            "details": {},
            "mapping": {"from_key": from_key, "to_key": to_key},
            "merge_result": merge_result,
        }
    ), 202


@app.route("/api/maintenance/dedupe-display-names", methods=["POST"])
def api_dedupe_display_names():
    summary = dedupe_customers_by_display_name()
    return jsonify({"success": True, "summary": summary}), 200


@app.route("/api/maintenance/purge-empty-duplicates", methods=["POST"])
def api_purge_empty_duplicates():
    summary = purge_empty_duplicate_customer_rows()
    trigger_sync_async("maintenance-purge-empty-duplicates")
    return jsonify({"success": True, "message": "Empty duplicate cleanup complete. Sync started.", "summary": summary}), 202


@app.route("/api/maintenance/reset-merges", methods=["POST"])
def api_reset_merges():
    reset_merge_state_and_cached_data()
    trigger_sync_async("reset-merges")
    return jsonify({"success": True, "message": "Merge mappings cleared, cache reset, and sync started"}), 202


if __name__ == "__main__":
    init_db()

    # Prime cache on first run so the UI has data quickly.
    if not has_cached_rows():
        run_sync("startup")

    scheduler = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler.start()

    app.run(host="0.0.0.0", port=PORT)
