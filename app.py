import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
from flask import Flask, jsonify, request, send_from_directory


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(APP_DIR, "data", "billing_cache.db"))
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "180"))
PORT = int(os.environ.get("PORT", "8083"))

NABLE_TOKEN = os.environ.get("NABLE_TOKEN", "")
NABLE_API_BASE = os.environ.get("NABLE_API_BASE", "https://ncod153.n-able.com")
NABLE_AUTH_PATH = os.environ.get("NABLE_AUTH_PATH", "/api/auth/authenticate")
NABLE_DEVICES_PATH = os.environ.get("NABLE_DEVICES_PATH", "/api/devices")

SOPHOS_CLIENT_ID = os.environ.get("SOPHOS_CLIENT_ID", "")
SOPHOS_CLIENT_SECRET = os.environ.get("SOPHOS_CLIENT_SECRET", "")
SOPHOS_TOKEN_URL = os.environ.get(
    "SOPHOS_TOKEN_URL",
    "https://id.sophos.com/api/v2/oauth2/token",
)

REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = float(os.environ.get("RETRY_DELAY_SECONDS", "1.5"))


app = Flask(__name__, static_folder="assets")
db_lock = threading.Lock()
sync_lock = threading.Lock()

runtime_state = {
    "sync_running": False,
    "last_sync_started_at": None,
    "last_sync_finished_at": None,
    "last_sync_status": "never",
    "last_sync_error": None,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_customer_name(name: str) -> str:
    lowered = (name or "").strip().lower()
    lowered = re.sub(r"[^\w\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    tokens = [t for t in lowered.split(" ") if t]
    suffixes = {"ltd", "limited", "llc", "inc", "corp", "co"}
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    return " ".join(tokens)


def ensure_db_dir() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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
                nable_source_name TEXT,
                sophos_source_name TEXT
            );

            CREATE TABLE IF NOT EXISTS customer_counts_latest (
                customer_id INTEGER PRIMARY KEY,
                nable_count INTEGER NOT NULL DEFAULT 0,
                sophos_count INTEGER NOT NULL DEFAULT 0,
                has_nable INTEGER NOT NULL DEFAULT 0,
                has_sophos INTEGER NOT NULL DEFAULT 0,
                last_synced_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            );

            CREATE TABLE IF NOT EXISTS customer_count_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                nable_count INTEGER NOT NULL DEFAULT 0,
                sophos_count INTEGER NOT NULL DEFAULT 0,
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
            """
        )
        conn.commit()
        conn.close()


def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Request failed after retries for {url}: {last_error}")


def extract_nable_customer_name(device: Dict) -> str:
    candidates = [
        device.get("customerName"),
        device.get("customer"),
        device.get("clientName"),
        (device.get("client") or {}).get("name") if isinstance(device.get("client"), dict) else None,
        (device.get("site") or {}).get("customerName") if isinstance(device.get("site"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "Unknown Customer"


def extract_devices_from_response(payload) -> List[Dict]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("devices", "items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [v for v in value if isinstance(v, dict)]
    return []


def fetch_nable_access_token() -> str:
    if not NABLE_TOKEN:
        raise RuntimeError("Missing NABLE_TOKEN")

    auth_url = f"{NABLE_API_BASE.rstrip('/')}{NABLE_AUTH_PATH}"
    response = request_with_retry(
        "POST",
        auth_url,
        headers={"Authorization": f"Bearer {NABLE_TOKEN}", "Accept": "application/json"},
    )
    payload = response.json()
    token = ((payload.get("tokens") or {}).get("access") or {}).get("token")
    if not token:
        raise RuntimeError("N-able authenticate response missing access token")
    return token


def fetch_all_nable_devices(access_token: str) -> List[Dict]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    devices: List[Dict] = []
    next_url = f"{NABLE_API_BASE.rstrip('/')}{NABLE_DEVICES_PATH}?pageSize=1000"

    while next_url:
        response = request_with_retry("GET", next_url, headers=headers)
        payload = response.json()
        devices.extend(extract_devices_from_response(payload))

        next_page = ((payload.get("_links") or {}).get("nextPage")) if isinstance(payload, dict) else None
        if next_page:
            next_url = f"{NABLE_API_BASE.rstrip('/')}{next_page}"
        else:
            next_url = None

    return devices


def fetch_nable_counts() -> Dict[str, Dict]:
    access_token = fetch_nable_access_token()
    devices = fetch_all_nable_devices(access_token)

    counts: Dict[str, Dict] = {}
    for device in devices:
        raw_name = extract_nable_customer_name(device)
        normalized = normalize_customer_name(raw_name)
        if not normalized:
            continue
        if normalized not in counts:
            counts[normalized] = {"display_name": raw_name, "count": 0}
        counts[normalized]["count"] += 1
        if len(raw_name) > len(counts[normalized]["display_name"]):
            counts[normalized]["display_name"] = raw_name

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
    return token


def fetch_sophos_counts() -> Dict[str, Dict]:
    token = fetch_sophos_token()
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

    counts: Dict[str, Dict] = {}
    for tenant in tenants:
        tenant_id = tenant.get("id")
        tenant_name = tenant.get("name", "Unknown Tenant")
        if not tenant_id:
            continue

        tenant_api_host = (tenant.get("apiHost") or "https://api.central.sophos.com").rstrip("/")
        endpoint_url = f"{tenant_api_host}/endpoint/v1/endpoints"
        count = 0
        try:
            endpoint_resp = request_with_retry(
                "GET",
                endpoint_url,
                headers={**headers, "X-Tenant-ID": tenant_id},
                params={"pageSize": 1, "view": "full", "pageTotal": "true"},
            )
            endpoint_payload = endpoint_resp.json()
            pages = endpoint_payload.get("pages") or {}
            count = pages.get("total") or len(endpoint_payload.get("items") or [])
            if not isinstance(count, int):
                count = 0
        except Exception:
            count = 0

        normalized = normalize_customer_name(tenant_name)
        if not normalized:
            continue

        if normalized not in counts:
            counts[normalized] = {"display_name": tenant_name, "count": 0}
        counts[normalized]["count"] += count
        if len(tenant_name) > len(counts[normalized]["display_name"]):
            counts[normalized]["display_name"] = tenant_name

    return counts


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


def upsert_counts(nable_counts: Dict[str, Dict], sophos_counts: Dict[str, Dict], synced_at: str) -> None:
    all_keys = sorted(set(nable_counts.keys()) | set(sophos_counts.keys()))
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()

        for key in all_keys:
            nable_entry = nable_counts.get(key)
            sophos_entry = sophos_counts.get(key)

            display_name = None
            if nable_entry and sophos_entry:
                display_name = nable_entry["display_name"] if len(nable_entry["display_name"]) >= len(sophos_entry["display_name"]) else sophos_entry["display_name"]
            elif nable_entry:
                display_name = nable_entry["display_name"]
            elif sophos_entry:
                display_name = sophos_entry["display_name"]
            else:
                display_name = key

            nable_source_name = nable_entry["display_name"] if nable_entry else None
            sophos_source_name = sophos_entry["display_name"] if sophos_entry else None
            nable_count = int(nable_entry["count"]) if nable_entry else 0
            sophos_count = int(sophos_entry["count"]) if sophos_entry else 0
            has_nable = 1 if nable_count > 0 else 0
            has_sophos = 1 if sophos_count > 0 else 0

            cur.execute(
                """
                INSERT INTO customers (display_name, normalized_key, nable_source_name, sophos_source_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(normalized_key) DO UPDATE SET
                    display_name=excluded.display_name,
                    nable_source_name=excluded.nable_source_name,
                    sophos_source_name=excluded.sophos_source_name
                """,
                (display_name, key, nable_source_name, sophos_source_name),
            )
            cur.execute("SELECT id FROM customers WHERE normalized_key = ?", (key,))
            customer_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO customer_counts_latest
                    (customer_id, nable_count, sophos_count, has_nable, has_sophos, last_synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    nable_count=excluded.nable_count,
                    sophos_count=excluded.sophos_count,
                    has_nable=excluded.has_nable,
                    has_sophos=excluded.has_sophos,
                    last_synced_at=excluded.last_synced_at
                """,
                (customer_id, nable_count, sophos_count, has_nable, has_sophos, synced_at),
            )
            cur.execute(
                """
                INSERT INTO customer_count_history (customer_id, nable_count, sophos_count, captured_at)
                VALUES (?, ?, ?, ?)
                """,
                (customer_id, nable_count, sophos_count, synced_at),
            )

        conn.commit()
        conn.close()


def run_sync(trigger: str = "scheduled") -> Tuple[bool, str]:
    if not sync_lock.acquire(blocking=False):
        return False, "Sync already in progress"

    started_at = utc_now_iso()
    runtime_state["sync_running"] = True
    runtime_state["last_sync_started_at"] = started_at
    runtime_state["last_sync_status"] = "running"
    runtime_state["last_sync_error"] = None

    try:
        nable_counts = fetch_nable_counts()
        sophos_counts = fetch_sophos_counts()
        synced_at = utc_now_iso()
        upsert_counts(nable_counts, sophos_counts, synced_at)
        runtime_state["last_sync_finished_at"] = synced_at
        runtime_state["last_sync_status"] = "ok"
        write_sync_run(started_at, synced_at, "ok", None)
        return True, f"Sync complete ({trigger})"
    except Exception as exc:
        finished_at = utc_now_iso()
        message = str(exc)
        runtime_state["last_sync_finished_at"] = finished_at
        runtime_state["last_sync_status"] = "error"
        runtime_state["last_sync_error"] = message
        write_sync_run(started_at, finished_at, "error", message)
        return False, message
    finally:
        runtime_state["sync_running"] = False
        sync_lock.release()


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


@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(os.path.join(APP_DIR, "assets"), filename)


@app.route("/api/customers", methods=["GET"])
def api_customers():
    search = (request.args.get("search") or "").strip().lower()
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.display_name,
                c.nable_source_name,
                c.sophos_source_name,
                l.nable_count,
                l.sophos_count,
                l.has_nable,
                l.has_sophos,
                l.last_synced_at
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            ORDER BY c.display_name COLLATE NOCASE ASC
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()

    if search:
        rows = [r for r in rows if search in (r["display_name"] or "").lower()]

    return jsonify({"items": rows, "count": len(rows)})


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
                c.nable_source_name,
                c.sophos_source_name,
                l.nable_count,
                l.sophos_count,
                l.has_nable,
                l.has_sophos,
                l.last_synced_at
            FROM customers c
            INNER JOIN customer_counts_latest l ON l.customer_id = c.id
            WHERE c.id = ?
            """,
            (customer_id,),
        )
        row = cur.fetchone()
        conn.close()

    if not row:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(dict(row))


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
    success, message = run_sync("manual")
    code = 200 if success else 500
    return jsonify({"success": success, "message": message}), code


if __name__ == "__main__":
    init_db()

    # Prime cache on first run so the UI has data quickly.
    if not has_cached_rows():
        run_sync("startup")

    scheduler = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler.start()

    app.run(host="0.0.0.0", port=PORT)
