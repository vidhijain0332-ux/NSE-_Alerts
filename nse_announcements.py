import os
import json
import time
import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import httpx

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Timezone ──────────────────────────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")

# ── Environment Variables ─────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
SHEET_ID             = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDS_JSON    = os.environ["GOOGLE_CREDENTIALS_JSON"]   # full JSON string

# 6 channel IDs
CHANNEL_RESULTS      = os.environ["TELEGRAM_CHANNEL_RESULTS"]
CHANNEL_INVESTORS    = os.environ["TELEGRAM_CHANNEL_INVESTORS"]
CHANNEL_ACQ          = os.environ["TELEGRAM_CHANNEL_ACQ"]
CHANNEL_DEMERGER     = os.environ["TELEGRAM_CHANNEL_DEMERGER"]
CHANNEL_MANAGEMENT   = os.environ["TELEGRAM_CHANNEL_MANAGEMENT"]
CHANNEL_OTHERS       = os.environ["TELEGRAM_CHANNEL_OTHERS"]

SEEN_IDS_FILE = "seen_ids.json"

# ── Category Config ───────────────────────────────────────────────────────────
CATEGORIES = {
    "Results": {
        "sheet_tab": "Results",
        "channel":   CHANNEL_RESULTS,
        "keywords":  [
            "financial results", "quarterly results", "half yearly results",
            "annual results", "unaudited", "audited", "board meeting",
            "q1 results", "q2 results", "q3 results", "q4 results",
            "first quarter", "second quarter", "third quarter", "fourth quarter",
            "half year", "full year", "standalone results", "consolidated results",
            "limited review", "financial statements"
        ],
        "exclude_keywords": []
    },
    "Investors Meet": {
        "sheet_tab": "Investors Meet",
        "channel":   CHANNEL_INVESTORS,
        "keywords":  [
            "investor meet", "investor meeting", "investors meet",
            "analyst meet", "analyst meeting", "conference call",
            "earnings call", "transcript", "recording", "webinar",
            "ndr", "non-deal roadshow", "management meet", "management meeting",
            "jefferies", "clsa", "citi", "citibank", "citigroup",
            "bofa", "bank of america", "goldman sachs", "jp morgan", "jpmorgan",
            "morgan stanley", "bandhan small cap", "hdfc mutual fund",
            "motilal oswal", "investor presentation", "roadshow"
        ],
        "exclude_keywords": []
    },
    "Acquisition & Merger": {
        "sheet_tab": "Acquisition & Merger",
        "channel":   CHANNEL_ACQ,
        "keywords":  [
            "acquisition", "merger", "amalgamation", "takeover",
            "share purchase agreement", "spa", "letter of intent", "loi",
            "due diligence", "term sheet", "scheme of arrangement",
            "business transfer", "slump sale", "asset acquisition",
            "stake acquisition", "open offer", "delisting", "buy back",
            "buyback", "strategic investment", "joint venture"
        ],
        "exclude_keywords": [
            "publication", "published", "book", "newspaper", "magazine",
            "journal", "advertisement", "advertise", "notice for publication"
        ]
    },
    "Demerger": {
        "sheet_tab": "Demerger",
        "channel":   CHANNEL_DEMERGER,
        "keywords":  [
            "demerger", "de-merger", "spin off", "spinoff", "spin-off",
            "carve-out", "carve out", "composite scheme",
            "separate listing", "scheme of arrangement",
            "hive off", "hive-off", "restructuring", "demerge"
        ],
        "exclude_keywords": []
    },
    "Change in Management": {
        "sheet_tab": "Change in Management",
        "channel":   CHANNEL_MANAGEMENT,
        "keywords":  [
            "resignation", "resigns", "resigned",
            "change in management", "change in directorate",
            "appointment", "appointed", "new director", "new ceo",
            "new md", "managing director", "chief executive",
            "director appointed", "director resigned", "board change",
            "key managerial", "kmp", "whole time director",
            "independent director", "non executive director",
            "cessation", "vacates", "steps down", "stepping down",
            "re-appointment", "reappointment", "additional director",
            "company secretary", "chief financial officer", "cfo appointed",
            "cfo resigned", "chairman", "vice chairman"
        ],
        "exclude_keywords": []
    }
    # "Others" is the catch-all — no keywords needed
}

# ── Seen IDs ──────────────────────────────────────────────────────────────────
def load_seen_ids() -> set:
    if os.path.exists(SEEN_IDS_FILE):
        with open(SEEN_IDS_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("seen_ids", []))
    return set()

def save_seen_ids(seen: set):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump({"seen_ids": list(seen)}, f)

# ── NSE Fetch ─────────────────────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
}

def get_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    # Hit the main page first to get cookies
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(2)
        session.get(
            "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            timeout=15
        )
        time.sleep(1)
    except Exception as e:
        log.warning(f"Session warm-up failed: {e}")
    return session

def fetch_nse_announcements(session: requests.Session) -> list:
    """Fetch last 24h announcements from NSE."""
    url = (
        "https://www.nseindia.com/api/corporate-announcements"
        "?index=equities&from_date=&to_date=&category=&symbol="
    )
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        log.error(f"NSE fetch error: {e}")
        return []

def screener_link(symbol: str) -> str:
    return f"https://www.screener.in/company/{symbol}/announcements/"

def nse_link(symbol: str, an_id: str) -> str:
    return f"https://www.nseindia.com/companies-listing/corporate-filings-announcements"

# ── Categorise ────────────────────────────────────────────────────────────────
def categorise(subject: str, desc: str) -> str:
    text = (subject + " " + desc).lower()

    # Check exclusions for Acq & Merger first
    acq_excluded = any(kw in text for kw in CATEGORIES["Acquisition & Merger"]["exclude_keywords"])

    for cat, cfg in CATEGORIES.items():
        if any(kw in text for kw in cfg["keywords"]):
            if cat == "Acquisition & Merger" and acq_excluded:
                continue
            # Demerger before Acquisition (more specific)
            if cat == "Acquisition & Merger":
                if any(kw in text for kw in CATEGORIES["Demerger"]["keywords"]):
                    continue  # will be caught by Demerger
            return cat

    return "Others"

def extract_investor_name(subject: str, desc: str) -> str:
    text = (subject + " " + desc).lower()
    brokerages = [
        "Jefferies", "CLSA", "Citi", "BofA", "Bank of America",
        "Goldman Sachs", "JP Morgan", "Morgan Stanley",
        "Bandhan Small Cap", "HDFC Mutual Fund", "Motilal Oswal"
    ]
    found = []
    for b in brokerages:
        if b.lower() in text:
            found.append(b)
    return ", ".join(found) if found else ""

# ── Google Sheets ─────────────────────────────────────────────────────────────
def get_sheets_client():
    creds_info = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def append_to_sheet(client, tab_name: str, row: list):
    try:
        sheet = client.open_by_key(SHEET_ID)
        ws = sheet.worksheet(tab_name)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        log.error(f"Sheet append error ({tab_name}): {e}")

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(channel_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": channel_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        if not r.ok:
            log.error(f"Telegram error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"Telegram send error: {e}")

def format_telegram_message(ann: dict, category: str) -> str:
    symbol   = ann.get("symbol", "")
    company  = ann.get("corp_name", ann.get("company_name", ""))
    subject  = ann.get("subject", ann.get("desc", ""))
    an_date  = ann.get("sort_date", ann.get("an_dt", ""))
    nse_url  = ann.get("attchmntFile", "")
    sc_link  = screener_link(symbol)

    lines = [
        f"<b>📢 {category}</b>",
        f"<b>Company:</b> {company} ({symbol})",
        f"<b>Subject:</b> {subject}",
        f"<b>Date:</b> {an_date}",
    ]
    if nse_url:
        lines.append(f"<b>NSE:</b> <a href='{nse_url}'>Download</a>")
    lines.append(f"<b>Screener:</b> <a href='{sc_link}'>View</a>")
    return "\n".join(lines)

# ── Parse date from NSE ───────────────────────────────────────────────────────
def parse_nse_date(ann: dict) -> datetime | None:
    for field in ("sort_date", "an_dt", "bm_dt", "exchdisstime"):
        raw = ann.get(field, "")
        if not raw:
            continue
        for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S",
                    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                return dt.replace(tzinfo=IST)
            except ValueError:
                continue
    return None

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("=== NSE Bot run started ===")
    seen_ids = load_seen_ids()

    session      = get_nse_session()
    announcements = fetch_nse_announcements(session)
    log.info(f"Fetched {len(announcements)} announcements from NSE")

    sheets_client = get_sheets_client()

    now_ist   = datetime.now(IST)
    cutoff    = now_ist - timedelta(hours=24)

    new_count = 0

    for ann in announcements:
        # ── Unique ID ──
        an_id = str(ann.get("an_id", ann.get("seqNo", ann.get("sort_date", ""))))
        symbol = ann.get("symbol", "")
        unique_key = f"{symbol}_{an_id}"

        if unique_key in seen_ids:
            continue

        # ── Date filter ──
        dt = parse_nse_date(ann)
        if dt and dt < cutoff:
            seen_ids.add(unique_key)
            continue

        # ── Extract fields ──
        company  = ann.get("corp_name", ann.get("company_name", ""))
        subject  = ann.get("subject", ann.get("desc", ""))
        desc     = ann.get("attchmntText", ann.get("body", ""))
        nse_date = ann.get("sort_date", ann.get("an_dt", ""))
        first_disc = ann.get("exchdisstime", "")
        nse_file_url = ann.get("attchmntFile", "")
        sc_url   = screener_link(symbol)

        category = categorise(subject, desc)
        cfg      = CATEGORIES.get(category, {"sheet_tab": "Others", "channel": CHANNEL_OTHERS})

        # ── Build sheet row ──
        logged_at = now_ist.strftime("%d-%m-%Y %H:%M")

        if category == "Investors Meet":
            investor_name = extract_investor_name(subject, desc)
            row = [
                logged_at, company, symbol, category, subject,
                desc[:500] if desc else subject,
                investor_name, nse_date, first_disc, nse_file_url, sc_url
            ]
        else:
            row = [
                logged_at, company, symbol, category, subject,
                desc[:500] if desc else subject,
                nse_date, first_disc, nse_file_url, sc_url
            ]

        tab = cfg["sheet_tab"] if isinstance(cfg, dict) else "Others"
        channel = cfg["channel"] if isinstance(cfg, dict) else CHANNEL_OTHERS

        append_to_sheet(sheets_client, tab, row)

        # ── Telegram message ──
        msg = format_telegram_message(ann, category)
        send_telegram(channel, msg)

        seen_ids.add(unique_key)
        new_count += 1
        time.sleep(0.5)  # polite rate-limit

    save_seen_ids(seen_ids)
    log.info(f"=== Run complete. {new_count} new announcements processed ===")

if __name__ == "__main__":
    run()
