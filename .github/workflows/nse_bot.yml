import os
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import gspread
from google.oauth2.service_account import Credentials

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# ── Environment Variables ─────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
SHEET_ID             = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDS_JSON    = os.environ["GOOGLE_CREDENTIALS_JSON"]

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
}

FIRST_DISC_CATEGORIES = {"Acquisition & Merger", "Demerger", "Change in Management"}

FIRST_DISC_POSITIVE = [
    "intimation", "first disclosure", "initial disclosure",
    "proposed", "intends to", "intention to", "considering",
    "exploring", "letter of intent", "loi", "term sheet",
    "mou", "memorandum of understanding", "in-principle",
    "board has approved", "board approved", "board has decided",
    "entered into", "signing", "signed", "execution of",
    "agreement signed", "agreement entered"
]

FIRST_DISC_NEGATIVE = [
    "outcome", "completion", "completed", "effective date",
    "nclt", "tribunal", "court approval", "approved by",
    "record date", "allotment", "post merger", "post acquisition",
    "pursuant to", "further to", "follow-up", "followup",
    "subsequent", "update on", "status of", "progress of"
]

# ── Seen IDs — dual key system ────────────────────────────────────────────────
def load_seen_ids() -> set:
    if os.path.exists(SEEN_IDS_FILE):
        try:
            with open(SEEN_IDS_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("seen_ids", []))
        except Exception as e:
            log.warning(f"Could not load seen_ids.json: {e} — starting fresh")
    return set()

def save_seen_ids(seen: set):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump({"seen_ids": list(seen)}, f, indent=2)

def make_content_hash(symbol: str, subject: str, date_str: str) -> str:
    """
    Secondary dedup key based on content.
    Catches cases where an_id differs across endpoints for the same announcement.
    """
    raw = f"{symbol.lower()}|{subject.lower().strip()}|{date_str}"
    return "h_" + hashlib.md5(raw.encode()).hexdigest()[:16]

# ── NSE Session ───────────────────────────────────────────────────────────────
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
    now_ist = datetime.now(IST)
    from_dt = (now_ist - timedelta(days=2)).strftime("%d-%m-%Y")
    to_dt   = now_ist.strftime("%d-%m-%Y")

    endpoints = [
        "https://www.nseindia.com/api/corporate-announcements?index=equities",
        f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_dt}&to_date={to_dt}",
        "https://www.nseindia.com/api/corporate-announcements?index=sme",
    ]

    all_items = []
    seen_keys = set()

    for url in endpoints:
        try:
            resp = session.get(url, timeout=25)
            resp.raise_for_status()
            data  = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            log.info(f"Endpoint returned {len(items)} items: {url[:80]}")
            for item in items:
                an_id  = str(item.get("an_id", item.get("seqNo", "")))
                symbol = str(item.get("symbol", ""))
                key    = f"{symbol}_{an_id}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_items.append(item)
            time.sleep(1.5)
        except Exception as e:
            log.warning(f"Endpoint failed ({url[:70]}): {e}")

    log.info(f"Total unique announcements after merge: {len(all_items)}")
    return all_items

# ── Field extractors ──────────────────────────────────────────────────────────
def extract_company_name(ann: dict) -> str:
    for field in ("corp_name", "companyName", "company_name",
                  "sm_name", "company", "issuerName", "name"):
        val = ann.get(field, "")
        if val and str(val).strip():
            return str(val).strip()
    return ann.get("symbol", "Unknown")

def is_first_disclosure(subject: str, desc: str, category: str) -> str:
    if category not in FIRST_DISC_CATEGORIES:
        return ""
    text = (subject + " " + desc).lower()
    if any(kw in text for kw in FIRST_DISC_NEGATIVE):
        return "No"
    if any(kw in text for kw in FIRST_DISC_POSITIVE):
        return "Yes"
    return "Yes"

def screener_link(symbol: str) -> str:
    return f"https://www.screener.in/company/{symbol}/announcements/"

def categorise(subject: str, desc: str) -> str:
    text = (subject + " " + desc).lower()
    acq_excluded = any(
        kw in text for kw in CATEGORIES["Acquisition & Merger"]["exclude_keywords"]
    )
    for cat, cfg in CATEGORIES.items():
        if any(kw in text for kw in cfg["keywords"]):
            if cat == "Acquisition & Merger" and acq_excluded:
                continue
            if cat == "Acquisition & Merger":
                if any(kw in text for kw in CATEGORIES["Demerger"]["keywords"]):
                    continue
            return cat
    return "Others"

def extract_investor_name(subject: str, desc: str) -> str:
    text = (subject + " " + desc).lower()
    brokerages = [
        "Jefferies", "CLSA", "Citi", "BofA", "Bank of America",
        "Goldman Sachs", "JP Morgan", "Morgan Stanley",
        "Bandhan Small Cap", "HDFC Mutual Fund", "Motilal Oswal"
    ]
    found = [b for b in brokerages if b.lower() in text]
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
        ws    = sheet.worksheet(tab_name)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        log.error(f"Sheet append error ({tab_name}): {e}")

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(channel_id: str, text: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
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

def format_telegram_message(ann: dict, category: str,
                             company: str, first_disc_flag: str) -> str:
    symbol  = ann.get("symbol", "")
    subject = ann.get("subject", ann.get("desc", ""))
    an_date = ann.get("sort_date", ann.get("an_dt", ""))
    nse_url = ann.get("attchmntFile", "")
    sc_link = screener_link(symbol)

    lines = [
        f"<b>📢 {category}</b>",
        f"<b>Company:</b> {company} ({symbol})",
        f"<b>Subject:</b> {subject}",
        f"<b>Date:</b> {an_date}",
    ]
    if first_disc_flag:
        lines.append(f"<b>First Disclosure:</b> {first_disc_flag}")
    if nse_url:
        lines.append(f"<b>NSE:</b> <a href='{nse_url}'>Download</a>")
    lines.append(f"<b>Screener:</b> <a href='{sc_link}'>View</a>")
    return "\n".join(lines)

# ── Parse date ────────────────────────────────────────────────────────────────
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
    log.info(f"Loaded {len(seen_ids)} seen IDs from file")

    session       = get_nse_session()
    announcements = fetch_nse_announcements(session)

    sheets_client = get_sheets_client()

    now_ist = datetime.now(IST)
    cutoff  = now_ist - timedelta(hours=24)

    new_count  = 0
    skip_count = 0

    for ann in announcements:
        symbol  = ann.get("symbol", "")
        an_id   = str(ann.get("an_id", ann.get("seqNo", ann.get("sort_date", ""))))
        subject = ann.get("subject", ann.get("desc", ""))
        nse_date = ann.get("sort_date", ann.get("an_dt", ""))

        # ── Primary key: symbol + an_id ───────────────────────────────────
        primary_key = f"{symbol}_{an_id}"

        # ── Secondary key: content hash (catches same ann with diff an_id) ─
        content_key = make_content_hash(symbol, subject, nse_date)

        if primary_key in seen_ids or content_key in seen_ids:
            skip_count += 1
            continue

        # ── Date filter ───────────────────────────────────────────────────
        dt = parse_nse_date(ann)
        if dt and dt < cutoff:
            seen_ids.add(primary_key)
            seen_ids.add(content_key)
            continue

        # ── Extract fields ────────────────────────────────────────────────
        company      = extract_company_name(ann)
        desc         = ann.get("attchmntText", ann.get("body", ""))
        nse_file_url = ann.get("attchmntFile", "")
        sc_url       = screener_link(symbol)

        category        = categorise(subject, desc)
        first_disc_flag = is_first_disclosure(subject, desc, category)

        cfg     = CATEGORIES.get(category, {"sheet_tab": "Others", "channel": CHANNEL_OTHERS})
        tab     = cfg["sheet_tab"] if isinstance(cfg, dict) else "Others"
        channel = cfg["channel"]   if isinstance(cfg, dict) else CHANNEL_OTHERS

        logged_at = now_ist.strftime("%d-%m-%Y %H:%M")

        if category == "Investors Meet":
            investor_name = extract_investor_name(subject, desc)
            row = [
                logged_at, company, symbol, category, subject,
                desc[:500] if desc else subject,
                investor_name, nse_date, first_disc_flag, nse_file_url, sc_url
            ]
        else:
            row = [
                logged_at, company, symbol, category, subject,
                desc[:500] if desc else subject,
                nse_date, first_disc_flag, nse_file_url, sc_url
            ]

        append_to_sheet(sheets_client, tab, row)

        msg = format_telegram_message(ann, category, company, first_disc_flag)
        send_telegram(channel, msg)

        # Mark both keys as seen
        seen_ids.add(primary_key)
        seen_ids.add(content_key)
        new_count += 1
        log.info(f"[{category}] {company} ({symbol}) — {subject[:70]}")
        time.sleep(0.4)

    save_seen_ids(seen_ids)
    log.info(f"=== Run complete. New: {new_count} | Skipped (already seen): {skip_count} ===")

if __name__ == "__main__":
    run()
