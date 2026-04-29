"""
Run this ONCE to create all 6 tabs with correct headers in your Google Sheet.
Usage:
  pip install gspread google-auth
  export GOOGLE_CREDENTIALS_JSON='<paste your service account JSON>'
  export GOOGLE_SHEET_ID='<your sheet id>'
  python setup_sheets.py
"""
import os, json
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID          = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

TABS = {
    "Results": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "NSE Dates",
        "First Disclosure", "NSE Link", "Screener Link"
    ],
    "Investors Meet": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "Investor Name",
        "NSE Dates", "First Disclosure", "NSE Link", "Screener Link"
    ],
    "Acquisition & Merger": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "NSE Dates",
        "First Disclosure", "NSE Link", "Screener Link"
    ],
    "Demerger": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "NSE Dates",
        "First Disclosure", "NSE Link", "Screener Link"
    ],
    "Change in Management": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "NSE Dates",
        "First Disclosure", "NSE Link", "Screener Link"
    ],
    "Others": [
        "Logged at", "Company Name", "Symbol", "Category",
        "Title", "Full Subject/Topic", "NSE Dates",
        "First Disclosure", "NSE Link", "Screener Link"
    ],
}

def main():
    creds_info = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open_by_key(SHEET_ID)
    existing = [ws.title for ws in spreadsheet.worksheets()]

    for tab_name, headers in TABS.items():
        if tab_name in existing:
            ws = spreadsheet.worksheet(tab_name)
            print(f"Tab '{tab_name}' already exists — updating headers.")
        else:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
            print(f"Created tab '{tab_name}'.")

        # Write headers to row 1
        ws.update([headers], "A1")

        # Bold the header row
        ws.format("1:1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.18, "green": 0.36, "blue": 0.6}
        })

    # Remove default 'Sheet1' if still present and empty
    try:
        default = spreadsheet.worksheet("Sheet1")
        if default.row_count <= 1:
            spreadsheet.del_worksheet(default)
            print("Removed default 'Sheet1'.")
    except gspread.exceptions.WorksheetNotFound:
        pass

    print("\n✅ All tabs created and headers set. You're good to go!")

if __name__ == "__main__":
    main()
