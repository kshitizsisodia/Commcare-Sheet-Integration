from flask import Flask, request, jsonify, abort
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Custom rate limiting (10 requests per minute per IP)
request_counts = defaultdict(list)

@app.before_request
def rate_limit():
    client_ip = request.remote_addr
    current_time = time.time()

    # Initialize or clean up old requests
    request_counts[client_ip] = [
        t for t in request_counts[client_ip] if current_time - t < 60
    ]

    # Enforce rate limit
    if len(request_counts[client_ip]) >= 10:
        abort(429, description="Too many requests. Please try again later.")
    request_counts[client_ip].append(current_time)

# CommCare API details
base_url = "https://india.commcarehq.org/a/kangaroo-mother-care-ansh/api/v0.5/form/"
api_key = os.getenv("COMMCARE_API_KEY")
username = os.getenv("COMMCARE_USERNAME")

if not api_key or not username:
    raise ValueError("COMMCARE_API_KEY or COMMCARE_USERNAME environment variable is missing")

headers = {"Authorization": f"ApiKey {username}:{api_key}"}

# Google Sheets credentials
creds_content = os.getenv("GOOGLE_SHEETS_CRED")
if not creds_content:
    raise ValueError("Environment variable GOOGLE_SHEETS_CRED is not set or empty")

# Write credentials to a file
credentials_path = "google_sheet_cred.json"
with open(credentials_path, "w") as f:
    f.write(creds_content)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(creds)

# Google Sheet name
sheet_name = "CommCare Realtime"
try:
    spreadsheet = client.open(sheet_name)
except gspread.exceptions.SpreadsheetNotFound:
    spreadsheet = client.create(sheet_name)

# API Token
API_TOKEN = "securedata@ansh123"

@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        return jsonify({"status": "POST request received at /"})
    return jsonify({"status": "CommCare Integration API is running!"})

# Fetch CommCare data
def fetch_commcare_data(xmlns):
    limit = 1000
    offset = 0
    all_data = []

    while True:
        api_url = f"{base_url}?xmlns={xmlns}&limit={limit}&offset={offset}"
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            records = data.get("objects", [])
            all_data.extend(records)
            if not data.get("meta", {}).get("next"):
                break
            offset += limit
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return pd.json_normalize(all_data) if all_data else pd.DataFrame()

# Clean DataFrame
def clean_dataframe(df):
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.fillna("")
    return df

# Update Google Sheets
@app.route("/update_sheets", methods=["POST"])
def update_sheets():
    if request.headers.get("Authorization") != f"Bearer {API_TOKEN}":
        abort(403)

    forms = [
        {"xmlns": "http://openrosa.org/formdesigner/65304B1B-FF8C-4683-8026-B935FD0DC674", "tab_name": "Cleaning Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/9EB06393-8DBE-4180-B537-A564850798B4", "tab_name": "KC Inventory Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/928FA710-2CC8-4348-8382-82E6A96EF714", "tab_name": "KMC Furnishing Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/855C2642-C2C8-4D86-8374-CEBBD5E8AC77", "tab_name": "Cleaning Checklist KMC Program"},
        {"xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1", "tab_name": "Fire a Hospital Staff"},
        {"xmlns": "http://openrosa.org/formdesigner/81FC2C13-CD6F-4F2A-BCBF-98C8466F0A3C", "tab_name": "File a damage/replacement"},
        {"xmlns": "http://openrosa.org/formdesigner/8C12ABAA-C695-46FB-A21F-B67612866DAE", "tab_name": "Validation of Weighting Process"},
        {"xmlns": "http://openrosa.org/formdesigner/6B79AADB-7492-4FF9-8389-C0A4D1AA6987", "tab_name": "Case Observations"},
        {"xmlns": "http://openrosa.org/formdesigner/40AF78C9-BE3E-4669-96DC-567FFFED09C0", "tab_name": "Phone Follow Up Monitoring"},
        {"xmlns": "http://openrosa.org/formdesigner/F562E2DC-F5DB-4AA3-BD8A-2060333C0045", "tab_name": "File a Review"},
        {"xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1", "tab_name": "Identification (Monthly)"},
        {"xmlns": "http://openrosa.org/formdesigner/A830988B-FF25-4545-B353-5B6531724A06", "tab_name": "Mother Checklist and Skill Test"},
        {"xmlns": "http://openrosa.org/formdesigner/C4572AEB-F1AB-46B6-A72E-C15DC082CDAD", "tab_name": "Mothers Feedback"},
        {"xmlns": "http://openrosa.org/formdesigner/7220BE06-2E2E-4A21-AB74-81AEEF65123C", "tab_name": "Nurses Feedback"},
        {"xmlns": "http://openrosa.org/formdesigner/B5044629-00EC-4356-B378-72B58E2E00EC", "tab_name": "Nurses Skill Test"},
    ]

    for form in forms:
        xmlns = form["xmlns"]
        tab_name = form["tab_name"]
        df = fetch_commcare_data(xmlns)
        if not df.empty:
            df = clean_dataframe(df)
            try:
                worksheet = spreadsheet.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=tab_name, rows="1000", cols="20")
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    return jsonify({"message": "Forms updated successfully!"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
