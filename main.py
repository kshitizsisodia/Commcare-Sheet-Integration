from flask import Flask, request, jsonify, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from redis import Redis
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Initialize Redis for Flask-Limiter
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")  # Default to localhost if REDIS_URL is not set
redis_client = Redis.from_url(redis_url)

# Initialize rate limiter
limiter = Limiter(get_remote_address, app=app, storage_uri=redis_url)

# CommCare API details (fetched from environment variables)
base_url = "https://india.commcarehq.org/a/kangaroo-mother-care-ansh/api/v0.5/form/"
api_key = os.getenv("COMMCARE_API_KEY")
username = os.getenv("COMMCARE_USERNAME")

if not api_key or not username:
    raise ValueError("COMMCARE_API_KEY or COMMCARE_USERNAME environment variable is missing")

headers = {"Authorization": f"ApiKey {username}:{api_key}"}

# Google Sheets credentials from environment variable
creds_content = os.getenv("GOOGLE_SHEETS_CRED")
if not creds_content:
    raise ValueError("Environment variable GOOGLE_SHEETS_CRED is not set or empty")

# Write the decoded credentials to a temporary file
credentials_path = "google_sheet_cred.json"
with open(credentials_path, "w") as f:
    f.write(creds_content)

# Authorize Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(creds)

# Target Google Sheet
sheet_name = "Facility Observations - CommCare Realtime"
try:
    spreadsheet = client.open(sheet_name)
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Spreadsheet '{sheet_name}' not found. Creating a new one...")
    spreadsheet = client.create(sheet_name)
    print(f"Spreadsheet '{sheet_name}' created. Share it with the service account for access.")

# API Token for security
API_TOKEN = "securedata@ansh123"

# Default route for testing
@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        return jsonify({"status": "POST request received at /"})
    return jsonify({"status": "CommCare Integration API is running!"})

# Function to fetch data from CommCare
def fetch_commcare_data(xmlns):
    """Fetch data from the CommCare API for a specific xmlns."""
    limit = 1000
    offset = 0
    all_data = []

    while True:
        api_url = f"{base_url}?xmlns={xmlns}&limit={limit}&offset={offset}"
        print(f"Fetching records for xmlns {xmlns}, starting at offset {offset}...")

        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            records = data.get("objects", [])
            all_data.extend(records)

            # Break if there's no next page
            if not data.get("meta", {}).get("next"):
                break
            offset += limit
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for xmlns {xmlns}: {e}")
            break

    return pd.json_normalize(all_data) if all_data else pd.DataFrame()

# Clean DataFrame to remove NaN, inf, -inf
def clean_dataframe(df):
    """Clean a DataFrame by replacing NaN and infinite values."""
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.fillna("")  # Replace NaN with an empty string
    return df

# Flask route to process forms
@app.route('/update_sheets', methods=['POST'])
@limiter.limit("10/minute")  # Rate limit: 10 requests per minute
def update_sheets():
    """Process all forms and update Google Sheets."""
    # Check API token
    token = request.headers.get("Authorization")
    if not token or token != f"Bearer {API_TOKEN}":
        abort(403)  # Forbidden

    forms_to_fetch = [
        {"xmlns": "http://openrosa.org/formdesigner/65304B1B-FF8C-4683-8026-B935FD0DC674", "tab_name": "Cleaning Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/9EB06393-8DBE-4180-B537-A564850798B4", "tab_name": "KC Inventory Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/928FA710-2CC8-4348-8382-82E6A96EF714", "tab_name": "KMC Furnishing Checklist"},
        {"xmlns": "http://openrosa.org/formdesigner/855C2642-C2C8-4D86-8374-CEBBD5E8AC77", "tab_name": "Cleaning Checklist KMC Program"},
        {"xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1", "tab_name": "Fire a Hospital Staff"},
        {"xmlns": "http://openrosa.org/formdesigner/81FC2C13-CD6F-4F2A-BCBF-98C8466F0A3C", "tab_name": "File a damage/replacement"},
    ]

    for form in forms_to_fetch:
        xmlns = form["xmlns"]
        tab_name = form["tab_name"]

        print(f"Processing form data for: {tab_name}")
        df = fetch_commcare_data(xmlns)

        if not df.empty:
            df = clean_dataframe(df)
            try:
                # Try to open the worksheet/tab; create it if it doesn't exist
                try:
                    worksheet = spreadsheet.worksheet(tab_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=tab_name, rows="1000", cols="20")

                # Clear the worksheet and update data
                worksheet.clear()
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                print(f"Updated sheet/tab: {tab_name}")
            except Exception as e:
                print(f"Error updating sheet/tab {tab_name}: {e}")
        else:
            print(f"No data found for form: {tab_name}")

    return jsonify({"message": "All forms processed successfully"}), 200

# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
