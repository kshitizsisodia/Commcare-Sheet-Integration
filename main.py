from flask import Flask, request, jsonify, abort
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
from collections import defaultdict
import time

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Rate limiting implementation (10 requests per minute per IP)
request_counts = defaultdict(list)

@app.before_request
def limit_requests():
    client_ip = request.remote_addr
    current_time = time.time()

    if client_ip not in request_counts:
        request_counts[client_ip] = []

    # Clean up old requests
    request_counts[client_ip] = [
        t for t in request_counts[client_ip] if current_time - t < 60
    ]

    # Check limit
    if len(request_counts[client_ip]) >= 10:
        abort(429, description="Too many requests. Please try again later.")

    request_counts[client_ip].append(current_time)

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

# Define Google Sheets and corresponding forms
sheet_1_name = "Facility Observations - CommCare Realtime"
sheet_2_name = "Healthcare Worker Observations - Commcare Realtime"
sheet_3_name = "Interviews - Commcare Realtime"

# Map sheets to forms
forms_sheet_1 = [
    {"xmlns": "http://openrosa.org/formdesigner/65304B1B-FF8C-4683-8026-B935FD0DC674", "tab_name": "Cleaning Checklist"},
    {"xmlns": "http://openrosa.org/formdesigner/9EB06393-8DBE-4180-B537-A564850798B4", "tab_name": "KC Inventory Checklist"},
    {"xmlns": "http://openrosa.org/formdesigner/928FA710-2CC8-4348-8382-82E6A96EF714", "tab_name": "KMC Furnishing Checklist"},
    {"xmlns": "http://openrosa.org/formdesigner/855C2642-C2C8-4D86-8374-CEBBD5E8AC77", "tab_name": "Cleaning Checklist KMC Program"},
    {"xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1", "tab_name": "Fire a Hospital Staff"},
    {"xmlns": "http://openrosa.org/formdesigner/81FC2C13-CD6F-4F2A-BCBF-98C8466F0A3C", "tab_name": "File a damage/replacement"},
]

forms_sheet_2 = [
    {"xmlns": "http://openrosa.org/formdesigner/8C12ABAA-C695-46FB-A21F-B67612866DAE", "tab_name": "Validation of Weighting Process"},
    {"xmlns": "http://openrosa.org/formdesigner/6B79AADB-7492-4FF9-8389-C0A4D1AA6987", "tab_name": "Case Observations"},
    {"xmlns": "http://openrosa.org/formdesigner/40AF78C9-BE3E-4669-96DC-567FFFED09C0", "tab_name": "Phone Follow Up Monitoring"},
    {"xmlns": "http://openrosa.org/formdesigner/F562E2DC-F5DB-4AA3-BD8A-2060333C0045", "tab_name": "File a Review"},
    {"xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1", "tab_name": "Identification (Monthly)"},
]

forms_sheet_3 = [
    {"xmlns": "http://openrosa.org/formdesigner/A830988B-FF25-4545-B353-5B6531724A06", "tab_name": "Mother Checklist and Skill Test"},
    {"xmlns": "http://openrosa.org/formdesigner/C4572AEB-F1AB-46B6-A72E-C15DC082CDAD", "tab_name": "Mothers Feedback"},
    {"xmlns": "http://openrosa.org/formdesigner/7220BE06-2E2E-4A21-AB74-81AEEF65123C", "tab_name": "Nurses Feedback"},
    {"xmlns": "http://openrosa.org/formdesigner/B5044629-00EC-4356-B378-72B58E2E00EC", "tab_name": "Nurses Skill Test"},
]

# Target spreadsheets
sheets = {
    sheet_1_name: client.open(sheet_1_name),
    sheet_2_name: client.open(sheet_2_name),
    sheet_3_name: client.open(sheet_3_name),
}

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

def update_sheet(sheet, forms):
    """Update the specified Google Sheet with form data."""
    for form in forms:
        xmlns = form["xmlns"]
        tab_name = form["tab_name"]

        print(f"Processing form data for: {tab_name}")
        df = fetch_commcare_data(xmlns)

        if not df.empty:
            df = clean_dataframe(df)
            try:
                # Try to open the worksheet/tab; create it if it doesn't exist
                try:
                    worksheet = sheet.worksheet(tab_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="20")

                # Clear the worksheet and update data
                worksheet.clear()
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                print(f"Updated sheet/tab: {tab_name}")
            except Exception as e:
                print(f"Error updating sheet/tab {tab_name}: {e}")
        else:
            print(f"No data found for form: {tab_name}")

@app.route('/update_sheets', methods=['POST'])
def update_sheets():
    """Route to update sheets based on query parameter."""
    # Verify API token
    token = request.headers.get("Authorization")
    if not token or token != f"Bearer {API_TOKEN}":
        abort(403)

    # Get the sheet type from the query parameter
    sheet_type = request.args.get("sheet")
    if sheet_type == "facility_observations":
        forms = forms_sheet_1
        sheet = sheets[sheet_1_name]
    elif sheet_type == "healthcare_worker_observations":
        forms = forms_sheet_2
        sheet = sheets[sheet_2_name]
    elif sheet_type == "interviews":
        forms = forms_sheet_3
        sheet = sheets[sheet_3_name]
    else:
        return jsonify({"error": "Invalid sheet type. Please specify a valid sheet type."}), 400

    # Update the specified sheet
    try:
        update_sheet(sheet, forms)
        return jsonify({"message": f"Successfully updated {sheet_type}"}), 200
    except Exception as e:
        print(f"Error updating sheet {sheet_type}: {e}")
        return jsonify({"error": f"Failed to update {sheet_type}"}), 500


# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
