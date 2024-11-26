from flask import Flask, request, jsonify
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from threading import Thread

# Initialize Flask app
app = Flask(__name__)

# CommCare API details
base_url = "https://india.commcarehq.org/a/kangaroo-mother-care-ansh/api/v0.5/form/"
api_key = "44332966626889987e4b1c421ac754dbbb97a626"
username = "kshitizz.sisodia@gmail.com"
headers = {"Authorization": f"ApiKey {username}:{api_key}"}

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
import os
credentials_path = "google_sheet_cred.json"  # This will be replaced
creds_content = os.environ["GOOGLE_SHEETS_CRED"]
with open("google_sheet_cred.json", "w") as f:
    f.write(creds_content)
credentials_path = "google_sheet_cred.json"


# Target Google Sheet
sheet_name = "Facility Observations - CommCare Realtime"  # Replace with your Google Sheet name
try:
    spreadsheet = client.open(sheet_name)
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Spreadsheet '{sheet_name}' not found. Creating a new one...")
    spreadsheet = client.create(sheet_name)
    print(f"Spreadsheet '{sheet_name}' created. Share it with the service account for access.")

# Default route for testing
@app.route("/")
def home():
    return "CommCare Integration API is running!"

# Function to fetch data from CommCare
def fetch_commcare_data(xmlns):
    """Fetch data from the CommCare API for a specific xmlns."""
    limit = 1000
    offset = 0
    all_data = []

    while True:
        api_url = f"{base_url}?xmlns={xmlns}&limit={limit}&offset={offset}"
        print(f"Fetching records for xmlns {xmlns}, starting at offset {offset}...")

        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            records = data.get("objects", [])
            all_data.extend(records)

            # Break if there's no next page
            if not data.get("meta", {}).get("next"):
                break
            offset += limit
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    return pd.json_normalize(all_data) if all_data else pd.DataFrame()

# Clean DataFrame to remove NaN, inf, -inf
def clean_dataframe(df):
    """Clean a DataFrame by replacing NaN and infinite values."""
    df = df.replace([float("inf"), float("-inf")], None)  # Replace inf and -inf with None
    df = df.fillna("")  # Replace NaN with an empty string
    return df

# Flask route to process forms
@app.route('/update_sheets', methods=['POST'])
def update_sheets():
    """Process all forms and update Google Sheets."""
    forms_to_fetch = [
        {
            "xmlns": "http://openrosa.org/formdesigner/65304B1B-FF8C-4683-8026-B935FD0DC674",
            "tab_name": "Cleaning Checklist",
        },
        {
            "xmlns": "http://openrosa.org/formdesigner/9EB06393-8DBE-4180-B537-A564850798B4",
            "tab_name": "KC Inventory Checklist",
        },
        {
            "xmlns": "http://openrosa.org/formdesigner/928FA710-2CC8-4348-8382-82E6A96EF714",
            "tab_name": "KMC Furnishing Checklist",
        },
        {
            "xmlns": "http://openrosa.org/formdesigner/855C2642-C2C8-4D86-8374-CEBBD5E8AC77",
            "tab_name": "Cleaning Checklist KMC Program",
        },
        {
            "xmlns": "http://openrosa.org/formdesigner/99D79080-56CA-43F8-85D5-FFF0DCD9C5E1",
            "tab_name": "Fire a Hospital Staff",
        },
        {
            "xmlns": "http://openrosa.org/formdesigner/81FC2C13-CD6F-4F2A-BCBF-98C8466F0A3C",
            "tab_name": "File a damage/replacement",
        },
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
