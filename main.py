from flask import Flask, request, jsonify
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Initialize Flask app
app = Flask(__name__)

# Environment variables for CommCare API details
base_url = os.getenv("COMMCARE_BASE_URL", "https://india.commcarehq.org/a/kangaroo-mother-care-ansh/api/v0.5/form/")
api_key = os.getenv("COMMCARE_API_KEY", "your-default-api-key")  # Replace with default for testing
username = os.getenv("COMMCARE_USERNAME", "your-default-username")  # Replace with default for testing
headers = {"Authorization": f"ApiKey {username}:{api_key}"}

# Google Sheets setup
creds_content = os.getenv("GOOGLE_SHEETS_CRED")

if not creds_content:
    raise ValueError("Environment variable GOOGLE_SHEETS_CRED is not set or empty.")

# Write the credentials to a temporary file
credentials_path = "google_sheet_cred.json"
with open(credentials_path, "w") as f:
    f.write(creds_content)

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
finally:
    # Clean up the temporary credential file
    if os.path.exists(credentials_path):
        os.remove(credentials_path)

# Target Google Sheet
sheet_name = os.getenv("SHEET_NAME", "Facility Observations - CommCare Realtime")
try:
    spreadsheet = client.open(sheet_name)
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Spreadsheet '{sheet_name}' not found. Creating a new one...")
    spreadsheet = client.create(sheet_name)
    print(f"Spreadsheet '{sheet_name}' created. Share it with the service account for access.")

# Default route for testing and health check
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "CommCare Integration API is running!"})

# Function to fetch data from CommCare
def fetch_commcare_data(xmlns):
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

            if not data.get("meta", {}).get("next"):
                break
            offset += limit
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for xmlns {xmlns}: {e}")
            break

    return pd.json_normalize(all_data) if all_data else pd.DataFrame()

# Clean DataFrame to handle NaN, inf, -inf
def clean_dataframe(df):
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.fillna("")
    return df

@app.route('/update_sheets', methods=['POST'])
def update_sheets():
    test_url = "https://india.commcarehq.org/a/kangaroo-mother-care-ansh/api/v0.5/form/"
    try:
        test_response = requests.get(test_url, headers=headers)
        print("Test API Response:", test_response.status_code)
        print("Test API Body:", test_response.text)
    except Exception as e:
        print("Error testing API access:", str(e))
    """Process all forms and update Google Sheets."""
    print("POST request received at /update_sheets")  # Debug log
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
                worksheet.clear()  # Ensure old data is removed
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
