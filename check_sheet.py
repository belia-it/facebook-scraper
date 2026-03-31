import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("CREDENTIALS_FILE"), scope)
    client = gspread.authorize(creds)
    sheet = client.open(os.getenv("SHEET_NAME")).get_worksheet(0)
    
    rows = sheet.get_all_values()
    print(f"Total rows: {len(rows)}")
    if rows:
        print("Last 5 rows:")
        for r in rows[-5:]:
            print(r)

if __name__ == "__main__":
    main()
