import gspread
from oauth2client.service_account import ServiceAccountCredentials
from facebook_scraper import get_posts
import pandas as pd
import time

# --- CONFIGURATION ---
GROUP_ID = "525468629029673"
SHEET_NAME = "covoiturage report" # <--- MAKE SURE THIS MATCHES YOUR GOOGLE SHEET NAME EXACTLY
CREDENTIALS_FILE = "credentials.json"
COOKIES_FILE = "facebook_cookies.txt"

def main():
    print("--- STARTING SCRAPER ---")

    # 1. CONNECT TO GOOGLE SHEETS
    print("1. Connecting to Google Sheets...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).worksheet("Feuille 1")
        print(f"✅ Successfully connected to Google Sheets: {SHEET_NAME}")
    except Exception as e:
        print(f"❌ Error connecting to Sheets: {e}")
        print("💡 Tip: Make sure the Google Sheet name matches exactly and the service account has access.")
        return

    # 2. SCRAPE FACEBOOK
    print("2. Scraping Facebook Group (Scanning last 3 pages)...")
    data_list = []
    
    try:
        # options={"comments": False} speeds it up by ignoring comments
        for post in get_posts(group=GROUP_ID, pages=3, cookies=COOKIES_FILE, options={"comments": False}):
            
            # Extract fields
            profile_name = post.get('username', 'Unknown')
            post_text = post.get('text', '')
            post_url = post.get('post_url', '')

            # Only add valid posts with a URL
            if post_url:
                # Prepare the row
                row = [profile_name, post_text, post_url]
                data_list.append(row)
                print(f"   Found post by: {profile_name}")
                
    except Exception as e:
        print(f"❌ Error scraping Facebook: {e}")
        print("💡 Tip: Check if 'facebook_cookies.txt' is in the folder and not expired.")
        return

    # 3. UPLOAD TO SHEETS
    print(f"3. Found {len(data_list)} posts. Uploading...")

    if data_list:
        try:
            # Check if sheet is empty (to add headers)
            if not sheet.get_all_values():
                sheet.append_row(["Profile Name", "Post Text", "Post URL"])
            
            # Append data
            sheet.append_rows(data_list)
            print("✅ Success! Data uploaded to Google Sheet.")
        except Exception as e:
            print(f"❌ Error uploading to Google: {e}")
    else:
        print("⚠️ No posts found. (This usually means the cookies are invalid or the group blocked the bot temporarily)")

if __name__ == "__main__":
    main()
