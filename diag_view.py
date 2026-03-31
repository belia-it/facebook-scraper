import os
import time
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

GROUP_URL = os.getenv("GROUP_URL")
if "?" not in GROUP_URL:
    GROUP_URL += "?sorting_setting=CHRONOLOGICAL"
STORAGE_STATE = os.getenv("STORAGE_STATE", "facebook_auth.json")

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=STORAGE_STATE if os.path.exists(STORAGE_STATE) else None,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        print(f"Checking: {GROUP_URL}")
        page.goto(GROUP_URL)
        time.sleep(10)
        
        # Check login status
        is_logged_in = page.query_selector('div[aria-label="Account"]') or page.query_selector('div[aria-label="Profil"]')
        print(f"Is Logged In: {bool(is_logged_in)}")
        
        # Save screenshot
        page.screenshot(path="debug_view.png")
        print("Screenshot saved to debug_view.png")
        
        # Print some text to see what it sees
        text = page.inner_text("body")
        print(f"Page text snippet: {text[:500]}...")
        
        browser.close()

if __name__ == "__main__":
    run()
