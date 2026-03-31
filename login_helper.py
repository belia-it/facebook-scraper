import os
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

STORAGE_STATE = os.getenv("STORAGE_STATE", "facebook_auth.json")

def run():
    with sync_playwright() as p:
        # Launch headful browser so user can log in
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            storage_state=STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        )
        page = context.new_page()
        
        print("Opening Facebook... Please log in if needed.")
        page.goto("https://www.facebook.com")
        
        print("Wait! Don't close the browser until you are logged in.")
        print("Once you are logged in and see your feed, come back here and press Enter.")
        input("Press Enter to save the session and close...")
        
        context.storage_state(path=STORAGE_STATE)
        print(f"Session saved to {STORAGE_STATE}")
        browser.close()

if __name__ == "__main__":
    run()
