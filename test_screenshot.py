from playwright.sync_api import sync_playwright
import os
import time
from dotenv import load_dotenv

load_dotenv()

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            storage_state="facebook_auth.json" if os.path.exists("facebook_auth.json") else None,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        print(f"Opening {os.getenv('GROUP_URL')}...")
        try:
            page.goto(os.getenv("GROUP_URL"), wait_until="networkidle", timeout=60000)
        except Exception as e:
            print(f"Goto failed (continuing anyway): {e}")
            
        time.sleep(10)
        page.screenshot(path="debug_vps.png")
        print("Screenshot saved to debug_vps.png")
        browser.close()

if __name__ == "__main__":
    main()
