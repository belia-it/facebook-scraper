import os
from playwright.sync_api import sync_playwright

def main():
    print("🚀 Starting headless browser for remote authentication...")
    print("Waiting for you to connect via http://localhost:9222 on your local machine.")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--remote-debugging-port=9222", 
                "--remote-debugging-address=0.0.0.0",
                "--remote-allow-origins=*"
            ]
        )
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://www.facebook.com/")
        
        print("\n🔒 The browser is open. Please:")
        print("1. Forward the port using SSH on your local Mac: ssh -L 9222:127.0.0.1:9222 houcem@192.168.100.45")
        print("2. Open Google Chrome on your Mac and navigate to: http://localhost:9222")
        print("3. Click on the 'Facebook - log in or sign up' link.")
        print("4. Log into Facebook. Complete any 2FA or security checks Facebook throws at this new IP.")
        print("\n⏳ I will keep this browser open for exactly 3 MINUTES for you to log in.")
        print("Please log in now. The session will save automatically when the timer ends...")
        
        import time
        for remaining in range(180, 0, -1):
            if remaining % 30 == 0:
                print(f"   ... {remaining} seconds remaining before saving ...")
            time.sleep(1)
            
        print("\n💾 Timer finished! Saving authenticated session to facebook_auth.json...")
        context.storage_state(path="facebook_auth.json")
        print("✅ Success! Authentication saved for the server's IP. You can now close this and run the scraper.")
        browser.close()

if __name__ == "__main__":
    main()
