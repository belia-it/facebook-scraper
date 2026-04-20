"""Launched by the webapp to open a visible browser for a one-click Facebook login.
When the user finishes logging in, cookies are saved to facebook_auth.json."""
import os, sys, time
from playwright.sync_api import sync_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTH = os.path.join(SCRIPT_DIR, os.getenv("STORAGE_STATE", "facebook_auth.json"))
GROUP_URL = os.getenv("GROUP_URL", "https://www.facebook.com/groups/covsousse")

def has_valid_session(context):
    """Check if session has c_user and xs (logged-in state)."""
    try:
        state = context.storage_state()
        names = {c.get("name") for c in state.get("cookies", [])}
        return "c_user" in names and "xs" in names
    except Exception:
        return False

def main():
    with sync_playwright() as p:
        # Headful so the user sees the window
        browser = p.chromium.launch(headless=False, args=["--no-sandbox"])
        ctx_kwargs = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "viewport": {"width": 1280, "height": 900},
        }
        if os.path.exists(AUTH):
            ctx_kwargs["storage_state"] = AUTH
        ctx = browser.new_context(**ctx_kwargs)
        page = ctx.new_page()
        print("Opening Facebook login window...", flush=True)
        page.goto("https://www.facebook.com/", wait_until="commit", timeout=60000)

        print("Waiting for you to log in (up to 10 minutes)...", flush=True)
        deadline = time.time() + 600  # 10 min
        last_url = ""
        while time.time() < deadline:
            try:
                url = page.url
                if url != last_url:
                    last_url = url
                # Detect logged-in state: has c_user + xs and we are not on /login or /checkpoint
                if has_valid_session(ctx) and "login" not in url and "checkpoint" not in url:
                    # Give 2s for any remaining redirect
                    time.sleep(2)
                    if has_valid_session(ctx):
                        break
            except Exception:
                # Page probably closed by user
                break
            time.sleep(1.5)

        if has_valid_session(ctx):
            try:
                ctx.storage_state(path=AUTH)
                state = ctx.storage_state()
                print(f"LOGIN_SUCCESS saved {len(state.get('cookies', []))} cookies", flush=True)
            except Exception as e:
                print(f"LOGIN_SAVE_ERROR {e}", flush=True)
                sys.exit(1)
        else:
            print("LOGIN_TIMEOUT or cancelled (no valid session detected)", flush=True)
            sys.exit(1)

        try: browser.close()
        except: pass

if __name__ == "__main__":
    main()
