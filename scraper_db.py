"""
scraper_db.py — Independent Facebook scraper that writes directly to SQLite (api/posts.db).
Completely separate from scraper_playwright.py (Google Sheets).
Shares only: Playwright browser automation, env variables, facebook_auth.json.
"""

import time
import datetime
import hashlib
import os
import json
import re
import sqlite3
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "api", "posts.db")

load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

GROUP_URL = os.getenv("GROUP_URL", "https://www.facebook.com/groups/covsousse?sorting_setting=CHRONOLOGICAL")
STORAGE_STATE = os.getenv("STORAGE_STATE", os.path.join(SCRIPT_DIR, "facebook_auth.json"))
TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "1"))
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "50"))
AGE_LIMIT_MINUTES = int(os.getenv("AGE_LIMIT_MINUTES", "59"))

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            post_url                  TEXT UNIQUE NOT NULL,
            post_time                 TEXT,
            post_date                 TEXT,
            calendar_week             TEXT,
            weekday                   TEXT,
            profile_name              TEXT,
            gender                    TEXT,
            offer_or_demand           TEXT,
            from_city                 TEXT,
            from_area                 TEXT,
            to_city                   TEXT,
            to_area                   TEXT,
            preferred_departure_time  TEXT,
            price                     TEXT,
            nr_passengers             TEXT,
            post_text                 TEXT,
            post_text_english         TEXT,
            post_text_french          TEXT,
            scrape_timestamp          TEXT,
            synced_at                 TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def get_existing_keys(conn):
    """Build a set of known post_urls and text-hashes for deduplication."""
    keys = set()
    rows = conn.execute("SELECT post_url, profile_name, post_text, post_date FROM posts").fetchall()
    for url, name, text, date in rows:
        if url:
            keys.add(url)
            # Extract post ID from URL
            m = re.search(r'/(?:posts|permalink|p)/(\d+)', url)
            if m:
                keys.add(m.group(1))
        if text and text != "[Media post - no text]" and name:
            h = hashlib.md5(re.sub(r'\s+', '', text.lower()).encode()).hexdigest()
            keys.add(f"{name}_{h}_{date or ''}")
    return keys


def upsert_post(conn, row: dict):
    conn.execute("""
        INSERT INTO posts (
            post_url, post_time, post_date, calendar_week, weekday,
            profile_name, gender, offer_or_demand, from_city, from_area,
            to_city, to_area, preferred_departure_time, price, nr_passengers,
            post_text, post_text_english, post_text_french, scrape_timestamp
        ) VALUES (
            :post_url, :post_time, :post_date, :calendar_week, :weekday,
            :profile_name, :gender, :offer_or_demand, :from_city, :from_area,
            :to_city, :to_area, :preferred_departure_time, :price, :nr_passengers,
            :post_text, :post_text_english, :post_text_french, :scrape_timestamp
        )
        ON CONFLICT(post_url) DO UPDATE SET
            post_time   = excluded.post_time,
            post_date   = excluded.post_date,
            profile_name = excluded.profile_name,
            post_text   = excluded.post_text,
            synced_at   = datetime('now')
    """, row)


# ---------------------------------------------------------------------------
# Facebook scraping helpers (independent copy of extraction logic)
# ---------------------------------------------------------------------------

def get_deep(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        elif isinstance(d, list) and isinstance(k, int):
            d = d[k] if k < len(d) else default
        else:
            return default
    return d


def extract_data_blocks(raw_text: str) -> list:
    blocks = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        for start in range(len(line)):
            if line[start] == '{':
                depth = 0
                for end in range(start, len(line)):
                    if line[end] == '{':
                        depth += 1
                    elif line[end] == '}':
                        depth -= 1
                    if depth == 0:
                        try:
                            obj = json.loads(line[start:end + 1])
                            if isinstance(obj, dict):
                                blocks.append(obj)
                        except Exception:
                            pass
                        break
    return blocks


def parse_facebook_date(date_str, ref_time=None):
    if not date_str:
        return None, None
    if ref_time is None:
        ref_time = datetime.datetime.now()
    ref_time = ref_time - datetime.timedelta(hours=TIMEZONE_OFFSET)

    date_str = date_str.strip()
    time_match = re.search(r'(\d{1,2}):(\d{2})', date_str)
    time_str = f"{int(time_match.group(1)):02d}:{time_match.group(2)}" if time_match else None

    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}', date_str):
        parts = re.findall(r'\d+', date_str)
        try:
            d = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
            return d.strftime("%Y-%m-%d"), time_str
        except Exception:
            pass

    low = date_str.lower()
    if any(w in low for w in ['just now', "à l'instant", 'maintenant']):
        return ref_time.strftime("%Y-%m-%d"), ref_time.strftime("%H:%M")
    if 'min' in low:
        nums = re.findall(r'\d+', date_str)
        if nums:
            t = ref_time - datetime.timedelta(minutes=int(nums[0]))
            return t.strftime("%Y-%m-%d"), t.strftime("%H:%M")
    if 'hr' in low or 'heure' in low or 'h ' in low:
        nums = re.findall(r'\d+', date_str)
        if nums:
            t = ref_time - datetime.timedelta(hours=int(nums[0]))
            return t.strftime("%Y-%m-%d"), t.strftime("%H:%M")
    if 'hier' in low or 'yesterday' in low:
        d = (ref_time - datetime.timedelta(days=1)).date()
        return d.strftime("%Y-%m-%d"), time_str
    if 'lundi' in low or 'monday' in low:
        return _day_of_week(ref_time, 0), time_str
    if 'mardi' in low or 'tuesday' in low:
        return _day_of_week(ref_time, 1), time_str
    if 'mercredi' in low or 'wednesday' in low:
        return _day_of_week(ref_time, 2), time_str
    if 'jeudi' in low or 'thursday' in low:
        return _day_of_week(ref_time, 3), time_str
    if 'vendredi' in low or 'friday' in low:
        return _day_of_week(ref_time, 4), time_str
    if 'samedi' in low or 'saturday' in low:
        return _day_of_week(ref_time, 5), time_str
    if 'dimanche' in low or 'sunday' in low:
        return _day_of_week(ref_time, 6), time_str
    return None, time_str


def _day_of_week(ref, target_weekday):
    diff = (ref.weekday() - target_weekday) % 7
    if diff == 0:
        diff = 7
    return (ref - datetime.timedelta(days=diff)).strftime("%Y-%m-%d")


def find_stories(obj):
    stories = []
    if isinstance(obj, dict):
        typename = obj.get('__typename', '')
        if typename in ('Story', 'FeedUnit'):
            stories.append(obj)
        for v in obj.values():
            stories.extend(find_stories(v))
    elif isinstance(obj, list):
        for item in obj:
            stories.extend(find_stories(item))
    return stories


def find_key_recursive(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = find_key_recursive(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_key_recursive(item, key)
            if result is not None:
                return result
    return None


def find_actual_message(story):
    for path in [
        ['comet_sections', 'content', 'story', 'message', 'text'],
        ['message', 'text'],
        ['body', 'text'],
    ]:
        val = get_deep(story, path)
        if val:
            return val
    return find_key_recursive(story, 'message') if not isinstance(
        find_key_recursive(story, 'message'), dict) else get_deep(
        find_key_recursive(story, 'message'), ['text'])


def find_actual_user(story):
    for path in [
        ['comet_sections', 'actor_photo', 'story', 'actors', 0, 'name'],
        ['actors', 0, 'name'],
        ['actor', 'name'],
    ]:
        val = get_deep(story, path)
        if val:
            return val
    return find_key_recursive(story, 'name')


def find_post_url(story):
    for path in [
        ['comet_sections', 'context', 'story', 'url'],
        ['url'],
    ]:
        val = get_deep(story, path)
        if val and 'facebook.com' in str(val):
            return val
    url_node = find_key_recursive(story, 'url')
    return url_node if url_node and 'facebook.com' in str(url_node) else None


def find_numeric_time(story, key='creation_time'):
    val = find_key_recursive(story, key)
    if isinstance(val, (int, float)) and val > 1_000_000_000:
        return val
    return None


def parse_stories_to_rows(stories, ref_time):
    """Convert a list of story dicts into structured row dicts for SQLite."""
    rows = []
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for story in stories:
        url = find_post_url(story)
        if not url:
            continue

        user = find_actual_user(story) or ""
        message = find_actual_message(story) or "[Media post - no text]"

        # Try numeric creation_time first
        epoch = find_numeric_time(story, 'creation_time')
        if epoch:
            dt = datetime.datetime.fromtimestamp(epoch) - datetime.timedelta(hours=TIMEZONE_OFFSET)
            post_date = dt.strftime("%Y-%m-%d")
            post_time = dt.strftime("%H:%M")
        else:
            raw_date = find_key_recursive(story, 'creation_time_string') or \
                       find_key_recursive(story, 'timestamp_string') or ""
            post_date, post_time = parse_facebook_date(str(raw_date), ref_time)

        if not post_date:
            post_date = ref_time.strftime("%Y-%m-%d")

        # Age filter
        try:
            post_dt = datetime.datetime.strptime(f"{post_date} {post_time or '00:00'}", "%Y-%m-%d %H:%M")
            age_mins = (ref_time - post_dt).total_seconds() / 60
            if age_mins > AGE_LIMIT_MINUTES:
                continue
        except Exception:
            pass

        cw = datetime.datetime.strptime(post_date, "%Y-%m-%d").isocalendar()[1] if post_date else ""
        wd = datetime.datetime.strptime(post_date, "%Y-%m-%d").strftime("%A") if post_date else ""

        rows.append({
            "post_url": url,
            "post_time": post_time,
            "post_date": post_date,
            "calendar_week": str(cw),
            "weekday": wd,
            "profile_name": user,
            "gender": None,
            "offer_or_demand": None,
            "from_city": None,
            "from_area": None,
            "to_city": None,
            "to_area": None,
            "preferred_departure_time": None,
            "price": None,
            "nr_passengers": None,
            "post_text": message,
            "post_text_english": None,
            "post_text_french": None,
            "scrape_timestamp": now_str,
        })
    return rows


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def extract_post_id(url_str):
    m = re.search(r'/(?:posts|permalink|p)/(\d+)', url_str)
    if m:
        return m.group(1)
    fb = re.search(r'(\d+)/?$', url_str)
    return fb.group(1) if fb else url_str


def main():
    conn = init_db()
    existing_keys = get_existing_keys(conn)

    ref_time = datetime.datetime.now()
    captured_rows = []
    intercepted_count = 0

    def handle_response(response):
        nonlocal intercepted_count
        try:
            if 'graphql' not in response.url:
                return
            text = response.text()
            for block in extract_data_blocks(text):
                stories = find_stories(block)
                if stories:
                    intercepted_count += len(stories)
                    rows = parse_stories_to_rows(stories, ref_time)
                    captured_rows.extend(rows)
        except Exception:
            pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(storage_state=STORAGE_STATE)
        page = ctx.new_page()
        page.on("response", handle_response)

        print(f"[DB Scraper] Navigating to {GROUP_URL}")
        page.goto(GROUP_URL, timeout=60_000)
        time.sleep(5)

        # Bypass "Continue as" modal
        for sel in ['[role="button"][tabindex="0"]', 'button:has-text("Continue")']:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(2)
                    break
            except Exception:
                pass

        for i in range(MAX_SCROLLS):
            page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
            time.sleep(2)
            print(f"  Scroll {i+1}/{MAX_SCROLLS} | captured {len(captured_rows)}", end="\r")

        browser.close()

    print(f"\n[DB Scraper] Intercepted {intercepted_count} stories, parsed {len(captured_rows)} rows.")

    # Deduplicate and save
    saved = 0
    for row in captured_rows:
        url = row["post_url"]
        pid = extract_post_id(url)
        if url in existing_keys or pid in existing_keys:
            continue
        text = row.get("post_text", "")
        if text and text != "[Media post - no text]":
            h = hashlib.md5(re.sub(r'\s+', '', text.lower()).encode()).hexdigest()
            tk = f"{row['profile_name']}_{h}_{row['post_date'] or ''}"
            if tk in existing_keys:
                continue
            existing_keys.add(tk)

        try:
            upsert_post(conn, row)
            saved += 1
        except Exception as e:
            print(f"  [DB] skip: {e}")

        existing_keys.add(url)
        existing_keys.add(pid)

    conn.commit()
    conn.close()
    print(f"✅ Saved {saved} new posts to SQLite.")


if __name__ == "__main__":
    main()
