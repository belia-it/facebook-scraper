"""
scraper_db.py — Robust Facebook scraper writing directly to SQLite (api/posts.db).
Logic is a faithful port of scraper_playwright.py (which works correctly).
Key fixes:
  - find_actual_user(): tries actors[], actor{}, author{} in order – never grabs generic "name" fields
  - find_actual_message(): uses message->text priority, filters out FB UI labels
  - find_stories(): heuristic matching (typename OR time+actors) to catch more story formats
  - DOM scrape: uses correct selectors [role="article"] exactly like scraper_playwright.py
  - Date: parse_facebook_date() fully ported — handles Unix timestamps, relative strings, etc.
  - Deduplication: checks both post_url and extracted post_id
"""

import time
import datetime
import os
import json
import re
import hashlib
import sqlite3
import requests
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

# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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


def get_existing_ids(conn):
    """Return a set of all known post_urls, numeric post IDs, and text-based dedup keys."""
    keys = set()
    for row in conn.execute("SELECT post_url, profile_name, post_text, post_date FROM posts").fetchall():
        url, profile, text, date_val = row
        keys.add(url)
        m = re.search(r'/(?:posts|permalink|p)/(\d+)', url or '')
        if m:
            keys.add(m.group(1))
        # Text-based dedup key
        if text and text != "[Media post - no text]":
            norm = re.sub(r'\s+', '', (text or '').lower())
            keys.add(f"{profile}_{hashlib.md5(norm.encode()).hexdigest()}_{date_val}")
    return keys


def upsert_post(conn, data: dict):
    conn.execute("""
        INSERT INTO posts (
            post_url, post_time, post_date, calendar_week, weekday,
            profile_name, post_text, scrape_timestamp
        ) VALUES (
            :post_url, :post_time, :post_date, :calendar_week, :weekday,
            :profile_name, :post_text, :scrape_timestamp
        )
        ON CONFLICT(post_url) DO UPDATE SET
            profile_name     = excluded.profile_name,
            post_text        = excluded.post_text,
            post_time        = excluded.post_time,
            post_date        = excluded.post_date,
            synced_at        = datetime('now')
    """, data)


# ─────────────────────────────────────────────────────────────────────────────
# Extraction helpers (ported from scraper_playwright.py)
# ─────────────────────────────────────────────────────────────────────────────

def extract_data_blocks(raw_text):
    """Extract all JSON data blocks starting with {"data": ...} from raw FB text."""
    # FB sometimes prefixes with "for (;;);"
    raw_text = raw_text.replace("for (;;);", "").strip()
    blocks = []
    i = 0
    n = len(raw_text)
    while True:
        idx = raw_text.find('"data"', i)
        if idx == -1:
            break
        brace_start = raw_text.find('{', idx)
        if brace_start == -1:
            break
        depth, end_idx = 0, -1
        for j in range(brace_start, n):
            if raw_text[j] == '{':
                depth += 1
            elif raw_text[j] == '}':
                depth -= 1
                if depth == 0:
                    end_idx = j
                    break
        if end_idx != -1:
            try:
                block = json.loads(raw_text[brace_start:end_idx + 1])
                if isinstance(block, dict):
                    blocks.append(block)
            except json.JSONDecodeError:
                pass
            i = end_idx + 1
        else:
            break
    return blocks


STORY_TYPENAMES = {
    "Story", "FeedUnit", "GroupFeedStory", "GroupPost",
    "UserPost", "FeedStory", "GroupCommerceProductItem"
}
TIME_FIELDS = {
    "creation_time", "timestamp", "publish_time", "created_time",
    "publish_timestamp", "created_timestamp"
}


def find_stories(obj):
    """
    Recursively find story-like objects.
    Heuristic: matches typename OR (has timestamp AND has actor/actors/author).
    This mirrors the logic in scraper_playwright.py exactly.
    """
    found = []
    if isinstance(obj, dict):
        typename = obj.get("__typename")
        has_time = any(k in obj for k in TIME_FIELDS)
        has_actors = (
            ("actors" in obj and isinstance(obj["actors"], list)) or
            ("actor" in obj and isinstance(obj["actor"], dict)) or
            ("author" in obj and isinstance(obj["author"], dict))
        )
        has_post_id = "post_id" in obj or ("id" in obj and isinstance(obj.get("id"), str))

        is_story = typename in STORY_TYPENAMES
        if not is_story and has_time and has_actors:
            is_story = True
        if not is_story and has_time and has_post_id:
            is_story = True

        if is_story:
            found.append(obj)

        for v in obj.values():
            found.extend(find_stories(v))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(find_stories(v))
    return found


def find_key_recursive(obj, key):
    """Find first occurrence of key anywhere in nested dict/list."""
    if isinstance(obj, dict):
        if key in obj and obj[key]:
            return obj[key]
        for v in obj.values():
            res = find_key_recursive(v, key)
            if res:
                return res
    elif isinstance(obj, list):
        for v in obj:
            res = find_key_recursive(v, key)
            if res:
                return res
    return None


# FB UI labels we should NEVER return as post content
_FB_INTERNAL_LABELS = {
    "S", "e", "·", "J'aime", "Commenter", "Partager",
    "Like", "Comment", "Share", "Ok", "J\u2019aime"
}


def find_actual_message(s):
    """
    Extract the real post text.
    Priority 1: message -> text  (most reliable)
    Priority 2: recursive search for 'text' key, skipping FB internal labels
    """
    m = s.get("message")
    if isinstance(m, dict) and "text" in m:
        t = m["text"].strip()
        if t and t not in _FB_INTERNAL_LABELS:
            return t

    def check_text(obj):
        if isinstance(obj, dict):
            if "text" in obj and isinstance(obj["text"], str):
                t = obj["text"].strip()
                if len(t) >= 10 and t not in _FB_INTERNAL_LABELS:
                    return t
            for v in obj.values():
                res = check_text(v)
                if res:
                    return res
        elif isinstance(obj, list):
            for v in obj:
                res = check_text(v)
                if res:
                    return res
        return None

    return check_text(s)


def find_actual_user(s):
    """
    Extract the poster's name.
    Checks actors[], actor{}, author{} — never falls back to generic name keys
    which could be city names, labels, etc.
    """
    # Try actors list
    actors = find_key_recursive(s, "actors")
    if actors and isinstance(actors, list) and len(actors) > 0:
        first = actors[0]
        if isinstance(first, dict):
            name = first.get("name")
            if name and name not in ("Unknown User", ""):
                return name

    # Try singular actor
    actor = find_key_recursive(s, "actor")
    if actor and isinstance(actor, dict):
        name = actor.get("name")
        if name and name not in ("Unknown User", ""):
            return name

    # Try author
    author = find_key_recursive(s, "author")
    if author and isinstance(author, dict):
        name = author.get("name")
        if name and name not in ("Unknown User", ""):
            return name

    return "Unknown User"


def find_numeric_time(obj, key):
    """Find first numeric value for a given key anywhere in nested structure."""
    if isinstance(obj, dict):
        if key in obj:
            v = obj[key]
            if isinstance(v, (int, float)):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        for child in obj.values():
            res = find_numeric_time(child, key)
            if res is not None:
                return res
    elif isinstance(obj, list):
        for child in obj:
            res = find_numeric_time(child, key)
            if res is not None:
                return res
    return None


def parse_facebook_date(date_str, ref_time=None):
    """
    Full port from scraper_playwright.py.
    Handles Unix timestamps, relative strings ("8 min", "2 heures") and exact dates.
    Returns (date_str, time_str, raw_str).
    """
    if not ref_time:
        ref_time = datetime.datetime.now() + datetime.timedelta(hours=0)  # local time

    if not date_str:
        return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M'), ""

    try:
        if isinstance(date_str, (int, float)) or (
                isinstance(date_str, str) and str(date_str).isdigit()):
            ts = int(date_str)
            dt_utc = datetime.datetime.utcfromtimestamp(ts)
            target = dt_utc + datetime.timedelta(hours=TIMEZONE_OFFSET)
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M'), f"API_{ts}"
    except Exception:
        pass

    ds = str(date_str).lower().strip()
    months = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
        'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12,
        'janv': 1, 'févr': 2, 'sept': 9, 'oct': 10, 'nov': 11, 'déc': 12,
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'dec': 12,
    }
    all_month_names = '|'.join(months.keys())

    try:
        if ds in ('just now', 'now') or "l'instant" in ds:
            return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M'), date_str

        exact_fr = re.search(
            r'(\d{1,2})\s+(' + all_month_names + r')\s+(\d{4})\s+[àa]\s+(\d{1,2}):(\d{2})', ds)
        if exact_fr:
            day, m_name, year, h, m = exact_fr.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M'), date_str

        exact_en = re.search(
            r'(' + all_month_names + r')\s+(\d{1,2}),?\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})', ds)
        if exact_en:
            m_name, day, year, h, m = exact_en.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M'), date_str

        rel = re.search(
            r'(\d+)\s*(minutes?|mins?|m|hours?|heures?|h|jours?|j|days?|d|seconds?|s)\b', ds)
        if rel:
            val, unit = rel.groups()
            val = int(val)
            if unit in ('min', 'mins', 'minute', 'minutes', 'm'):
                target = ref_time - datetime.timedelta(minutes=val)
            elif unit in ('h', 'hour', 'hours', 'heure', 'heures'):
                target = ref_time - datetime.timedelta(hours=val)
            elif unit in ('j', 'jour', 'jours', 'd', 'day', 'days'):
                target = ref_time - datetime.timedelta(days=val)
            elif unit in ('s', 'second', 'seconds'):
                target = ref_time - datetime.timedelta(seconds=val)
            else:
                target = ref_time
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M'), date_str

        if 'hier' in ds or 'yesterday' in ds:
            time_match = re.search(r'(\d{1,2})[:h](\d{2})', ds)
            if time_match:
                h, m = time_match.groups()
                target = (ref_time - datetime.timedelta(days=1)).replace(
                    hour=int(h), minute=int(m), second=0)
            else:
                target = ref_time - datetime.timedelta(days=1)
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M'), date_str

    except Exception as e:
        print(f"   [Warning] Date parsing failed for '{date_str}': {e}")

    return None, None, date_str


def extract_post_id(url):
    m = re.search(r'/(?:posts|permalink|p)/(\d+)', str(url))
    return m.group(1) if m else None


NOTIFIED_POSTS = set()


def notify_api(post_id, data, ref_time):
    """Push new post to the FastAPI WebSocket broadcast endpoint."""
    if not post_id or post_id in NOTIFIED_POSTS:
        return
    text = (data.get('post_text') or '').lower()
    type_tag = "OFFER" if any(
        x in text for x in ["offre", "chauffeur", "dispo", "disponible", "partage", "offer"]
    ) else "DEMAND" if any(
        x in text for x in ["chercher", "demande", "besoin", "demand"]
    ) else "UNKNOWN"

    payload = {
        "profile_name": data.get('profile_name', 'Unknown User'),
        "post_date": data.get('post_date'),
        "post_time": data.get('post_time'),
        "post_text": data.get('post_text', ''),
        "offer_or_demand": type_tag,
        "post_url": data.get('post_url', '')
    }
    try:
        requests.post("http://localhost:8000/api/internal/post-update",
                      json=payload, timeout=2)
        NOTIFIED_POSTS.add(post_id)
        print(f"   [Live] Notified API: {post_id}")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=== scraper_db.py starting ===")
    conn = init_db()
    existing = get_existing_ids(conn)
    ref_time = datetime.datetime.now()
    captured = {}   # pid -> row dict
    browser_alive = True   # flag to prevent response handler from running after close

    # ── GraphQL interception ─────────────────────────────────────────────────
    def handle_response(response):
        if not browser_alive:
            return
        if "graphql" not in response.url.lower() or response.status != 200:
            return
        try:
            raw = response.text()
            print(f"   [API] Intercepted GraphQL ({len(raw)} bytes): {response.url[:80]}")
            for block in extract_data_blocks(raw):
                for s in find_stories(block):
                    post_id = s.get("post_id") or s.get("id")
                    if not post_id:
                        continue

                    pid_str = str(post_id)

                    msg = find_actual_message(s) or "[Media post - no text]"
                    user = find_actual_user(s)

                    # Extract timestamp
                    creation_time = None
                    for tf in TIME_FIELDS:
                        val = find_numeric_time(s, tf)
                        if val is not None:
                            creation_time = val
                            break

                    post_date, post_time, _ = parse_facebook_date(creation_time, ref_time)
                    if not post_date:
                        post_date = ref_time.strftime('%Y-%m-%d')
                    if not post_time:
                        post_time = ref_time.strftime('%H:%M')

                    # Age filter
                    try:
                        post_dt = datetime.datetime.strptime(
                            f"{post_date} {post_time}", "%Y-%m-%d %H:%M")
                        if (ref_time - post_dt).total_seconds() / 60 > AGE_LIMIT_MINUTES:
                            continue
                    except Exception:
                        pass

                    # Build URL
                    url = f"https://www.facebook.com/{post_id}"
                    meta_url = find_key_recursive(s, "url")
                    if meta_url and "facebook.com" in str(meta_url):
                        url = meta_url

                    if pid_str not in captured and pid_str not in existing and url not in existing:
                        print(f"   [API] ✓ {user[:25]} | {post_date} {post_time} | {msg[:40]}")
                        row = {
                            "post_url": url, "post_time": post_time, "post_date": post_date,
                            "calendar_week": str(datetime.datetime.strptime(post_date, "%Y-%m-%d").isocalendar()[1]),
                            "weekday": datetime.datetime.strptime(post_date, "%Y-%m-%d").strftime("%A"),
                            "profile_name": user, "post_text": msg,
                            "scrape_timestamp": ref_time.isoformat()
                        }
                        captured[pid_str] = row
                        notify_api(pid_str, row, ref_time)

        except Exception as e:
            print(f"   [API Error] {e}")

    # ── Browser ──────────────────────────────────────────────────────────────
    with sync_playwright() as p:
        # Validate auth
        auth_path = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        if auth_path:
            try:
                with open(auth_path) as f:
                    a = json.load(f)
                if not a.get("cookies"):
                    print("   [WARNING] Auth file has no cookies. Running unauthenticated.")
                    auth_path = None
                else:
                    print(f"   Auth loaded: {len(a['cookies'])} cookies.")
            except Exception as e:
                print(f"   [WARNING] Auth file error: {e}. Running unauthenticated.")
                auth_path = None
        else:
            print(f"   [WARNING] No auth file at {STORAGE_STATE}.")

        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = browser.new_context(
            storage_state=auth_path,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800}
        )
        page = ctx.new_page()
        page.on("response", handle_response)

        print(f"Navigating to {GROUP_URL} ...")
        try:
            page.goto(GROUP_URL, wait_until="commit", timeout=120000)
        except Exception as e:
            print(f"❌ Navigation failed: {e}")
            try:
                page.screenshot(path="vps_error.png")
            except: pass
            browser_alive = False
            browser.close()
            return

        current_url = page.url
        print(f"   Landed on: {current_url[:80]}")
        if "login" in current_url.lower() or "checkpoint" in current_url.lower():
            print("❌ Redirected to login/checkpoint. Session expired!")
            try:
                page.screenshot(path="vps_error.png")
            except: pass
            browser_alive = False
            browser.close()
            return

        # Modal bypass — same as scraper_playwright.py
        BYPASS_TEXTS = ["Continuer en tant que", "Continue as", "Continuar como"]
        for attempt in range(3):
            found = False
            for txt in BYPASS_TEXTS:
                target = page.get_by_text(txt, exact=False).first
                if target.count() > 0:
                    print(f"   Modal: clicking '{txt}'")
                    target.click(force=True, timeout=5000)
                    time.sleep(10)
                    found = True
                    break
            if found:
                break
            time.sleep(5)

        # Wait for feed
        try:
            page.wait_for_selector('[role="main"]', timeout=45000)
        except Exception:
            print("   ⚠️ Timeout waiting for [role='main']. Refreshing...")
            try:
                page.reload(wait_until="commit", timeout=60000)
                time.sleep(15)
                page.wait_for_selector('[role="main"]', timeout=30000)
            except Exception as re_e:
                print(f"   ⚠️ Reload also failed: {re_e}. Continuing anyway.")

        time.sleep(10)  # Hydration buffer for JS-heavy page
        try:
            page.screenshot(path="vps_db_check.png")
            print("   📸 Screenshot: vps_db_check.png")
        except: pass

        # ── DOM capture + Scroll loop ────────────────────────────────────────
        dom_captured = {}  # dedup_key -> row dict

        def scrape_dom():
            """Extract posts from visible DOM using dual selectors."""
            try:
                dom_posts = page.evaluate("""() => {
                    const posts = [];
                    const seen = new Set();
                    const feed = document.querySelector('[role="feed"]');
                    const containers = feed ? Array.from(feed.children) : [];
                    document.querySelectorAll('[role="article"]').forEach(a => containers.push(a));

                    for (const item of containers) {
                        try {
                            let user = '';
                            const headingLink = item.querySelector('h2 a, h3 a, h4 a, strong a, [data-ad-rendering-role="profile_name"] a');
                            if (headingLink) user = headingLink.textContent.trim();
                            if (!user || user === 'Nouvelles publications' || user === 'Recent posts') continue;

                            let text = '';
                            const allDirAuto = item.querySelectorAll('[dir="auto"]');
                            let maxLen = 0;
                            allDirAuto.forEach(el => {
                                const t = el.textContent.trim();
                                if (t.length > maxLen && t.length > 5) { maxLen = t.length; text = t; }
                            });

                            let url = '';
                            const links = item.querySelectorAll('a[href]');
                            for (const link of links) {
                                const href = link.getAttribute('href');
                                if (href && (href.includes('/posts/') || href.includes('/permalink/') || href.includes('/p/'))) {
                                    url = href.startsWith('http') ? href : 'https://www.facebook.com' + href;
                                    break;
                                }
                            }
                            if (!url && headingLink) {
                                const href = headingLink.getAttribute('href');
                                if (href) url = href.startsWith('http') ? href : 'https://www.facebook.com' + href;
                            }

                            const key = user + '|' + (text || '').substring(0, 50);
                            if (seen.has(key)) continue;
                            seen.add(key);

                            posts.push({ user, text: text || '[Media post - no text]', url: url || '' });
                        } catch(e) {}
                    }
                    return posts;
                }""")

                new_count = 0
                for dp in dom_posts:
                    norm_text = re.sub(r'\s+', '', dp['text'].lower())
                    dedup_key = f"{dp['user']}_{hashlib.md5(norm_text.encode()).hexdigest()}"
                    if dedup_key in dom_captured:
                        continue

                    post_date = ref_time.strftime('%Y-%m-%d')
                    post_time = ref_time.strftime('%H:%M')

                    row = {
                        "post_url": dp['url'] or f"dom://{dedup_key}",
                        "post_time": post_time, "post_date": post_date,
                        "calendar_week": str(ref_time.isocalendar()[1]),
                        "weekday": ref_time.strftime("%A"),
                        "profile_name": dp['user'],
                        "post_text": dp['text'],
                        "scrape_timestamp": ref_time.isoformat(),
                        "_dedup_key": dedup_key
                    }
                    dom_captured[dedup_key] = row
                    notify_api(dedup_key, row, ref_time)
                    new_count += 1
                return new_count
            except Exception as e:
                print(f"   [DOM Warning] {e}")
                return 0

        def enrich_dom_with_api():
            """Enrich DOM posts with timestamps from API data."""
            enriched = 0
            for key, dom_row in dom_captured.items():
                dom_norm = re.sub(r'\s+', '', dom_row['post_text'].lower())[:80]
                for api_row in captured.values():
                    api_norm = re.sub(r'\s+', '', api_row['post_text'].lower())[:80]
                    if dom_norm == api_norm or (dom_row['profile_name'] == api_row['profile_name'] and len(dom_norm) > 10 and dom_norm[:40] == api_norm[:40]):
                        dom_row['post_time'] = api_row['post_time']
                        dom_row['post_date'] = api_row['post_date']
                        dom_row['calendar_week'] = api_row['calendar_week']
                        dom_row['weekday'] = api_row['weekday']
                        if api_row['post_url'] and 'facebook.com' in api_row['post_url']:
                            dom_row['post_url'] = api_row['post_url']
                        enriched += 1
                        break
            return enriched

        initial = scrape_dom()
        if initial > 0:
            print(f"   Initial DOM: {initial} posts.")

        stall_count = 0
        for i in range(MAX_SCROLLS):
            prev_dom = len(dom_captured)
            prev_api = len(captured)

            for _ in range(3):
                page.keyboard.press("PageDown")
                time.sleep(1.5)
            time.sleep(2)

            scrape_dom()

            if len(dom_captured) == prev_dom and len(captured) == prev_api:
                stall_count += 1
                time.sleep(3)
                page.keyboard.press("PageUp")
                time.sleep(1.5)
                page.keyboard.press("PageDown")
                time.sleep(1.5)
                scrape_dom()
                if stall_count >= 5:
                    print(f"\n   Stop: Feed exhausted after {stall_count} stall batches.")
                    break
            else:
                stall_count = 0

            if i % 2 == 0:
                print(f"   Scroll {i+1}/{MAX_SCROLLS} | DOM: {len(dom_captured)} | API: {len(captured)}")

        enriched = enrich_dom_with_api()
        print(f"   Enriched {enriched} DOM posts with API timestamps.")

        browser_alive = False
        browser.close()

    # ── Merge DOM + API, then save to SQLite ────────────────────────────────
    # Start with DOM posts, then add API-only posts
    all_posts = dict(dom_captured)
    api_only = 0
    for pid, api_row in captured.items():
        api_norm = re.sub(r'\s+', '', api_row['post_text'].lower())
        api_key = f"{api_row['profile_name']}_{hashlib.md5(api_norm.encode()).hexdigest()}"
        already_in = api_key in all_posts
        if not already_in:
            for dom_row in dom_captured.values():
                dom_norm = re.sub(r'\s+', '', dom_row['post_text'].lower())[:80]
                if api_norm[:80] == dom_norm:
                    already_in = True
                    break
        if not already_in and len(api_row['post_text']) > 1:
            all_posts[api_key] = api_row
            api_only += 1
    if api_only:
        print(f"   Added {api_only} API-only posts.")

    print(f"Total merged: {len(all_posts)} ({len(dom_captured)} DOM + {api_only} API-only)")
    saved = 0
    for key, row in all_posts.items():
        url = row['post_url']
        # Skip synthetic DOM URLs for dedup check
        if url.startswith('dom://'):
            # Use text-based dedup
            norm = re.sub(r'\s+', '', row['post_text'].lower())
            text_key = f"{row['profile_name']}_{hashlib.md5(norm.encode()).hexdigest()}_{row['post_date']}"
            if text_key in existing:
                continue
        else:
            pid = extract_post_id(url)
            if url in existing or (pid and pid in existing):
                continue

        try:
            upsert_post(conn, row)
            existing.add(url)
            saved += 1
        except Exception as e:
            print(f"   [DB Error] {e}")

    conn.commit()
    conn.close()
    print(f"✅ Done. Saved {saved} new posts to SQLite.")


if __name__ == "__main__":
    main()
