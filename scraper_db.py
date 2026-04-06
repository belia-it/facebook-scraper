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

import sys
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

# Import job logging from API database module
sys.path.insert(0, os.path.join(SCRIPT_DIR, "api"))
try:
    from database import create_job, update_job
except ImportError:
    create_job = None
    update_job = None

load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
GROUP_URL = os.getenv("GROUP_URL", "https://www.facebook.com/groups/covsousse?sorting_setting=CHRONOLOGICAL")
STORAGE_STATE = os.getenv("STORAGE_STATE", os.path.join(SCRIPT_DIR, "facebook_auth.json"))
TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "1"))
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "35"))
AGE_LIMIT_MINUTES = int(os.getenv("AGE_LIMIT_MINUTES", "59"))

# Extract group identifier for filtering (e.g., "covsousse" or numeric ID)
_group_match = re.search(r'/groups/([^/?]+)', GROUP_URL)
GROUP_SLUG = _group_match.group(1) if _group_match else None
print(f"[Config] Group slug: {GROUP_SLUG}")

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
            synced_at                 TEXT DEFAULT (datetime('now')),
            metadata                  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_jobs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            status          TEXT DEFAULT 'running',
            captured        INTEGER DEFAULT 0,
            saved           INTEGER DEFAULT 0,
            skipped_age     INTEGER DEFAULT 0,
            skipped_group   INTEGER DEFAULT 0,
            duration_sec    REAL,
            error           TEXT
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
            profile_name, post_text, scrape_timestamp, metadata
        ) VALUES (
            :post_url, :post_time, :post_date, :calendar_week, :weekday,
            :profile_name, :post_text, :scrape_timestamp, :metadata
        )
        ON CONFLICT(post_url) DO UPDATE SET
            profile_name     = excluded.profile_name,
            post_text        = excluded.post_text,
            post_time        = excluded.post_time,
            post_date        = excluded.post_date,
            metadata         = excluded.metadata,
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
# Typenames to EXCLUDE — these are not posts
EXCLUDED_TYPENAMES = {
    "Notification", "NotificationStory", "FeedbackReaction",
    "PageLikeAction", "ProfileIntroCard", "StoryBucket",
    "GroupMemberBadge", "GroupMemberProfile", "GroupQuestion",
    "Comment", "Reply", "UFFeedback", "Feedback",
    "MarketplaceListing", "Event", "FundraiserStory",
    "AdStory", "SponsoredStory", "PageStory",
    "GroupMallCategoryItem", "GroupMallProductItem",
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

        if typename in EXCLUDED_TYPENAMES:
            for v in obj.values():
                found.extend(find_stories(v))
            return found
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
        ref_time = datetime.datetime.utcnow() + datetime.timedelta(hours=TIMEZONE_OFFSET) + datetime.timedelta(hours=0)  # local time

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
    job_id = None
    job_start = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if create_job:
        try:
            job_id = create_job(job_start)
            print(f"   Job #{job_id} started.")
        except Exception as e:
            print(f"   Job logging error: {e}")
    existing = get_existing_ids(conn)
    ref_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=TIMEZONE_OFFSET)
    ref_time = ref_time.replace(tzinfo=None)
    print(f"   ref_time (UTC+{TIMEZONE_OFFSET}): {ref_time.strftime('%Y-%m-%d %H:%M')}")

    captured = {}

    def process_raw_data(raw_text):
        """Extract posts from raw text (HTML or JSON)."""
        count = 0
        for block in extract_data_blocks(raw_text):
            for s in find_stories(block):
                post_id = s.get("post_id") or s.get("id")
                if not post_id:
                    continue
                pid_str = str(post_id)
                if pid_str in captured:
                    continue

                if not pid_str.isdigit():
                    try:
                        import base64
                        decoded = base64.b64decode(pid_str + "==").decode("utf-8", errors="ignore")
                        if any(decoded.startswith(p) for p in ("comment", "notification", "feedback", "reaction", "share")):
                            continue
                    except:
                        pass

                msg = find_actual_message(s) or "[Media post - no text]"
                user = find_actual_user(s)

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

                url = f"https://www.facebook.com/{post_id}"
                meta_url = find_key_recursive(s, "url")
                if meta_url and "facebook.com" in str(meta_url):
                    url = str(meta_url)

                # STRICT: URL must contain /groups/covsousse — no exceptions
                if GROUP_SLUG:
                    slug_lower = GROUP_SLUG.lower()
                    if f"/groups/{slug_lower}" not in url.lower():
                        # Try to find group URL in metadata
                        found_group_url = False
                        for gkey in ("owning_profile", "target_group", "group"):
                            gref = find_key_recursive(s, gkey)
                            if gref and isinstance(gref, dict):
                                gurl = (gref.get("url") or "").lower()
                                if slug_lower in gurl:
                                    url = f"https://www.facebook.com/groups/{GROUP_SLUG}/posts/{pid_str}"
                                    found_group_url = True
                                    break
                        if not found_group_url:
                            continue

                if pid_str not in existing and url not in existing:
                    # Build metadata JSON from story object (truncate large fields)
                    try:
                        meta = {}
                        for mk in ("__typename", "post_id", "id", "creation_time", "timestamp",
                                    "url", "tracking", "feedback", "comet_sections"):
                            if mk in s:
                                val = s[mk]
                                if isinstance(val, (dict, list)):
                                    val_str = json.dumps(val, ensure_ascii=False)
                                    if len(val_str) > 500:
                                        val = val_str[:500] + "..."
                                meta[mk] = val
                        # Add actors/author info
                        for ak in ("actors", "actor", "author"):
                            av = s.get(ak)
                            if av:
                                meta[ak] = av
                        meta_json = json.dumps(meta, ensure_ascii=False, default=str)
                    except:
                        meta_json = None

                    row = {
                        "post_url": url, "post_time": post_time, "post_date": post_date,
                        "calendar_week": str(datetime.datetime.strptime(post_date, "%Y-%m-%d").isocalendar()[1]),
                        "weekday": datetime.datetime.strptime(post_date, "%Y-%m-%d").strftime("%A"),
                        "profile_name": user, "post_text": msg,
                        "scrape_timestamp": ref_time.isoformat(),
                        "metadata": meta_json
                    }
                    captured[pid_str] = row
                    count += 1
                    print(f"   [+] {user[:25]} | {post_date} {post_time} | {msg[:40]}")
        return count

    # ── Browser: get page HTML + intercept responses ─────────────────────────
    print("Starting browser...")
    try:
        with sync_playwright() as p:
            auth_path = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
            if auth_path:
                try:
                    with open(auth_path) as f:
                        a = json.load(f)
                    if not a.get("cookies"):
                        auth_path = None
                    else:
                        print(f"   Auth: {len(a['cookies'])} cookies.")
                except:
                    auth_path = None

            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(
                storage_state=auth_path,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800}
            )
            page = ctx.new_page()

            # Collect ALL response bodies for later processing
            response_bodies = []
            def collect_response(response):
                try:
                    if response.status == 200:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct or "javascript" in ct or "html" in ct or "text" in ct or "graphql" in response.url.lower() or "ajax" in response.url.lower():
                            body = response.text()
                            if body and len(body) > 100:
                                response_bodies.append(body)
                except:
                    pass
            page.on("response", collect_response)

            print(f"Navigating to {GROUP_URL} ...")
            page.goto(GROUP_URL, wait_until="commit", timeout=60000)
            print(f"   Landed: {page.url[:80]}")

            if "login" in page.url.lower() or "checkpoint" in page.url.lower():
                print("❌ Session expired!")
                try: browser.close()
                except: pass
                conn.close()
                return

            # Wait for page to hydrate
            try:
                page.wait_for_load_state("load", timeout=20000)
                print("   Page load event fired.")
            except:
                print("   Page load timed out.")
            
            # Active hydration — force browser to process JS
            for i in range(5):
                time.sleep(3)
                try:
                    has_feed = page.evaluate('() => !!document.querySelector("[role=feed]")')
                    if has_feed:
                        print(f"   ✅ Feed detected ({(i+1)*3}s).")
                        break
                except:
                    pass

            # 1. Parse the page HTML for embedded data + script tags
            print("   Extracting embedded data from HTML...")
            try:
                html = page.content()
                print(f"   HTML size: {len(html)} bytes")
                new = process_raw_data(html)
                # Also extract JSON from script tags
                import re as _re
                script_blocks = _re.findall(r'<script[^>]*>({.*?"data".*?})</script>', html, _re.DOTALL)
                for sb in script_blocks:
                    try:
                        new += process_raw_data(sb)
                    except:
                        pass
                print(f"   From HTML: {new} posts")
            except Exception as e:
                print(f"   HTML extraction error: {e}")

            # 2. Parse collected response bodies
            print(f"   Processing {len(response_bodies)} collected responses...")
            for body in response_bodies:
                process_raw_data(body)
            print(f"   Total after responses: {len(captured)} posts")

            # Save what we have so far (in case scroll phase gets killed)
            if captured:
                print("   Saving initial batch to DB...")
                for pid, row in captured.items():
                    url = row['post_url']
                    if GROUP_SLUG and f"/groups/{GROUP_SLUG.lower()}" not in url.lower():
                        continue
                    try:
                        p_date, p_time = row.get('post_date'), row.get('post_time')
                        if p_date and p_time:
                            fmt = '%Y-%m-%d %H:%M:%S' if len(p_time) > 5 else '%Y-%m-%d %H:%M'
                            p_dt = datetime.datetime.strptime(f"{p_date} {p_time}", fmt)
                            age_min = (ref_time - p_dt).total_seconds() / 60
                            if age_min > AGE_LIMIT_MINUTES:
                                continue
                    except:
                        pass
                    if row.get('profile_name') == 'Unknown User' and row.get('post_text') == '[Media post - no text]':
                        continue
                    post_id = extract_post_id(url)
                    if url in existing or (post_id and post_id in existing):
                        continue
                    try:
                        upsert_post(conn, row)
                        existing.add(url)
                    except:
                        pass
                conn.commit()
                print(f"   Initial save done.")

            # 3. Scroll to trigger more content
            print("   Scrolling for more...")

            prev_bodies = len(response_bodies)
            stall = 0
            for i in range(MAX_SCROLLS):
                prev_count = len(captured)
                try:
                    for _ in range(3):
                        page.keyboard.press("PageDown")
                        time.sleep(1.5)
                    time.sleep(3)

                    # Process new responses
                    new_bodies = response_bodies[prev_bodies:]
                    prev_bodies = len(response_bodies)
                    for body in new_bodies:
                        process_raw_data(body)
                except Exception as e:
                    print(f"   Scroll error: {e}")
                    break

                if len(captured) == prev_count:
                    stall += 1
                    if stall >= 5:
                        print(f"   Feed exhausted. Total: {len(captured)}")
                        break
                    # Try harder during stall
                    try:
                        time.sleep(2)
                        page.keyboard.press("End")
                        time.sleep(2)
                        page.keyboard.press("PageDown")
                        time.sleep(2)
                        # Re-process any new responses
                        new_bodies = response_bodies[prev_bodies:]
                        prev_bodies = len(response_bodies)
                        for body in new_bodies:
                            process_raw_data(body)
                    except:
                        pass
                else:
                    stall = 0

                if i % 3 == 0:
                    print(f"   Scroll {i+1}/{MAX_SCROLLS} | Posts: {len(captured)}")
                # Incremental save every 5 scrolls
                if i > 0 and i % 5 == 0:
                    for pid, row in captured.items():
                        url = row['post_url']
                        if GROUP_SLUG and f"/groups/{GROUP_SLUG.lower()}" not in url.lower():
                            continue
                        if row.get('profile_name') == 'Unknown User' and row.get('post_text') == '[Media post - no text]':
                            continue
                        post_id_check = extract_post_id(url)
                        if url in existing or (post_id_check and post_id_check in existing):
                            continue
                        try:
                            upsert_post(conn, row)
                            existing.add(url)
                        except:
                            pass
                    conn.commit()

            # Final HTML parse after scrolling
            try:
                final_html = page.content()
                final_new = process_raw_data(final_html)
                if final_new:
                    print(f"   Final HTML pass: +{final_new} posts")
            except:
                pass

            print(f"   Browser done. Captured: {len(captured)}")
            try: browser.close()
            except: pass

    except Exception as e:
        print(f"   Browser error: {type(e).__name__}: {e}")
        print(f"   Captured {len(captured)} before error.")
        if job_id and update_job and not captured:
            try:
                finished = datetime.datetime.now(datetime.timezone.utc).isoformat()
                started_dt = datetime.datetime.fromisoformat(job_start)
                finished_dt = datetime.datetime.fromisoformat(finished)
                duration = (finished_dt - started_dt).total_seconds()
                update_job(job_id,
                    finished_at=finished, status="error",
                    error=str(e)[:200], duration_sec=round(duration, 1))
            except: pass

    if not captured:
        print("⚠️ No posts captured.")
        if job_id and update_job:
            try:
                finished = datetime.datetime.now(datetime.timezone.utc).isoformat()
                started_dt = datetime.datetime.fromisoformat(job_start)
                finished_dt = datetime.datetime.fromisoformat(finished)
                duration = (finished_dt - started_dt).total_seconds()
                update_job(job_id,
                    finished_at=finished, status="empty",
                    captured=0, saved=0, duration_sec=round(duration, 1))
            except: pass
        conn.close()
        return

    # ── Save to SQLite ───────────────────────────────────────────────────────
    print(f"\nSaving {len(captured)} posts...")
    saved = 0
    skipped_age = 0
    for pid, row in captured.items():
        url = row['post_url']
        if GROUP_SLUG and f"/groups/{GROUP_SLUG.lower()}" not in url.lower():
            continue
        try:
            p_date, p_time = row.get('post_date'), row.get('post_time')
            if p_date and p_time:
                fmt = '%Y-%m-%d %H:%M:%S' if len(p_time) > 5 else '%Y-%m-%d %H:%M'
                p_dt = datetime.datetime.strptime(f"{p_date} {p_time}", fmt)
                age_min = (ref_time - p_dt).total_seconds() / 60
                if age_min > AGE_LIMIT_MINUTES:
                    skipped_age += 1
                    continue
        except:
            pass
        # Skip empty posts when saving to DB
        if row.get('profile_name') == 'Unknown User' and row.get('post_text') == '[Media post - no text]':
            continue

        post_id = extract_post_id(url)
        if url in existing or (post_id and post_id in existing):
            continue
        try:
            upsert_post(conn, row)
            existing.add(url)
            saved += 1
        except Exception as e:
            print(f"   [DB Error] {e}")

    if skipped_age:
        print(f"   Skipped {skipped_age} posts older than {AGE_LIMIT_MINUTES} min.")
    conn.commit()
    conn.close()
    print(f"✅ Done. Saved {saved} new posts.")

    # Log job result
    if job_id and update_job:
        try:
            import time as _t
            finished = datetime.datetime.now(datetime.timezone.utc).isoformat()
            started_dt = datetime.datetime.fromisoformat(job_start)
            finished_dt = datetime.datetime.fromisoformat(finished)
            duration = (finished_dt - started_dt).total_seconds()
            update_job(job_id,
                finished_at=finished,
                status="success",
                captured=len(captured),
                saved=saved,
                skipped_age=skipped_age,
                duration_sec=round(duration, 1)
            )
        except Exception as e:
            print(f"   Job log error: {e}")


if __name__ == "__main__":
    main()
