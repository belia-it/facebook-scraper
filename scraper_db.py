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
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "api", "_scrape_progress.json")

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
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "200"))  # safety cap; adaptive logic stops earlier
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
            metadata                  TEXT,
            job_id                    INTEGER,
            captured_at               TEXT
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
            profile_name, post_text, scrape_timestamp, metadata, job_id, captured_at
        ) VALUES (
            :post_url, :post_time, :post_date, :calendar_week, :weekday,
            :profile_name, :post_text, :scrape_timestamp, :metadata, :job_id, :captured_at
        )
        ON CONFLICT(post_url) DO UPDATE SET
            profile_name     = excluded.profile_name,
            post_text        = excluded.post_text,
            post_time        = excluded.post_time,
            post_date        = excluded.post_date,
            metadata         = excluded.metadata,
            job_id           = excluded.job_id,
            captured_at      = excluded.captured_at,
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
    "UserPost", "FeedStory", "GroupCommerceProductItem",
    "GroupFeedUnit", "CometFeedStory", "FeedObject",
    "GroupDiscussionRootStory", "GroupQuestion",
}
# Typenames to EXCLUDE — these are not posts
EXCLUDED_TYPENAMES = {
    "Notification", "NotificationStory", "FeedbackReaction",
    "PageLikeAction", "ProfileIntroCard", "StoryBucket",
    "GroupMemberBadge", "GroupMemberProfile",
    "Comment", "Reply", "UFFeedback", "Feedback",
    "MarketplaceListing", "Event", "FundraiserStory",
    "AdStory", "SponsoredStory", "PageStory",
    "GroupMallCategoryItem", "GroupMallProductItem",
}
TIME_FIELDS = {
    "creation_time", "timestamp", "publish_time", "created_time",
    "publish_timestamp", "created_timestamp",
    "updated_time", "update_time", "last_edit_time", "edit_time",
    "bumped_time", "last_modified_time", "modified_time",
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
        has_message = "message" in obj and isinstance(obj.get("message"), dict)
        has_url = "url" in obj and isinstance(obj.get("url"), str) and "facebook.com" in (obj.get("url") or "")

        if typename in EXCLUDED_TYPENAMES:
            for v in obj.values():
                found.extend(find_stories(v))
            return found

        is_story = typename in STORY_TYPENAMES
        if not is_story and has_time and has_actors:
            is_story = True
        if not is_story and has_time and has_post_id:
            is_story = True
        # Broader: object with message text + any identifier (time, url, or id)
        if not is_story and has_message and (has_time or has_url or has_post_id):
            is_story = True
        # Even broader: object with comet_sections (Facebook's story wrapper)
        if not is_story and "comet_sections" in obj and (has_time or has_post_id):
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
    # Direct message field
    m = s.get("message")
    if isinstance(m, dict) and "text" in m:
        t = m["text"].strip()
        if t and t not in _FB_INTERNAL_LABELS:
            return t
    # Try comet_sections -> content -> story -> message -> text
    cs = s.get("comet_sections")
    if isinstance(cs, dict):
        content_obj = cs.get("content")
        if isinstance(content_obj, dict):
            story_obj = content_obj.get("story")
            if isinstance(story_obj, dict):
                m2 = story_obj.get("message")
                if isinstance(m2, dict) and "text" in m2:
                    t = m2["text"].strip()
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

    # Try owning_profile or target -> name
    for pk in ("owning_profile", "target", "node"):
        pv = s.get(pk)
        if isinstance(pv, dict):
            name = pv.get("name")
            if name and name not in ("Unknown User", ""):
                return name

    # Try tracking -> content_owner -> name
    tracking = s.get("tracking")
    if isinstance(tracking, dict):
        co = tracking.get("content_owner")
        if isinstance(co, dict):
            name = co.get("name")
            if name and name not in ("Unknown User", ""):
                return name

    # Recursive search for first name in an actor-like object
    def find_author_name(obj, depth=0):
        if depth > 5:
            return None
        if isinstance(obj, dict):
            tn = obj.get("__typename", "")
            if ("User" in tn or "Profile" in tn or "Author" in tn or "Actor" in tn) and "name" in obj:
                n = obj["name"]
                if n and n not in ("Unknown User", "", "Facebook") and len(n) > 1:
                    return n
            for v in obj.values():
                r = find_author_name(v, depth+1)
                if r:
                    return r
        elif isinstance(obj, list):
            for v in obj:
                r = find_author_name(v, depth+1)
                if r:
                    return r
        return None

    deep_name = find_author_name(s)
    if deep_name:
        return deep_name

    # Try comet_sections -> content -> story -> actors
    cs = s.get("comet_sections")
    if isinstance(cs, dict):
        content_obj = cs.get("content")
        if isinstance(content_obj, dict):
            story_obj = content_obj.get("story")
            if isinstance(story_obj, dict):
                for ak in ("actors", "actor", "author"):
                    av = story_obj.get(ak)
                    if isinstance(av, list) and av:
                        name = av[0].get("name") if isinstance(av[0], dict) else None
                        if name and name != "Unknown User":
                            return name
                    elif isinstance(av, dict):
                        name = av.get("name")
                        if name and name != "Unknown User":
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
        return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M:%S'), ""

    try:
        if isinstance(date_str, (int, float)) or (
                isinstance(date_str, str) and str(date_str).isdigit()):
            ts = int(date_str)
            dt_utc = datetime.datetime.utcfromtimestamp(ts)
            target = dt_utc + datetime.timedelta(hours=TIMEZONE_OFFSET)
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), f"API_{ts}"
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
            return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M:%S'), date_str

        exact_fr = re.search(
            r'(\d{1,2})\s+(' + all_month_names + r')\s+(\d{4})\s+[àa]\s+(\d{1,2}):(\d{2})', ds)
        if exact_fr:
            day, m_name, year, h, m = exact_fr.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

        exact_en = re.search(
            r'(' + all_month_names + r')\s+(\d{1,2}),?\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})', ds)
        if exact_en:
            m_name, day, year, h, m = exact_en.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

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
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

        if 'hier' in ds or 'yesterday' in ds:
            time_match = re.search(r'(\d{1,2})[:h](\d{2})', ds)
            if time_match:
                h, m = time_match.groups()
                target = (ref_time - datetime.timedelta(days=1)).replace(
                    hour=int(h), minute=int(m), second=0)
            else:
                target = ref_time - datetime.timedelta(days=1)
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

    except Exception as e:
        print(f"   [Warning] Date parsing failed for '{date_str}': {e}")

    return None, None, date_str


def extract_post_id(url):
    m = re.search(r'/(?:posts|permalink|p)/(\d+)', str(url))
    return m.group(1) if m else None


NOTIFIED_POSTS = set()


def report_progress(phase, detail="", captured=0, saved=0, scroll=None, max_scroll=None):
    try:
        data = {
            "phase": phase,
            "detail": detail,
            "captured": captured,
            "saved": saved,
            "scroll": scroll,
            "max_scroll": max_scroll,
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(data, f)
    except:
        pass


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
    print(f"   ref_time (UTC+{TIMEZONE_OFFSET}): {ref_time.strftime('%Y-%m-%d %H:%M:%S')}")

    captured = {}
    saved_count = [0]  # mutable counter for incremental saves
    skip_reasons = {
        "no_post_id": 0, "dup_pid": 0, "base64_skip": 0,
        "no_text": 0, "no_timestamp": 0, "not_group": 0,
        "dup_unknown_twin": 0, "date_parse_failed": 0,
    }

    def process_raw_data(raw_text):
        """Extract posts from raw text (HTML or JSON)."""
        count = 0
        for block in extract_data_blocks(raw_text):
            for s in find_stories(block):
                post_id = s.get("post_id") or s.get("id")
                if not post_id:
                    skip_reasons["no_post_id"] += 1
                    continue
                pid_str = str(post_id)
                if pid_str in captured:
                    skip_reasons["dup_pid"] += 1
                    continue

                if not pid_str.isdigit():
                    try:
                        import base64
                        decoded = base64.b64decode(pid_str + "==").decode("utf-8", errors="ignore")
                        if any(decoded.startswith(p) for p in ("comment", "notification", "feedback", "reaction", "share")):
                            skip_reasons["base64_skip"] += 1
                            continue
                    except:
                        pass

                msg = find_actual_message(s)
                user = find_actual_user(s)

                # Skip posts without real text content — carpooling posts always have text.
                # Media-only posts (photos/videos with no caption) are not useful for this use case.
                if not msg:
                    skip_reasons["no_text"] += 1
                    # Save first 3 samples for inspection
                    if skip_reasons["no_text"] <= 3:
                        try:
                            import os as _os, json as _j
                            sample_path = "/tmp/no_text_sample_" + str(skip_reasons["no_text"]) + ".json"
                            if not _os.path.exists(sample_path):
                                with open(sample_path, "w") as _f:
                                    # Save a trimmed version — don't dump massive comet_sections
                                    trimmed = {k: v for k, v in s.items() if k not in ("comet_sections", "feedback")}
                                    trimmed["_top_level_keys"] = list(s.keys())
                                    _j.dump(trimmed, _f, indent=2, ensure_ascii=False, default=str)
                        except: pass
                    continue

                # Collect ALL timestamp values in this story and use the LATEST one.
                # Posts edited/bumped get a recent update_time even though creation_time is old.
                creation_time = None
                all_times = []
                for tf in TIME_FIELDS:
                    val = find_numeric_time(s, tf)
                    if val is not None and 1000000000 < val < 9999999999:  # sane unix ts range
                        all_times.append(val)
                if all_times:
                    creation_time = max(all_times)

                # No numeric timestamp? Try to find a relative time string ("5 min", "2 h", ...)
                # in the story before giving up.
                if creation_time is None:
                    def _find_reltime(obj, depth=0):
                        if depth > 6: return None
                        if isinstance(obj, str):
                            s_ = obj.lower().strip()
                            if s_ and len(s_) < 40 and re.search(r"\b\d+\s*(min|mins?|h|heures?|jours?|j|days?|d|sec|seconds?|maintenant|now|just|hier|yesterday)\b", s_):
                                return obj
                        elif isinstance(obj, dict):
                            for v in obj.values():
                                r = _find_reltime(v, depth+1)
                                if r: return r
                        elif isinstance(obj, list):
                            for v in obj:
                                r = _find_reltime(v, depth+1)
                                if r: return r
                        return None
                    rel_str = _find_reltime(s)
                    if rel_str:
                        post_date, post_time, _ = parse_facebook_date(rel_str, ref_time)
                        if post_date and post_time:
                            # Reconstruct as a unix-style timestamp path: we already have post_date/time
                            creation_time = rel_str  # marker so we skip numeric parse below
                if creation_time is None:
                    skip_reasons["no_timestamp"] += 1
                    continue

                if isinstance(creation_time, (int, float)):
                    post_date, post_time, _ = parse_facebook_date(creation_time, ref_time)
                # else: already parsed above from rel_str
                if not post_date or not post_time:
                    skip_reasons["date_parse_failed"] += 1
                    continue

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
                            skip_reasons["not_group"] += 1
                            continue

                if pid_str not in existing and url not in existing:
                    # Dedup: skip "Unknown User" if a named post at same timestamp already captured
                    if user == "Unknown User" and msg == "[Media post - no text]":
                        post_date_val, post_time_val, _ = parse_facebook_date(creation_time, ref_time)
                        ts_key = f"{post_date_val}_{post_time_val}"
                        # Check if a named post at this exact timestamp already exists
                        has_named = any(
                            r.get('profile_name') != 'Unknown User'
                            and f"{r.get('post_date')}_{r.get('post_time')}" == ts_key
                            for r in captured.values()
                        )
                        if has_named:
                            skip_reasons["dup_unknown_twin"] += 1
                            continue

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
                        "metadata": meta_json,
                        "job_id": job_id,
                        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    captured[pid_str] = row
                    count += 1
                    print(f"   [+] {user[:25]} | {post_date} {post_time} | {msg[:40]}")
        return count

    # ── Browser: get page HTML + intercept responses ─────────────────────────
    report_progress("starting", "Initializing browser...")
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

            report_progress("navigating", "Opening group page...")
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
            report_progress("hydrating", "Waiting for page to render...")
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
            report_progress("extracting", "Parsing page data...")
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
            report_progress("extracting", f"Processing {len(response_bodies)} responses...", captured=len(captured), saved=saved_count[0])
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
                            if (ref_time - p_dt).total_seconds() / 60 > AGE_LIMIT_MINUTES:
                                continue
                    except:
                        pass
                    post_id = extract_post_id(url)
                    if url in existing or (post_id and post_id in existing):
                        continue
                    try:
                        upsert_post(conn, row)
                        existing.add(url)
                        saved_count[0] += 1
                    except:
                        pass
                conn.commit()
                print(f"   Initial save: {saved_count[0]} posts.")

            # 3. Scroll to trigger more content
            print("   Scrolling for more...")

            # Helper: age (minutes) of the N-th oldest captured post, ignoring pinned/outlier posts.
            # Pinned posts (welcome messages) sit at the top with ancient timestamps. Using the
            # single oldest would make us stop too early. Instead, require at least N "normal" posts
            # to be older than the window before declaring it covered.
            def age_to_cover_window():
                # Return True if at least 3 normal (non-pinned) posts are older than AGE_LIMIT_MINUTES.
                # A "normal" post has age < 7 days (anything older is pinned/archival).
                SEVEN_DAYS_MIN = 7 * 24 * 60
                ages = []
                for row in captured.values():
                    p_date, p_time = row.get('post_date'), row.get('post_time')
                    if not (p_date and p_time):
                        continue
                    try:
                        fmt = '%Y-%m-%d %H:%M:%S' if len(p_time) > 5 else '%Y-%m-%d %H:%M'
                        p_dt = datetime.datetime.strptime(f"{p_date} {p_time}", fmt)
                        age = (ref_time - p_dt).total_seconds() / 60
                        # Ignore pinned/archival outliers
                        if age < SEVEN_DAYS_MIN:
                            ages.append(age)
                    except:
                        pass
                if not ages:
                    return 0, False, 0
                ages.sort()
                # Soft coverage: 5 old posts — start safety countdown
                # Hard coverage: 20 old posts — stop now (we have ample buffer past the window)
                old_count = sum(1 for a in ages if a > AGE_LIMIT_MINUTES)
                covered = old_count >= 5
                oldest_nonpinned = ages[-1]
                return oldest_nonpinned, covered, old_count

            def oldest_age_min():
                # For display only — age of oldest non-pinned post.
                age, _, _ = age_to_cover_window()
                return age

            prev_bodies = len(response_bodies)
            stall = 0
            window_covered = False
            dom_raw = {}  # url -> {author, text, timeText} collected across scroll positions

            # Inline JS that pulls visible feed posts from the DOM. Called during scrolling so
            # we capture posts that Facebook's virtualisation would otherwise drop from the tree.
            DOM_EXTRACT_JS = r"""() => {
                const feed = document.querySelector('[role="feed"]');
                if (!feed) return [];
                const out = [];
                for (const item of feed.children) {
                    try {
                        let author = '';
                        const ah = item.querySelector('h2 a, h3 a, strong a, [data-ad-rendering-role="profile_name"] a');
                        if (ah) author = ah.textContent.trim();
                        if (!author || author === 'Nouvelles publications' || author === 'Recent posts') continue;

                        let text = '', tlen = 0;
                        item.querySelectorAll('[dir="auto"]').forEach(el => {
                            const t = el.textContent.trim();
                            if (t.length > tlen && t.length > 5) { tlen = t.length; text = t; }
                        });
                        if (!text) continue;

                        let timeText = '';
                        item.querySelectorAll('a[href*="/posts/"], a[href*="/permalink/"], a[href*="/p/"]').forEach(a => {
                            if (timeText) return;
                            const t = a.textContent.trim();
                            if (t && t.length < 30) timeText = t;
                        });

                        let url = '';
                        for (const a of item.querySelectorAll('a[href]')) {
                            const href = a.getAttribute('href') || '';
                            const full = href.startsWith('http') ? href : 'https://www.facebook.com' + href;
                            if ((href.includes('/posts/') || href.includes('/permalink/') || href.includes('/p/')) &&
                                (full.includes('/groups/') || href.includes('/groups/'))) {
                                url = full.split('?')[0];
                                break;
                            }
                        }
                        // Keep posts even without a permalink — we'll synthesise a URL on the Python side
                        // so Facebook-just-rendered posts (no link yet) are not dropped.
                        out.push({ author, text, url, timeText });
                    } catch(e) {}
                }
                return out;
            }"""

            def capture_visible_dom():
                try:
                    posts = page.evaluate(DOM_EXTRACT_JS)
                except Exception as e:
                    return 0
                added = 0
                for p_ in posts or []:
                    u = p_.get("url") or ""
                    if not u:
                        # No permalink in DOM — synthesise a URL that still passes
                        # the /groups/{slug} filter and is unique per (author, text).
                        a = p_.get("author", "anon")
                        t = (p_.get("text", "") or "")[:100]
                        h = hashlib.md5((a + "::" + t).encode()).hexdigest()[:20]
                        u = f"https://www.facebook.com/groups/{GROUP_SLUG or 'unknown'}/posts/dom-{h}"
                        p_["url"] = u
                    if u not in dom_raw:
                        dom_raw[u] = p_
                        added += 1
                return added
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

                    # Extract whatever is visible in the DOM right now
                    before_dom = len(dom_raw)
                    capture_visible_dom()
                    dom_delta = len(dom_raw) - before_dom
                    if dom_delta:
                        print(f"   [DOM]  +{dom_delta} new (total in DOM buffer: {len(dom_raw)})")
                except Exception as e:
                    print(f"   Scroll error: {e}")
                    break

                # Smart stop: we have a strong signal when the buffer of "old" posts grows.
                # Soft (5 old) = window reached; Hard (20 old) = definitely past the window, stop.
                age_min, covered, old_count = age_to_cover_window()
                if covered and not window_covered:
                    window_covered = True
                    print(f"   Window reached (oldest: {age_min:.0f}m, {old_count} old posts). Continuing for safety...")
                # Hard stop: require 60 non-pinned posts older than window.
                # This ensures we've scrolled WELL past the window so everything in the
                # window has been seen (including posts that virtualisation only
                # exposes deeper in the feed).
                if old_count >= 60:
                    print(f"   Hard stop: {old_count} posts older than window, {len(captured)} total captured.")
                    break

                if len(captured) == prev_count:
                    stall += 1
                    if stall >= 5:
                        print(f"   Feed exhausted at scroll {i+1}. Total: {len(captured)}.")
                        break
                    # Try harder during stall
                    try:
                        time.sleep(2)
                        page.keyboard.press("End")
                        time.sleep(2)
                        page.keyboard.press("PageDown")
                        time.sleep(2)
                        new_bodies = response_bodies[prev_bodies:]
                        prev_bodies = len(response_bodies)
                        for body in new_bodies:
                            process_raw_data(body)
                    except:
                        pass
                else:
                    stall = 0

                if i % 3 == 0:
                    report_progress("scrolling", f"Scroll {i+1}/{MAX_SCROLLS}", captured=len(captured), saved=saved_count[0], scroll=i+1, max_scroll=MAX_SCROLLS)
                    _, _, _oc = age_to_cover_window()
                    print(f"   Scroll {i+1}/{MAX_SCROLLS} | Posts: {len(captured)} | Oldest: {age_min:.0f}m | Old: {_oc}")
                # Incremental save every 5 scrolls
                if i > 0 and i % 5 == 0:
                    for pid, row in captured.items():
                        url = row['post_url']
                        if GROUP_SLUG and f"/groups/{GROUP_SLUG.lower()}" not in url.lower():
                            continue
                        # Age filter
                        try:
                            p_date, p_time = row.get('post_date'), row.get('post_time')
                            if p_date and p_time:
                                fmt = '%Y-%m-%d %H:%M:%S' if len(p_time) > 5 else '%Y-%m-%d %H:%M'
                                p_dt = datetime.datetime.strptime(f"{p_date} {p_time}", fmt)
                                if (ref_time - p_dt).total_seconds() / 60 > AGE_LIMIT_MINUTES:
                                    continue
                        except:
                            pass
                        post_id_check = extract_post_id(url)
                        if url in existing or (post_id_check and post_id_check in existing):
                            continue
                        try:
                            upsert_post(conn, row)
                            existing.add(url)
                            saved_count[0] += 1
                        except:
                            pass
                    conn.commit()

            # Scroll back to top and re-scrape to catch posts added during the scrape
            print("   Scrolling back to top for final sweep...")
            try:
                page.keyboard.press("Home")
                time.sleep(3)
                page.keyboard.press("Home")
                time.sleep(3)
                # Collect any new responses from the top
                new_top = response_bodies[prev_bodies:]
                prev_bodies = len(response_bodies)
                for body in new_top:
                    process_raw_data(body)
            except:
                pass

            # Final HTML parse
            try:
                final_html = page.content()
                final_new = process_raw_data(final_html)
                if final_new:
                    print(f"   Final sweep: +{final_new} new posts")
            except:
                pass

            # ── Merge accumulated DOM-visible posts into captured ─────────────
            # dom_raw was populated during scrolling via capture_visible_dom().
            # For each DOM post not already in captured (via post_id / URL / text-match),
            # parse its relative time ("2 h", "45 min") and save.
            try:
                dom_posts = list(dom_raw.values())
                print(f"   DOM sweep: merging {len(dom_posts)} visible-DOM posts into captured")
                dom_added = 0
                dom_skipped_no_time = 0
                dom_skipped_dup = 0
                for dp in dom_posts:
                    post_url = dp.get("url", "")
                    if not post_url:
                        # Synthesise a stable URL based on author+text hash so the post can
                        # still be saved and deduplicated in future runs.
                        if GROUP_SLUG:
                            _author = dp.get("author", "anon")
                            _txt = (dp.get("text", "") or "")[:100]
                            _h = hashlib.md5((_author + "::" + _txt).encode()).hexdigest()[:20]
                            post_url = f"https://www.facebook.com/groups/{GROUP_SLUG}/posts/dom-{_h}"
                        else:
                            continue
                    # Normalize — only reject if URL exists but is for a different group
                    if GROUP_SLUG and f"/groups/{GROUP_SLUG.lower()}" not in post_url.lower():
                        continue

                    # Dedup: already captured by URL?
                    pid = extract_post_id(post_url)
                    already = False
                    if pid:
                        if pid in captured:
                            already = True
                        else:
                            for row in captured.values():
                                rpid = extract_post_id(row.get("post_url", ""))
                                if rpid and rpid == pid:
                                    already = True; break
                    if already:
                        dom_skipped_dup += 1
                        continue

                    # Also dedup by (author, text-normalized) to avoid saving same content twice
                    author = dp.get("author", "")
                    text = dp.get("text", "")
                    if not text: continue
                    norm_text = re.sub(r"\s+", "", text.lower())[:80]
                    text_dup = False
                    for row in captured.values():
                        if row.get("profile_name") == author:
                            rnorm = re.sub(r"\s+", "", (row.get("post_text") or "").lower())[:80]
                            if rnorm == norm_text:
                                text_dup = True; break
                    if text_dup:
                        dom_skipped_dup += 1
                        continue

                    # Parse time from DOM text (e.g., "2 h", "45 min", "hier").
                    # If no time label is present, Facebook usually means "just now" —
                    # the post is so recent the UI hasn't rendered a timestamp yet.
                    # Fall back to ref_time so the post is kept (rather than dropped).
                    time_text = dp.get("timeText", "")
                    if time_text:
                        pd, pt, _ = parse_facebook_date(time_text, ref_time)
                    else:
                        pd, pt = ref_time.strftime("%Y-%m-%d"), ref_time.strftime("%H:%M:%S")
                    if not pd or not pt:
                        dom_skipped_no_time += 1
                        continue

                    # Build row
                    key = pid or f"DOM_{len(captured)}_{hash(post_url)}"
                    row = {
                        "post_url": post_url,
                        "post_time": pt,
                        "post_date": pd,
                        "calendar_week": str(datetime.datetime.strptime(pd, "%Y-%m-%d").isocalendar()[1]),
                        "weekday": datetime.datetime.strptime(pd, "%Y-%m-%d").strftime("%A"),
                        "profile_name": author or "Unknown User",
                        "post_text": text,
                        "scrape_timestamp": ref_time.isoformat(),
                        "metadata": None,
                        "job_id": job_id,
                        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    captured[key] = row
                    dom_added += 1
                    print(f"   [DOM] + {author[:25]} | {pd} {pt} | {text[:40]}")

                if dom_added or dom_skipped_no_time or dom_skipped_dup:
                    print(f"   DOM sweep: +{dom_added} new, {dom_skipped_dup} dups, {dom_skipped_no_time} no-time")
            except Exception as _dom_err:
                print(f"   [DOM sweep error] {_dom_err}")

            report_progress("saving", f"Saving {len(captured)} posts...", captured=len(captured))
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
        report_progress("done", "No posts captured", captured=0, saved=0)
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
    report_progress("done", f"Saved {saved} new posts", captured=len(captured), saved=saved)
    # Print skip diagnostics
    print(f"Skip reasons: {skip_reasons}")
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
                duration_sec=round(duration, 1),
                error=f"skip_reasons={skip_reasons}"
            )
        except Exception as e:
            print(f"   Job log error: {e}")


if __name__ == "__main__":
    main()
    try:
        os.remove(PROGRESS_FILE)
    except:
        pass
