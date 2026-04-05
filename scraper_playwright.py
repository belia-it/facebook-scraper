import time
import datetime
import gspread
import hashlib
import os
import json
import re
import requests
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

# Get the absolute path of the script directory to prevent cron path errors
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables using absolute path
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

# --- CONFIGURATION ---
GROUP_URL = os.getenv("GROUP_URL", "https://www.facebook.com/groups/covsousse?sorting_setting=CHRONOLOGICAL")
SHEET_NAME = os.getenv("SHEET_NAME", "covoiturage report") 
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", os.path.join(SCRIPT_DIR, "credentials.json"))
STORAGE_STATE = os.getenv("STORAGE_STATE", os.path.join(SCRIPT_DIR, "facebook_auth.json"))
TIMEZONE_OFFSET = int(os.getenv("TIMEZONE_OFFSET", "1")) # Default to UTC+1 for user
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "50"))
AGE_LIMIT_MINUTES = int(os.getenv("AGE_LIMIT_MINUTES", "59"))

# Extract group identifier for filtering
_group_match = re.search(r'/groups/([^/?]+)', GROUP_URL)
GROUP_SLUG = _group_match.group(1) if _group_match else None
print(f"[Config] Group slug: {GROUP_SLUG}")

SHEET_HEADERS = [
    "post_url", "post_time", "post_date", "calendar_week", "weekday", "profile_name",
    "gender", "offer_or_demand", "from_city", "from_area", "to_city", "to_area",
    "preferred_departure_time", "price", "nr_passengers", "post_text",
    "post_text_english", "post_text_french", "scrape_timestamp"
]

# --- JSON PARSING HELPERS ---

def extract_data_blocks(raw_text):
    """Extract all JSON data blocks from raw FB response text."""
    blocks = []
    i = 0
    n = len(raw_text)
    while True:
        idx = raw_text.find('"data"', i)
        if idx == -1: break
        brace_start = raw_text.find('{', idx)
        if brace_start == -1: break
        depth, end_idx = 0, -1
        for j in range(brace_start, n):
            if raw_text[j] == '{': depth += 1
            elif raw_text[j] == '}':
                depth -= 1
                if depth == 0:
                    end_idx = j
                    break
        if end_idx != -1:
            try:
                block = json.loads(raw_text[brace_start:end_idx+1])
                blocks.append(block)
            except json.JSONDecodeError as e:
                print(f"   [Warning] Malformed JSON block at offset {brace_start}: {e}")
            i = end_idx + 1
        else: break
    return blocks

def parse_fb_response(text):
    """Clean and parse FB response."""
    text = text.replace("for (;;);", "").strip()
    return extract_data_blocks(text)

def parse_facebook_date(date_str, ref_time=None):
    """
    Parse Facebook date string to extract date and time.
    Supports both human-readable strings and Unix timestamps.
    """
    if not ref_time:
        ref_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(hours=TIMEZONE_OFFSET)
    
    if not date_str:
        return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M:%S'), ""

    # Check if it's already a Unix timestamp (from API)
    try:
        if isinstance(date_str, (int, float)) or (isinstance(date_str, str) and date_str.isdigit()):
            ts = int(date_str)
            # Use UTC as the base to avoid system local time discrepancies
            dt_utc = datetime.datetime.fromtimestamp(ts, datetime.UTC).replace(tzinfo=None)
            target = dt_utc + datetime.timedelta(hours=TIMEZONE_OFFSET)
            print(f"   [Debug Time] Raw TS: {ts} -> UTC: {dt_utc.strftime('%H:%M:%S')} -> Shifted: {target.strftime('%H:%M:%S')}")
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), f"API_{ts}"
    except Exception as e:
        print(f"   [Debug Time Error] {e}")

    ds = date_str.lower().strip()
    months = {
        # French
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
        'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12,
        'janv': 1, 'févr': 2, 'sept': 9, 'oct': 10, 'nov': 11, 'déc': 12,
        # English
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'nov': 11, 'dec': 12,
    }
    all_month_names = '|'.join(months.keys())

    try:
        # 0. "Just now" / "A l'instant"
        if ds in ('just now', 'now') or "l'instant" in ds:
            return ref_time.strftime('%Y-%m-%d'), ref_time.strftime('%H:%M:%S'), date_str

        # 1. French exact format "5 mars 2026 à 06:42"
        exact_fr = re.search(r'(\d{1,2})\s+(' + all_month_names + r')\s+(\d{4})\s+[àa]\s+(\d{1,2}):(\d{2})', ds)
        if exact_fr:
            day, m_name, year, h, m = exact_fr.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

        # 1b. English exact format "March 5, 2026 at 06:42"
        exact_en = re.search(r'(' + all_month_names + r')\s+(\d{1,2}),?\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})', ds)
        if exact_en:
            m_name, day, year, h, m = exact_en.groups()
            target = datetime.datetime(int(year), months[m_name], int(day), int(h), int(m))
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

        # 2. Relative time "8 min", "8 mins", "3 hours", "1 jour", "30 seconds", etc.
        rel = re.search(r'(\d+)\s*(minutes?|mins?|m|hours?|heures?|h|jours?|j|days?|d|seconds?|s)\b', ds)
        if rel:
            val, unit = rel.groups()
            val = int(val)
            if unit in ['min', 'mins', 'minute', 'minutes', 'm']:
                target = ref_time - datetime.timedelta(minutes=val)
            elif unit in ['h', 'hour', 'hours', 'heure', 'heures']:
                target = ref_time - datetime.timedelta(hours=val)
            elif unit in ['j', 'jour', 'jours', 'd', 'day', 'days']:
                target = ref_time - datetime.timedelta(days=val)
            elif unit in ['s', 'second', 'seconds']:
                target = ref_time - datetime.timedelta(seconds=val)
            else:
                target = ref_time
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str

        # 3. "Yesterday" / "Hier" with optional time
        if 'hier' in ds or 'yesterday' in ds:
            time_match = re.search(r'(\d{1,2})[:h](\d{2})', ds)
            if time_match:
                h, m = time_match.groups()
                target = (ref_time - datetime.timedelta(days=1)).replace(hour=int(h), minute=int(m), second=0)
            else:
                target = ref_time - datetime.timedelta(days=1)
            return target.strftime('%Y-%m-%d'), target.strftime('%H:%M:%S'), date_str
    except Exception as e:
        print(f"   [Warning] Date parsing failed for '{date_str}': {e}")

    return None, None, date_str

NOTIFIED_POSTS = set()

def notify_api(post_id, data, now_run):
    """Notify the FastAPI server about a new post to broadcast via WebSocket."""
    if post_id in NOTIFIED_POSTS:
        return
    
    from_date, from_time, _ = parse_facebook_date(data.get('postedAt'), now_run)
    
    # Real-time basic parsing for UI tags
    text = data.get('text', '').lower()
    type_tag = "OFFER" if any(x in text for x in ["offre", "chauffeur", "dispo", "disponible", "partage", "offer"]) else "DEMAND" if any(x in text for x in ["chercher", "demande", "besoin", "demand"]) else "UNKNOWN"
    
    payload = {
        "profile_name": data.get('user', 'Unknown User'),
        "post_date": from_date,
        "post_time": from_time,
        "post_text": data.get('text', ''),
        "offer_or_demand": type_tag,
        "post_url": data.get('url', '')
    }
    
    try:
        # Use localhost as the scraper and API run on the same VPS
        requests.post("http://localhost:8000/api/internal/post-update", json=payload, timeout=2)
        NOTIFIED_POSTS.add(post_id)
        print(f"   [Live] Notified API about post: {post_id}")
    except Exception as e:
        # Silently fail if API is not reachable; scraper continues its work
        pass

def main():
    print("--- STARTING PLAYWRIGHT SCRAPER (API VERSION) ---")
    now_run = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(hours=TIMEZONE_OFFSET)
    
    # 1. CONNECT TO GOOGLE SHEETS
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).worksheet("Feuille 1")
        print("✅ Connected to Sheets.")
        # Ensure header row exists for translate_posts.py compatibility
        existing_rows = sheet.get_all_values()
        if not existing_rows:
            sheet.append_row(SHEET_HEADERS)
            print("   Added header row to empty sheet.")
        elif existing_rows[0] != SHEET_HEADERS:
            sheet.insert_row(SHEET_HEADERS, index=1)
            print("   Inserted header row (existing data had no headers).")
    except Exception as e:
        print(f"❌ Sheets Error: {e}")
        return

    # 2. RUN BROWSER
    api_captured_posts = {} # Map post_id -> data

    def get_deep(d, keys, default=None):
        for k in keys:
            if isinstance(d, dict): d = d.get(k, {})
            else: return default
        return d if d else default

    def handle_response(response):
        """Interception listener for background GraphQL responses."""
        if "graphql" in response.url.lower() and response.status == 200:
            try:
                text = response.text()
                print(f"   [Debug API] Intercepted: {response.url[:100]}... (Size: {len(text)})")
                
                if len(text) > 100000:
                    try:
                        with open("debug_large_response.json", "w") as f:
                            f.write(text)
                        print(f"   [Debug API] Saved large response to debug_large_response.json")
                    except:
                        pass

                blocks = parse_fb_response(text)
                
                STORY_TYPENAMES = {"Story", "FeedUnit", "GroupFeedStory", "GroupPost", "UserPost", "FeedStory", "GroupCommerceProductItem"}
                EXCLUDED_TYPENAMES = {
                    "Notification", "NotificationStory", "FeedbackReaction",
                    "PageLikeAction", "ProfileIntroCard", "StoryBucket",
                    "GroupMemberBadge", "GroupMemberProfile", "GroupQuestion",
                    "MarketplaceListing", "Event", "FundraiserStory",
                    "AdStory", "SponsoredStory", "PageStory",
                }
                TIME_FIELDS = {"creation_time", "timestamp", "publish_time", "created_time", "publish_timestamp", "created_timestamp"}

                def find_stories(obj):
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
                        if not is_story and (has_time and has_actors):
                            is_story = True
                        if not is_story and (has_time and has_post_id):
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
                    if isinstance(obj, dict):
                        if key in obj and obj[key]: return obj[key]
                        for v in obj.values():
                            res = find_key_recursive(v, key)
                            if res: return res
                    elif isinstance(obj, list):
                        for v in obj:
                            res = find_key_recursive(v, key)
                            if res: return res
                    return None

                def find_actual_message(s):
                    # Priority 1: message -> text
                    m = s.get("message")
                    if isinstance(m, dict) and "text" in m:
                        return m["text"]
                    # Priority 2: comet_sections -> content -> story -> message -> text
                    # (handled by recursion below but with smarter checks)
                    
                    # Search for 'text' but ignore very short strings if they look like metadata
                    FB_INTERNAL_LABELS = {"S", "e", "·", "J\u2019aime", "Commenter", "Partager", "Like", "Comment", "Share"}
                    def check_text(obj):
                        if isinstance(obj, dict):
                            if "text" in obj and isinstance(obj["text"], str):
                                t = obj["text"].strip()
                                if len(t) >= 1 and t not in FB_INTERNAL_LABELS:
                                    return t
                            for v in obj.values():
                                res = check_text(v)
                                if res: return res
                        elif isinstance(obj, list):
                            for v in obj:
                                res = check_text(v)
                                if res: return res
                        return None
                    return check_text(s)

                def find_actual_user(s):
                    # Try actors list
                    actors = find_key_recursive(s, "actors")
                    if actors and isinstance(actors, list) and len(actors) > 0:
                        first_actor = actors[0]
                        if isinstance(first_actor, dict):
                            name = first_actor.get("name")
                            if name and name != "Unknown User":
                                return name
                    # Try singular actor
                    actor = find_key_recursive(s, "actor")
                    if actor and isinstance(actor, dict):
                        name = actor.get("name")
                        if name and name != "Unknown User":
                            return name
                    # Try author
                    author = find_key_recursive(s, "author")
                    if author and isinstance(author, dict):
                        name = author.get("name")
                        if name and name != "Unknown User":
                            return name
                    return "Unknown User"

                for block in blocks:
                    stories = find_stories(block)
                    for s in stories:
                        post_id = s.get("post_id") or s.get("id")
                        if not post_id: continue
                        
                        # Filter out comments: base64 IDs decode to "comment:..."
                        pid_str = str(post_id)
                        if not pid_str.isdigit():
                            try:
                                import base64
                                decoded = base64.b64decode(pid_str + "==").decode("utf-8", errors="ignore")
                                if decoded.startswith("comment") or decoded.startswith("notification"):
                                    continue
                            except Exception:
                                pass
                        
                        # Extract Message
                        msg = find_actual_message(s)
                        if not msg:
                            msg = "[Media post - no text]"
                        
                        # Extract User
                        user = find_actual_user(s)
                        
                        # Extract Time — prefer numeric timestamps, skip dict strategy objects
                        creation_time = None
                        def find_numeric_time(obj, key):
                            """Find first numeric (int/float) value for a key, recursively."""
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

                        for time_field in TIME_FIELDS:
                            val = find_numeric_time(s, time_field)
                            if val is not None:
                                creation_time = val
                                break
                        
                        if not creation_time:
                            # Log the keys of the story to see what we are missing
                            print(f"   [Debug API] Story found but NO timestamp. Keys: {list(s.keys())[:10]}...")
                        else:
                            print(f"   [Debug API] Captured story: {user[:20]}... (Text Len: {len(msg)})")

                        # Extract URL
                        url = f"https://www.facebook.com/{post_id}"
                        # Try to find a real URL in metadata if available
                        meta_url = find_key_recursive(s, "url")
                        if meta_url and "facebook.com" in str(meta_url):
                            url = meta_url

                        # ── Group filter: only accept group posts ──
                        is_group_post = False
                        if GROUP_SLUG:
                            if GROUP_SLUG in url or "/groups/" in url:
                                is_group_post = True
                        else:
                            is_group_post = True
                        if not is_group_post:
                            continue

                        api_captured_posts[post_id] = {
                            'user': user,
                            'text': msg,
                            'url': url,
                            'postedAt': creation_time,
                            'isFromApi': True
                        }
                        # Notify API in real-time
                        notify_api(post_id, api_captured_posts[post_id], now_run)
            except Exception as e:
                print(f"   [Error API] Failed to parse block: {e}")
                pass

    with sync_playwright() as p:
        is_headless = os.getenv("HEADLESS", "true").lower() == "true"

        # Validate auth file before launching browser
        auth_path = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        if auth_path:
            try:
                with open(auth_path, 'r') as f:
                    auth_data = json.load(f)
                if 'cookies' not in auth_data or len(auth_data.get('cookies', [])) == 0:
                    print("   [WARNING] Auth file has no cookies. Will run unauthenticated.")
                    auth_path = None
                else:
                    print(f"   Auth file loaded: {len(auth_data['cookies'])} cookies found.")
            except (json.JSONDecodeError, IOError) as e:
                print(f"   [WARNING] Auth file corrupted: {e}. Will run unauthenticated.")
                auth_path = None
        else:
            print(f"   [WARNING] No auth file found at {STORAGE_STATE}. Will run unauthenticated.")

        browser = p.chromium.launch(headless=is_headless, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            storage_state=auth_path,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        page = context.new_page()
        # NOTE: response handler attached AFTER navigation to avoid capturing home feed

        print(f"2. Opening Group Feed: {GROUP_URL}")
        try:
            # First try with the sorting parameter
            print(f"   ⏳ Navigating to {GROUP_URL}...")
            page.goto(GROUP_URL, wait_until="commit", timeout=120000)

            # Check if we got redirected to login
            if "login" in page.url.lower():
                print("   [ERROR] Redirected to Facebook login. Session is expired!")
                print("   Please run login_helper.py or start_remote_auth.py to refresh your session.")
                browser.close()
                return
            
            # --- BYPASS PROFILE MODAL ---
            try:
                print("   ⏳ Checking for profile selection modal...")
                try:
                    page.screenshot(path="vps_pre_bypass.png")
                except:
                    pass

                BYPASS_TEXTS = [
                    "Continuer en tant que",   # French
                    "Continue as",             # English
                    "Continuar como",          # Spanish
                ]
                for attempt in range(3):
                    found_bypass = False
                    for bypass_text in BYPASS_TEXTS:
                        target = page.get_by_text(bypass_text, exact=False).first
                        if target.count() > 0:
                            print(f"   Found bypass element: '{bypass_text}'. Clicking...")
                            target.click(force=True, timeout=5000)
                            time.sleep(10)
                            found_bypass = True
                            break
                    if found_bypass:
                        break
                    print(f"   ... Bypass button not found yet (attempt {attempt+1}).")
                    time.sleep(5)

                # Verify we are not stuck on login/checkpoint after bypass
                try:
                    current_url = page.url
                    if "login" in current_url.lower() or "checkpoint" in current_url.lower():
                        print("   [ERROR] Redirected to login/checkpoint after bypass. Session may be expired.")
                except:
                    pass

                try:
                    page.screenshot(path="vps_post_bypass.png")
                except:
                    pass
            except Exception as bypass_e:
                print(f"   ⚠️ Bypass logic encountered an issue: {bypass_e}")

            # Wait for any post element or "main" role to appear
            print("   ⏳ Waiting for content to load (primary attempt)...")
            try:
                # Try to wait for a story element or feed container
                page.wait_for_selector('[role="main"]', timeout=45000)
            except:
                print("   ⚠️ Timeout waiting for [role='main']. Refreshing with longer timeout...")
                try:
                    page.reload(wait_until="commit", timeout=60000)
                    time.sleep(15)
                    # Try one last time to see if main role appeared
                    page.wait_for_selector('[role="main"]', timeout=30000)
                except Exception as re_e:
                    print(f"   ⚠️ Reload attempt also failed: {re_e}")
            
            time.sleep(10) # Final hydration buffer

            # NOW attach response handler — we're on the group page
            page.on("response", handle_response)
            print("   ✅ Response handler attached (group page confirmed).")

            # Capture screenshot for visual verification on VPS
            try:
                page.screenshot(path="vps_check.png")
                print("   📸 Captured screenshot: vps_check.png")
            except:
                print("   ⚠️ Failed to capture screenshot.")
        except Exception as e:
            print(f"❌ Could not reach Group: {e}")
            try:
                page.screenshot(path="vps_error.png")
                print("   📸 Saved error screenshot: vps_error.png")
            except: pass
            browser.close()
            return

        # Primary DOM scraping — extracts posts from visible feed elements
        dom_captured_posts = {}  # dedup_key -> post data

        def scrape_dom_posts():
            """Extract posts from visible DOM feed items. Returns count of NEW posts found."""
            try:
                dom_posts = page.evaluate("""() => {
                    const posts = [];
                    const seen = new Set();

                    // Strategy 1: [role="feed"] > children (best for full browsers)
                    const feed = document.querySelector('[role="feed"]');
                    const containers = feed ? Array.from(feed.children) : [];

                    // Strategy 2: [role="article"] — only top-level (not comment articles nested inside posts)
                    const articles = document.querySelectorAll('[role="article"]');
                    articles.forEach(a => {
                        if (!a.closest('[role="article"] [role="article"]') || a.matches('[role="feed"] > * [role="article"]:first-of-type')) {
                            // Skip if this article is nested inside another article (it's a comment)
                            var parent = a.parentElement;
                            var isNested = false;
                            while (parent) {
                                if (parent.getAttribute && parent.getAttribute('role') === 'article') { isNested = true; break; }
                                parent = parent.parentElement;
                            }
                            if (!isNested) containers.push(a);
                        }
                    });

                    for (const item of containers) {
                        try {
                            // AUTHOR: heading links or strong links
                            let user = '';
                            const headingLink = item.querySelector('h2 a, h3 a, h4 a, strong a, [data-ad-rendering-role="profile_name"] a');
                            if (headingLink) user = headingLink.textContent.trim();
                            if (!user || user === 'Nouvelles publications' || user === 'Recent posts') continue;

                            // TEXT: longest [dir="auto"] text block
                            let text = '';
                            const allDirAuto = item.querySelectorAll('[dir="auto"]');
                            let maxLen = 0;
                            allDirAuto.forEach(el => {
                                const t = el.textContent.trim();
                                if (t.length > maxLen && t.length > 5) {
                                    maxLen = t.length;
                                    text = t;
                                }
                            });

                            // PERMALINK
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

                            // Deduplicate within this scan
                            const key = user + '|' + (text || '').substring(0, 50);
                            if (seen.has(key)) continue;
                            seen.add(key);

                            posts.push({
                                user: user,
                                text: text || '[Media post - no text]',
                                url: url || ''
                            });
                        } catch(e) {}
                    }
                    return posts;
                }""")

                new_count = 0
                for dp in dom_posts:
                    norm_text = re.sub(r'\s+', '', dp['text'].lower())
                    dedup_key = f"{dp['user']}_{hashlib.md5(norm_text.encode()).hexdigest()}"

                    # Group filter: only accept posts with group URLs
                    post_url = dp.get('url', '')
                    if post_url and GROUP_SLUG:
                        if '/groups/' not in post_url and GROUP_SLUG not in post_url:
                            continue

                    if dedup_key not in dom_captured_posts:
                        dom_captured_posts[dedup_key] = {
                            'user': dp['user'],
                            'text': dp['text'],
                            'url': dp['url'],
                            'postedAt': None,
                            'isFromApi': False,
                        }
                        notify_api(dedup_key, dom_captured_posts[dedup_key], now_run)
                        new_count += 1
                return new_count
            except Exception as dom_e:
                print(f"   [Warning] DOM scan failed: {dom_e}")
                return 0

        def enrich_dom_with_api():
            """Enrich DOM posts with timestamps from GraphQL API data."""
            enriched = 0
            for key, dom_post in dom_captured_posts.items():
                if dom_post['postedAt'] is not None:
                    continue
                dom_norm = re.sub(r'\s+', '', dom_post['text'].lower())[:80]
                for api_post in api_captured_posts.values():
                    api_norm = re.sub(r'\s+', '', api_post['text'].lower())[:80]
                    if dom_norm == api_norm or (dom_post['user'] == api_post['user'] and len(dom_norm) > 10 and dom_norm[:40] == api_norm[:40]):
                        dom_post['postedAt'] = api_post['postedAt']
                        if api_post.get('url') and '/groups/' in api_post.get('url', ''):
                            dom_post['url'] = api_post['url']
                        enriched += 1
                        break
            return enriched

        # Scroll Loop — DOM-primary with API enrichment
        print("3. Scrolling and extracting posts from DOM...")
        initial_dom = scrape_dom_posts()
        if initial_dom > 0:
            print(f"   Initial DOM scan captured {initial_dom} posts.")

        stall_count = 0
        for s in range(MAX_SCROLLS):
            prev_dom = len(dom_captured_posts)
            prev_api = len(api_captured_posts)
            
            for _ in range(3):
                page.keyboard.press("PageDown")
                time.sleep(1.5)
            
            time.sleep(2)

            scrape_dom_posts()

            # Stall = neither DOM nor API found anything new
            if len(dom_captured_posts) == prev_dom and len(api_captured_posts) == prev_api:
                stall_count += 1
                time.sleep(3)
                page.keyboard.press("PageUp")
                time.sleep(1.5)
                page.keyboard.press("PageDown")
                time.sleep(1.5)
                scrape_dom_posts()

                if stall_count >= 5:
                    print(f"   Stop: No new posts for {stall_count} consecutive scroll batches. Feed exhausted.")
                    break
            else:
                stall_count = 0

            if s % 2 == 0:
                print(f"   ... Scroll batch {s+1}/{MAX_SCROLLS}, DOM: {len(dom_captured_posts)} posts, API: {len(api_captured_posts)} stories.")

        # Enrich DOM posts with API timestamps
        enriched = enrich_dom_with_api()
        print(f"3b. Navigation complete. DOM: {len(dom_captured_posts)} posts, API enriched: {enriched}.")

        # Refresh session state — preserve critical auth cookies
        try:
            print(f"   💾 Refreshing session state in {STORAGE_STATE}...")
            new_state = context.storage_state()
            new_cookies = new_state.get("cookies", [])
            new_names = {c["name"] for c in new_cookies}
            CRITICAL_COOKIES = {"c_user", "xs", "datr", "sb", "fr"}
            missing = CRITICAL_COOKIES - new_names
            if missing:
                print(f"   ⚠️ Session refresh missing critical cookies: {missing}. Keeping old auth file.")
            else:
                context.storage_state(path=STORAGE_STATE)
                print(f"   ✅ Session refreshed with {len(new_cookies)} cookies.")
        except Exception as st_e:
            print(f"   ⚠️ Failed to save session state: {st_e}")

        browser.close()

    # 4. FINAL FILTERING AND UPLOAD
    print("4. Processing captured data...")
    # Merge: DOM posts + API posts that DOM missed
    all_captured = dict(dom_captured_posts)  # start with DOM posts
    api_only_added = 0
    for api_id, api_post in api_captured_posts.items():
        api_norm = re.sub(r'\s+', '', api_post['text'].lower())
        api_key = f"{api_post['user']}_{hashlib.md5(api_norm.encode()).hexdigest()}"
        # Check if this API post already matched a DOM post (by text similarity)
        already_in = api_key in all_captured
        if not already_in:
            # Also check by fuzzy text match against DOM posts
            for dom_post in dom_captured_posts.values():
                dom_norm = re.sub(r'\s+', '', dom_post['text'].lower())[:80]
                if api_norm[:80] == dom_norm:
                    already_in = True
                    break
        if not already_in and len(api_post['text']) > 1:
            all_captured[api_key] = api_post
            api_only_added += 1
    if api_only_added > 0:
        print(f"   Added {api_only_added} API-only posts not found in DOM.")

    # Filter by age limit (posts with timestamps) or include if no timestamp (visible on page = recent)
    final_posts = []
    for p in all_captured.values():
        try:
            if p['postedAt'] is None:
                final_posts.append(p)
                continue

            p_date, p_time, _ = parse_facebook_date(p['postedAt'], now_run)
            if not p_date or not p_time:
                final_posts.append(p)
                continue

            p_dt = datetime.datetime.strptime(f"{p_date} {p_time}", '%Y-%m-%d %H:%M:%S')
            age_min = (now_run - p_dt).total_seconds() / 60
            if age_min <= AGE_LIMIT_MINUTES:
                final_posts.append(p)
        except Exception as e:
            print(f"   [Warning] Could not parse date in final filter: {e}")

    if not final_posts:
        print(f"⚠️ No posts found in the last {AGE_LIMIT_MINUTES} minutes.")
        return

    print(f"✅ Success! Found {len(final_posts)} posts ({len(dom_captured_posts)} DOM + {api_only_added} API-only).")
    
    # Formating for Sheets
    formatted_rows = []
    for p in final_posts:
        if p['postedAt'] is not None:
            p_date, p_time, _ = parse_facebook_date(p['postedAt'], now_run)
        else:
            p_date, p_time = now_run.strftime('%Y-%m-%d'), now_run.strftime('%H:%M:%S')
        if not p_date or not p_time:
            continue
        post_dt = datetime.datetime.strptime(p_date, '%Y-%m-%d')
        calendar_wk = post_dt.isocalendar()[1]
        wd = post_dt.strftime('%A')
        formatted_rows.append([
            p['url'], p_time, p_date, calendar_wk, wd, p['user'],
            "", "", "", "", "", "", "", "", "", p['text'],
            "", "", now_run.strftime('%Y-%m-%d %H:%M:%S')
        ])
    
    if formatted_rows:
        print(f"   [Debug Sheet] Sample Row: {formatted_rows[0][:6]}")

    # Upload
    try:
        existing_rows = sheet.get_all_values()
        existing_keys = set()
        
        def extract_post_id(url_str):
            match = re.search(r'/(?:posts|permalink|p)/(\d+)', url_str)
            if match:
                return match.group(1)
            # Fallback to any sequence of numbers at the end
            fallback = re.search(r'(\d+)/?$', url_str)
            return fallback.group(1) if fallback else url_str

        for r in existing_rows:
            if len(r) > 0 and r[0] and r[0] != "post_url":
                existing_keys.add(r[0])
                existing_keys.add(extract_post_id(r[0]))
                
            if len(r) > 15 and r[15] and len(r) > 5 and r[15] != "[Media post - no text]":
                text_hash = hashlib.md5(re.sub(r'\s+', '', r[15].lower()).encode()).hexdigest()
                date_val = r[2] if len(r) > 2 else ""
                existing_keys.add(f"{r[5]}_{text_hash}_{date_val}")

        to_upload = []
        for row in formatted_rows:
            url, date_val, user, text = row[0], row[2], row[5], row[15]
            post_id = extract_post_id(url)
            
            if url in existing_keys or post_id in existing_keys: 
                continue
                
            if text != "[Media post - no text]":
                text_hash = hashlib.md5(re.sub(r'\s+', '', text.lower()).encode()).hexdigest()
                text_key = f"{user}_{text_hash}_{date_val}"
                if text_key in existing_keys: 
                    continue
                existing_keys.add(text_key)

            to_upload.append(row)
            existing_keys.add(url)
            existing_keys.add(post_id)

        if to_upload:
            print(f"   [Debug Sheet] Uploading {len(to_upload)} new rows. First time: {to_upload[0][1]}")
            sheet.append_rows(to_upload)
            print(f"🚀 Uploaded {len(to_upload)} new accurately captured posts.")
        else:
            print("✅ Data is already up to date.")
    except Exception as e:
        print(f"❌ Upload Error: {e}")

if __name__ == "__main__":
    main()
