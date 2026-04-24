import os
import sys
import subprocess
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
import json as _json
from datetime import datetime as _dt
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Paths ─────────────────────────────────────────────────────────────────
API_DIR  = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(API_DIR)
sys.path.append(API_DIR)

from database import init_db, query_posts, get_stats, clear_posts, get_jobs

SCRAPER_DB    = os.path.join(ROOT_DIR, "scraper_db.py")
DASHBOARD_HTML = os.path.join(API_DIR, "templates", "index.html")

class Post(BaseModel):
    id: int = None
    profile_name: str = None
    post_date: str = None
    post_time: str = None
    from_city: str = None
    to_city: str = None
    post_text: str = None
    post_text_english: str = None
    offer_or_demand: str = None
    price: str = None
    nr_passengers: str = None
    gender: str = None
    post_url: str = None

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

manager = ConnectionManager()


# ── App lifecycle ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print("✅  Database ready.")
    yield


app = FastAPI(
    title="Carpool Live Board",
    description="Live dashboard for Facebook carpool posts — Sousse",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=os.path.join(API_DIR, "static")), name="static")


@app.get("/", response_class=FileResponse)
async def dashboard():
    return FileResponse(DASHBOARD_HTML, media_type="text/html")


# ── REST API ────────────────────────────────────────────────────────────────

@app.get("/api/posts")
async def api_posts(
    search: str = Query("", description="Free-text search"),
    filter_type: str = Query("ALL", description="ALL | OFFER | DEMAND"),
    from_city: str = Query("", description="Filter by departure city"),
    limit: int = Query(200, le=500),
    job_id: int = Query(None, description="Filter by job ID"),
):
    combined_search = " ".join(filter(None, [search, from_city]))
    posts = query_posts(search=combined_search, filter_type=filter_type, limit=limit, job_id=job_id)
    return JSONResponse(content=posts)


@app.get("/api/stats")
async def api_stats():
    return get_stats()


# Track scraper subprocess so it can be cancelled from the UI
_scraper_process = None


@app.post("/api/scrape")
async def api_scrape(background_tasks: BackgroundTasks):
    """
    Trigger scraper_db.py as a background subprocess.
    Returns immediately; scraping happens asynchronously.
    """
    global _scraper_process
    if not os.path.exists(SCRAPER_DB):
        raise HTTPException(status_code=404, detail="scraper_db.py not found")

    if _scraper_process and _scraper_process.poll() is None:
        return {"status": "already_running", "message": "Scraper is already running. Stop it first."}

    def run_scraper():
        global _scraper_process
        try:
            _scraper_process = subprocess.Popen(
                [sys.executable, SCRAPER_DB],
                cwd=ROOT_DIR,
            )
            try:
                _scraper_process.wait(timeout=900)  # 15 min max
            except subprocess.TimeoutExpired:
                _scraper_process.kill()
                print("[Scraper] Timed out after 15 minutes.")
        except Exception as e:
            print(f"[Scraper] Error: {e}")
        finally:
            _scraper_process = None

    background_tasks.add_task(run_scraper)
    return {"status": "started", "message": "Scraper is running in the background."}


@app.post("/api/scrape/cancel")
async def api_scrape_cancel():
    """Kill the running scraper subprocess + any child browser/playwright processes."""
    global _scraper_process
    if not _scraper_process or _scraper_process.poll() is not None:
        return {"status": "not_running", "message": "No scraper is running."}

    try:
        # Kill the scraper process tree (includes playwright driver and chromium)
        import signal
        pid = _scraper_process.pid
        _scraper_process.kill()
        # Best-effort kill of related children (playwright driver + chromium)
        try:
            subprocess.run(["pkill", "-P", str(pid)], timeout=5)
        except: pass
        try:
            subprocess.run(["pkill", "-f", "scraper_db.py"], timeout=5)
        except: pass
        # Clean up progress file so the UI knows it stopped
        progress_file = os.path.join(ROOT_DIR, "api", "_scrape_progress.json")
        if os.path.exists(progress_file):
            try: os.remove(progress_file)
            except: pass
        return {"status": "cancelled", "message": "Scraper stopped."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.delete("/api/posts")
async def api_clear_posts():
    """Delete all posts from the database."""
    clear_posts()
    return {"status": "ok", "message": "All posts cleared."}


@app.get("/api/scrape/status")
async def scrape_status():
    progress_file = os.path.join(ROOT_DIR, "api", "_scrape_progress.json")
    if os.path.exists(progress_file):
        try:
            import json
            with open(progress_file) as f:
                return json.load(f)
        except:
            pass
    return {"phase": "idle"}


@app.get("/api/jobs")
async def api_jobs(limit: int = Query(20, le=100)):
    """Return recent scrape job logs."""
    return get_jobs(limit=limit)



# ── Settings ───────────────────────────────────────────────────────────────
DOTENV_PATH = os.path.join(ROOT_DIR, ".env")

@app.get("/api/settings")
async def get_settings():
    """Return current .env settings."""
    from dotenv import dotenv_values
    vals = dotenv_values(DOTENV_PATH)
    return {
        "AGE_LIMIT_MINUTES": int(vals.get("AGE_LIMIT_MINUTES", "59")),
        "MAX_SCROLLS": int(vals.get("MAX_SCROLLS", "35")),
        "GROUP_URL": vals.get("GROUP_URL", ""),
        "TIMEZONE_OFFSET": int(vals.get("TIMEZONE_OFFSET", "1")),
        "HEADLESS": vals.get("HEADLESS", "true"),
    }

@app.post("/api/settings")
async def update_settings(
    AGE_LIMIT_MINUTES: int = Form(None),
    MAX_SCROLLS: int = Form(None),
    TIMEZONE_OFFSET: int = Form(None),
    GROUP_URL: Optional[str] = Form(None),
):
    """Update .env settings."""
    from dotenv import dotenv_values
    vals = dotenv_values(DOTENV_PATH)

    if AGE_LIMIT_MINUTES is not None:
        vals["AGE_LIMIT_MINUTES"] = str(AGE_LIMIT_MINUTES)
    if MAX_SCROLLS is not None:
        vals["MAX_SCROLLS"] = str(MAX_SCROLLS)
    if TIMEZONE_OFFSET is not None:
        vals["TIMEZONE_OFFSET"] = str(TIMEZONE_OFFSET)
    if GROUP_URL is not None:
        vals["GROUP_URL"] = GROUP_URL

    # Read existing .env to preserve unknown keys
    lines = []
    existing_keys = set()
    if os.path.exists(DOTENV_PATH):
        with open(DOTENV_PATH) as f:
            for line in f:
                key = line.split("=")[0].strip()
                if key in vals:
                    lines.append(f"{key}={vals[key]}\n")
                    existing_keys.add(key)
                else:
                    lines.append(line)
    # Add new keys
    for k, v in vals.items():
        if k not in existing_keys:
            lines.append(f"{k}={v}\n")

    with open(DOTENV_PATH, 'w') as f:
        f.writelines(lines)

    return {"status": "ok", "message": "Settings saved. Restart scraper to apply."}


# ── Auth Management ────────────────────────────────────────────────────────
AUTH_FILE = os.path.join(ROOT_DIR, os.getenv("STORAGE_STATE", "facebook_auth.json"))
CRITICAL_COOKIES = {"c_user", "xs", "datr", "sb", "fr"}


@app.get("/api/auth/status")
async def auth_status():
    """Return current Facebook auth status."""
    if not os.path.exists(AUTH_FILE):
        return {"status": "missing", "message": "No auth file found", "cookies": 0, "critical": {}}

    try:
        with open(AUTH_FILE) as f:
            data = _json.load(f)
        cookies = data.get("cookies", [])
        cookie_names = {c.get("name") for c in cookies}
        critical_status = {name: name in cookie_names for name in CRITICAL_COOKIES}
        all_critical = all(critical_status.get(k) for k in ("c_user", "xs"))

        # File age
        mtime = os.path.getmtime(AUTH_FILE)
        modified = _dt.fromtimestamp(mtime).isoformat()
        age_hours = ((_dt.now().timestamp() - mtime) / 3600)

        return {
            "status": "valid" if all_critical else "expired",
            "message": "Session active" if all_critical else "Missing critical cookies (c_user/xs)",
            "cookies": len(cookies),
            "cookie_names": sorted(cookie_names),
            "critical": critical_status,
            "file_modified": modified,
            "age_hours": round(age_hours, 1),
            "file_path": AUTH_FILE,
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "cookies": 0, "critical": {}}


@app.post("/api/auth/upload")
async def auth_upload(file: UploadFile = File(...)):
    """Upload a facebook_auth.json file."""
    try:
        raw = await file.read()
        data = _json.loads(raw)
        if "cookies" not in data:
            raise HTTPException(status_code=400, detail="Invalid auth file: no 'cookies' key")
        cookies = data["cookies"]
        names = {c.get("name") for c in cookies}
        if "c_user" not in names or "xs" not in names:
            raise HTTPException(status_code=400, detail="Auth file missing critical cookies (c_user, xs)")
        with open(AUTH_FILE, 'w') as f:
            _json.dump(data, f, indent=2)
        return {"status": "ok", "message": f"Auth file updated with {len(cookies)} cookies"}
    except _json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON file")


@app.post("/api/auth/cookies")
async def auth_set_cookies(
    c_user: str = Form(...),
    xs: str = Form(...),
    datr: str = Form(""),
    sb: str = Form(""),
    fr: str = Form(""),
):
    """Set Facebook cookies manually."""
    # Load existing auth or create new
    data = {"cookies": [], "origins": []}
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE) as f:
                data = _json.load(f)
        except:
            pass

    existing = {c.get("name"): i for i, c in enumerate(data.get("cookies", []))}
    base = {"domain": ".facebook.com", "path": "/", "httpOnly": True, "secure": True, "sameSite": "None"}

    for name, value in [("c_user", c_user), ("xs", xs), ("datr", datr), ("sb", sb), ("fr", fr)]:
        if not value:
            continue
        cookie = {**base, "name": name, "value": value}
        if name in existing:
            data["cookies"][existing[name]] = cookie
        else:
            data["cookies"].append(cookie)

    with open(AUTH_FILE, 'w') as f:
        _json.dump(data, f, indent=2)

    return {"status": "ok", "message": f"Cookies updated ({len(data['cookies'])} total)"}




@app.post("/api/auth/import-browser")
async def auth_import_browser(browser: str = Form("auto")):
    """
    Read Facebook cookies directly from the user's installed browser.
    No copy/paste, no login prompts — uses browser_cookie3 to access the local cookie store.
    browser: one of auto, chrome, safari, firefox, edge, brave, opera, chromium, arc
    """
    try:
        import browser_cookie3 as bc3
    except ImportError:
        raise HTTPException(status_code=500, detail="browser_cookie3 not installed. Run: pip install browser_cookie3")

    backends = {
        "chrome": bc3.chrome, "safari": bc3.safari, "firefox": bc3.firefox,
        "edge": bc3.edge, "brave": bc3.brave, "opera": bc3.opera,
        "chromium": bc3.chromium,
    }
    if hasattr(bc3, "arc"):
        backends["arc"] = bc3.arc
    if hasattr(bc3, "opera_gx"):
        backends["opera_gx"] = bc3.opera_gx

    tried = []
    cookies = None
    source = None

    if browser == "auto":
        # Try each backend in order of likelihood on macOS
        order = ["chrome", "safari", "arc", "brave", "edge", "firefox", "opera", "opera_gx", "chromium"]
    else:
        if browser not in backends:
            raise HTTPException(status_code=400, detail=f"Unknown browser: {browser}. Try: {', '.join(backends.keys())}")
        order = [browser]

    for name in order:
        if name not in backends:
            continue
        try:
            jar = backends[name](domain_name="facebook.com")
            found = list(jar)
            tried.append({"browser": name, "count": len(found)})
            if len(found) > 0:
                # Check for critical cookies
                names = {c.name for c in found}
                if "c_user" in names and "xs" in names:
                    cookies = found
                    source = name
                    break
        except Exception as e:
            tried.append({"browser": name, "error": str(e)[:80]})

    if not cookies:
        msg_parts = []
        for t in tried:
            if "error" in t:
                msg_parts.append(f"{t['browser']}: {t['error']}")
            else:
                msg_parts.append(f"{t['browser']}: {t['count']} cookies")
        raise HTTPException(status_code=404,
            detail=f"Could not find Facebook cookies (c_user+xs) in any browser. Tried: {'; '.join(msg_parts)}. "
                   f"Make sure you are logged into facebook.com in at least one browser.")

    # Build Playwright-compatible storage_state
    playwright_cookies = []
    for c in cookies:
        # Map cookielib Cookie to Playwright cookie format
        same_site = "None"
        secure = bool(c.secure)
        http_only = bool(getattr(c, "_rest", {}).get("HttpOnly") or getattr(c, "has_nonstandard_attr", lambda x: False)("HttpOnly"))
        pw_cookie = {
            "name": c.name,
            "value": c.value,
            "domain": c.domain if c.domain.startswith(".") else f".{c.domain}" if "facebook.com" in c.domain else c.domain,
            "path": c.path or "/",
            "httpOnly": http_only,
            "secure": secure,
            "sameSite": same_site,
        }
        if c.expires:
            pw_cookie["expires"] = float(c.expires)
        playwright_cookies.append(pw_cookie)

    # Load existing auth or create new
    data = {"cookies": [], "origins": []}
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE) as f:
                data = _json.load(f)
        except:
            pass

    # Merge: replace cookies with same name, add new ones
    by_name = {c.get("name"): i for i, c in enumerate(data.get("cookies", []))}
    for pc in playwright_cookies:
        if pc["name"] in by_name:
            data["cookies"][by_name[pc["name"]]] = pc
        else:
            data["cookies"].append(pc)

    with open(AUTH_FILE, 'w') as f:
        _json.dump(data, f, indent=2)

    saved_names = sorted({c["name"] for c in playwright_cookies})
    return {
        "status": "ok",
        "source": source,
        "message": f"Imported {len(playwright_cookies)} cookies from {source}",
        "saved_names": saved_names,
        "tried": tried,
    }



# In-process state for login popup
_login_state = {"running": False, "success": False, "error": None, "started_at": None}
_login_process = None


@app.post("/api/auth/login")
async def auth_login(background_tasks: BackgroundTasks):
    """Launch a visible Chromium window on facebook.com so the user can log in once.
    When the feed loads, cookies are captured and saved automatically."""
    global _login_state, _login_process

    if _login_state.get("running"):
        return {"status": "already_running", "message": "Login window is already open."}

    helper_script = os.path.join(ROOT_DIR, "login_popup_helper.py")
    if not os.path.exists(helper_script):
        raise HTTPException(status_code=500, detail=f"Helper script missing: {helper_script}")

    _login_state = {"running": True, "success": False, "error": None, "started_at": _dt.now().isoformat()}

    def run_helper():
        global _login_process, _login_state
        try:
            proc = subprocess.Popen(
                [sys.executable, helper_script],
                cwd=ROOT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            _login_process = proc
            out, _ = proc.communicate(timeout=600)  # 10 min max for user to log in
            if proc.returncode == 0 and "LOGIN_SUCCESS" in (out or ""):
                _login_state["success"] = True
            else:
                _login_state["error"] = (out or "").strip()[-500:] or f"Exit code {proc.returncode}"
        except subprocess.TimeoutExpired:
            if _login_process:
                _login_process.kill()
            _login_state["error"] = "Login window timed out (10 min)"
        except Exception as e:
            _login_state["error"] = str(e)
        finally:
            _login_state["running"] = False
            _login_process = None

    background_tasks.add_task(run_helper)
    return {"status": "started", "message": "A browser window will open in a moment. Log in to Facebook there, then come back here."}


@app.get("/api/auth/login/status")
async def auth_login_status():
    """Poll the login popup status."""
    return dict(_login_state)


@app.post("/api/auth/login/cancel")
async def auth_login_cancel():
    """Kill the login popup if it is still open."""
    global _login_process, _login_state
    if _login_process:
        try:
            _login_process.kill()
        except: pass
    _login_state["running"] = False
    return {"status": "cancelled"}

@app.delete("/api/auth")
async def auth_clear():
    """Delete the auth file."""
    if os.path.exists(AUTH_FILE):
        os.remove(AUTH_FILE)
    return {"status": "ok", "message": "Auth file deleted"}


@app.get("/api/health")
async def health():
    stats = get_stats()
    return {"status": "ok", **stats}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/api/internal/post-update")
async def post_update(post: Post):
    """
    Called by the scraper when a new post is saved.
    Broadcasts it to all connected WebSocket clients.
    """
    await manager.broadcast({"type": "new_post", "data": post.dict()})
    return {"status": "ok"}
