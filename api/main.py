import os
import subprocess
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from database import init_db, query_posts, get_stats

# ── Paths ─────────────────────────────────────────────────────────────────
API_DIR  = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(API_DIR)
SCRAPER_DB    = os.path.join(ROOT_DIR, "scraper_db.py")
DASHBOARD_HTML = os.path.join(API_DIR, "templates", "index.html")


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
):
    combined_search = " ".join(filter(None, [search, from_city]))
    posts = query_posts(search=combined_search, filter_type=filter_type, limit=limit)
    return JSONResponse(content=posts)


@app.get("/api/stats")
async def api_stats():
    return get_stats()


@app.post("/api/scrape")
async def api_scrape(background_tasks: BackgroundTasks):
    """
    Trigger scraper_db.py as a background subprocess.
    Returns immediately; scraping happens asynchronously.
    """
    if not os.path.exists(SCRAPER_DB):
        raise HTTPException(status_code=404, detail="scraper_db.py not found")

    def run_scraper():
        try:
            subprocess.run(
                [sys.executable, SCRAPER_DB],
                cwd=ROOT_DIR,
                timeout=600,      # 10 min max
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"[Scraper] Error: {e}")
        except subprocess.TimeoutExpired:
            print("[Scraper] Timed out after 10 minutes.")

    background_tasks.add_task(run_scraper)
    return {"status": "started", "message": "Scraper is running in the background."}


@app.get("/api/health")
async def health():
    stats = get_stats()
    return {"status": "ok", **stats}
