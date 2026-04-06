import os
import sys
import subprocess
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
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
