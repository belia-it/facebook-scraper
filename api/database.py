import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "posts.db")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS posts (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    post_url                TEXT UNIQUE NOT NULL,
    post_time               TEXT,
    post_date               TEXT,
    calendar_week           TEXT,
    weekday                 TEXT,
    profile_name            TEXT,
    gender                  TEXT,
    offer_or_demand         TEXT,
    from_city               TEXT,
    from_area               TEXT,
    to_city                 TEXT,
    to_area                 TEXT,
    preferred_departure_time TEXT,
    price                   TEXT,
    nr_passengers           TEXT,
    post_text               TEXT,
    post_text_english       TEXT,
    post_text_french        TEXT,
    scrape_timestamp        TEXT,
    synced_at               TEXT DEFAULT (datetime('now'))
);
"""

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

CREATE_JOBS_SQL = """
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
);
"""

def init_db():
    conn = get_connection()
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(CREATE_JOBS_SQL)
    conn.commit()
    conn.close()

def upsert_posts(rows: list[dict]):
    """Insert or replace posts by post_url (upsert)."""
    if not rows:
        return 0
    conn = get_connection()
    inserted = 0
    for row in rows:
        try:
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
                    post_time               = excluded.post_time,
                    post_date               = excluded.post_date,
                    calendar_week           = excluded.calendar_week,
                    weekday                 = excluded.weekday,
                    profile_name            = excluded.profile_name,
                    gender                  = excluded.gender,
                    offer_or_demand         = excluded.offer_or_demand,
                    from_city               = excluded.from_city,
                    from_area               = excluded.from_area,
                    to_city                 = excluded.to_city,
                    to_area                 = excluded.to_area,
                    preferred_departure_time = excluded.preferred_departure_time,
                    price                   = excluded.price,
                    nr_passengers           = excluded.nr_passengers,
                    post_text               = excluded.post_text,
                    post_text_english       = excluded.post_text_english,
                    post_text_french        = excluded.post_text_french,
                    scrape_timestamp        = excluded.scrape_timestamp,
                    synced_at               = datetime('now')
            """, row)
            inserted += 1
        except Exception as e:
            print(f"[DB] Skipped row: {e}")
    conn.commit()
    conn.close()
    return inserted

def query_posts(search: str = "", filter_type: str = "ALL", limit: int = 200):
    conn = get_connection()
    base_sql = """
        SELECT * FROM posts
        WHERE 1=1
    """
    params = []

    if search:
        base_sql += """
            AND (
                profile_name LIKE ? OR from_city LIKE ? OR to_city LIKE ?
                OR from_area LIKE ? OR to_area LIKE ? OR post_text LIKE ?
                OR post_text_english LIKE ?
            )
        """
        like = f"%{search}%"
        params.extend([like] * 7)

    if filter_type == "OFFER":
        base_sql += " AND LOWER(offer_or_demand) LIKE '%offer%'"
    elif filter_type == "DEMAND":
        base_sql += " AND LOWER(offer_or_demand) LIKE '%demand%'"

    base_sql += " ORDER BY post_date DESC, post_time DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(base_sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def clear_posts():
    conn = get_connection()
    conn.execute("DELETE FROM posts")
    conn.commit()
    conn.close()
    # VACUUM must run outside a transaction
    conn2 = get_connection()
    conn2.execute("VACUUM")
    conn2.close()

def create_job(started_at):
    conn = get_connection()
    cur = conn.execute("INSERT INTO scrape_jobs (started_at) VALUES (?)", (started_at,))
    job_id = cur.lastrowid
    conn.commit()
    conn.close()
    return job_id

def update_job(job_id, **kwargs):
    conn = get_connection()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    conn.execute(f"UPDATE scrape_jobs SET {sets} WHERE id = ?", vals)
    conn.commit()
    conn.close()

def get_jobs(limit=20):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM scrape_jobs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_stats():
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    offers = conn.execute("SELECT COUNT(*) FROM posts WHERE LOWER(offer_or_demand) LIKE '%offer%'").fetchone()[0]
    demands = conn.execute("SELECT COUNT(*) FROM posts WHERE LOWER(offer_or_demand) LIKE '%demand%'").fetchone()[0]
    latest = conn.execute("SELECT synced_at FROM posts ORDER BY synced_at DESC LIMIT 1").fetchone()
    conn.close()
    return {
        "total": total,
        "offers": offers,
        "demands": demands,
        "latest_sync": latest[0] if latest else None
    }
