import sqlite3
import math
import os
import requests
import time
from fastapi import FastAPI, Query, HTTPException

# ───────────────── CONFIG ─────────────────

API_NAME = "Paradox Movie API | @Paradox0x0 | @ParadoxBackup"
API_VERSION = "1.0"

BLOB_DB_URL = os.getenv("MOVIES_DB_URL")
LOCAL_DB_PATH = "/tmp/movies.db"

if not BLOB_DB_URL:
    raise RuntimeError("MOVIES_DB_URL environment variable is not set")

APP_START_TIME = time.time()

app = FastAPI(
    title="Paradox Movie API",
    version=API_VERSION,
    docs_url=None,
    redoc_url=None
)

# ───────────────── DB HELPERS ─────────────────

def download_db_once():
    if os.path.exists(LOCAL_DB_PATH):
        return

    r = requests.get(BLOB_DB_URL, timeout=120)
    r.raise_for_status()

    with open(LOCAL_DB_PATH, "wb") as f:
        f.write(r.content)


def get_db():
    return sqlite3.connect(LOCAL_DB_PATH, check_same_thread=False)

# ───────────────── STARTUP ─────────────────

@app.on_event("startup")
def startup():
    download_db_once()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movies")
    total = cur.fetchone()[0]
    conn.close()

    print(f"🚀 SQLite DB ready: {total} records")

# ───────────────── ROUTES ─────────────────

# 1️⃣ BASE / HEALTH (MINIMAL INFO ONLY)
@app.get("/")
def health():
    return {
        "success": True,
        "api": API_NAME,
        "status": "online"
    }

# 2️⃣ STATS (ADMIN / PUBLIC SAFE)
@app.get("/stats")
def stats():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM movies")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM movies WHERE type='video'")
    videos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM movies WHERE type='document'")
    documents = cur.fetchone()[0]

    conn.close()

    return {
        "success": True,
        "api": API_NAME,
        "version": API_VERSION,
        "started_at": time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(APP_START_TIME)
        ),
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "database": {
            "total_items": total,
            "videos": videos,
            "documents": documents
        }
    }

# 3️⃣ STRICT MOVIE SEARCH (ANTI-SPAM)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    tokens = [t.lower().strip() for t in q.split() if len(t) > 1]
    if not tokens:
        raise HTTPException(status_code=400, detail="Invalid query")

    offset = (page - 1) * limit

    conditions = []
    params = []

    for t in tokens:
        conditions.append("""
        (
            lower(title) LIKE ?
            OR lower(file_name) LIKE ?
        )
        """)
        params.extend([f"%{t}%", f"%{t}%"])

    where_clause = " AND ".join(conditions)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        f"SELECT COUNT(*) FROM movies WHERE {where_clause}",
        params
    )
    total_results = cur.fetchone()[0]

    if total_results == 0:
        conn.close()
        return {
            "success": True,
            "query": q,
            "page": 1,
            "limit": limit,
            "total_results": 0,
            "total_pages": 0,
            "results": []
        }

    total_pages = math.ceil(total_results / limit)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit

    cur.execute(
        f"""
        SELECT uid, title, duration, size, type
        FROM movies
        WHERE {where_clause}
        ORDER BY post_id DESC
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset]
    )

    rows = cur.fetchall()
    conn.close()

    return {
        "success": True,
        "query": q,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": [
            {
                "uid": r[0],
                "title": r[1],
                "duration": r[2],
                "size": r[3],
                "type": r[4]
            }
            for r in rows
        ]
    }

# 4️⃣ UID → FILE RESOLVER (FAST & SAFE)
@app.get("/file")
def resolve(uid: str = Query(..., min_length=1)):
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT uid, post_id, channel_id, title
        FROM movies
        WHERE uid = ?
        LIMIT 1
        """,
        (uid,)
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Invalid UID")

    return {
        "success": True,
        "uid": row[0],
        "post_id": row[1],
        "channel_id": row[2],
        "title": row[3]
    }